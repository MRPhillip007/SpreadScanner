# -*- coding: utf-8 -*-
"""Дашборд форварда: читает data/forward.db (бот пишет — веб читает, развязано).
Запуск: python web.py  ->  http://127.0.0.1:8100"""
from __future__ import annotations

import functools
import os
import sqlite3
import time

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

ROOT = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(ROOT, "data", "forward.db")
app = FastAPI(title="Spread Forward")

_cache: dict = {}


def cached(ttl_s: float):
    """Кэш ответа на ttl_s секунд. База растёт (3.7М решений / 508 МБ за четверо
    суток), и пересчёт агрегатов на каждый опрос браузера — это часы CPU в сутки,
    отнятые у бота на двухъядерной машине (ровно тот механизм, что раньше давал
    отставание фида и фантомные сделки)."""
    def deco(fn):
        @functools.wraps(fn)
        def wrap(*a, **kw):
            key = (fn.__name__, a, tuple(sorted(kw.items())))
            now = time.time()
            hit = _cache.get(key)
            if hit is not None and now - hit[0] < ttl_s:
                return hit[1]
            val = fn(*a, **kw)
            _cache[key] = (now, val)
            return val
        return wrap
    return deco


def q(sql, args=()):
    if not os.path.exists(DB):
        return []
    db = sqlite3.connect(DB, timeout=2)
    db.row_factory = sqlite3.Row
    try:
        db.execute("PRAGMA query_only=ON")   # защита от записи вместо mode=ro
        db.execute("PRAGMA busy_timeout=2000")
        return [dict(r) for r in db.execute(sql, args).fetchall()]
    except sqlite3.OperationalError:
        return []                            # писатель в транзакции — отдадим пусто, не 500
    finally:
        db.close()


EMPTY_TR = dict(n=0, pnl=0, avg_pnl=0, wins=0, fees=0, slip=0, funding=0, avg_hold_s=0)


def cur_run():
    """Текущая эпоха. Все сводки считаются по ней, а не по всей истории базы:
    эпоха = версия конфига, смешивать разные версии в одних цифрах бессмысленно
    (и именно полный проход по истории жёг процессор)."""
    r = q("SELECT run_id, ts, cfg_hash FROM config_runs ORDER BY ts DESC LIMIT 1")
    return r[0] if r else dict(run_id="", ts=0, cfg_hash="")


@app.get("/api/summary")
@cached(12)
def summary():
    if not os.path.exists(DB):
        return dict(equity=[], trades=EMPTY_TR, no_fill=0, leg_risk=0, decisions=[],
                    exit_reasons=[], note="БД не найдена — сначала запусти forward.py")
    run = cur_run()
    rid, rts = run["run_id"], run["ts"]
    eq = q("""SELECT venue, cash FROM (SELECT venue, cash,
                ROW_NUMBER() OVER (PARTITION BY venue ORDER BY ts DESC) rn
                FROM equity WHERE ts >= ?) WHERE rn = 1""", (rts,))
    tr_rows = q("""SELECT COUNT(*) n, COALESCE(SUM(pnl_usd),0) pnl,
              COALESCE(AVG(pnl_usd),0) avg_pnl,
              SUM(CASE WHEN pnl_usd>0 THEN 1 ELSE 0 END) wins,
              COALESCE(SUM(fees_usd),0) fees, COALESCE(SUM(entry_slip_usd+exit_slip_usd),0) slip,
              COALESCE(SUM(funding_usd),0) funding, COALESCE(AVG(hold_ms)/1000.0,0) avg_hold_s
              FROM trades WHERE run=? AND status='closed'
                AND exit_reason NOT IN ('no_fill','maker_cancel','maker_ttl')""", (rid,))
    tr = tr_rows[0] if tr_rows else EMPTY_TR
    if tr.get("wins") is None:
        tr["wins"] = 0
    nf = q("SELECT COUNT(*) n FROM trades WHERE run=? AND exit_reason='no_fill'", (rid,))
    nofill = nf[0]["n"] if nf else 0
    lg = q("SELECT COUNT(*) n FROM incidents WHERE kind='leg_risk' AND ts>=?", (rts,))
    legr = lg[0]["n"] if lg else 0
    dec = q("""SELECT action, COUNT(*) n FROM decisions WHERE run=?
               GROUP BY action ORDER BY n DESC""", (rid,))
    reasons = q("""SELECT exit_reason, COUNT(*) n, COALESCE(SUM(pnl_usd),0) pnl
                   FROM trades WHERE run=? AND status='closed'
                   GROUP BY exit_reason ORDER BY pnl""", (rid,))
    return dict(equity=eq, trades=tr, no_fill=nofill, leg_risk=legr, decisions=dec,
                exit_reasons=reasons, epoch=run["run_id"], cfg=run["cfg_hash"][:8])


@app.get("/api/positions")
@cached(4)
def positions():
    return q("""SELECT trade_id, scheme, sym, buy_ex, sell_ex, t_open, status,
                notional_usd, theor_net_pct FROM trades
                WHERE status NOT IN ('closed') ORDER BY t_open DESC LIMIT 50""")


@app.get("/api/trades")
@cached(10)
def trades(limit: int = 100):
    return q("""SELECT trade_id, scheme, sym, buy_ex, sell_ex, t_open, t_close, qty,
                notional_usd, theor_net_pct, pnl_usd, entry_slip_usd, exit_slip_usd,
                fees_usd, funding_usd, exit_reason, hold_ms
                FROM trades WHERE status='closed'
                  AND exit_reason NOT IN ('maker_cancel','maker_ttl')
                ORDER BY t_close DESC LIMIT ?""", (limit,))


@app.get("/api/decisions")
@cached(10)
def decisions(limit: int = 200):
    # ix_dec_ts делает это мгновенным; без LIMIT по индексу был бы полный проход
    return q("SELECT * FROM decisions ORDER BY ts DESC LIMIT ?", (limit,))


@app.get("/api/health")
@cached(12)
def health():
    # оконная функция вместо коррелированного подзапроса: тот перебирал
    # всю таблицу для КАЖДОЙ строки (feeds пишется раз в 30с — за сутки тысячи строк)
    since = cur_run()["ts"]
    feeds = q("""SELECT venue, rate, lag_ms, ts FROM (SELECT venue, rate, lag_ms, ts,
                 ROW_NUMBER() OVER (PARTITION BY venue ORDER BY ts DESC) rn
                 FROM feeds WHERE ts >= ?) WHERE rn = 1""", (since,))
    rtt = q("""SELECT venue, rtt_ms, ts FROM (SELECT venue, rtt_ms, ts,
               ROW_NUMBER() OVER (PARTITION BY venue ORDER BY ts DESC) rn
               FROM venue_rtt WHERE ts >= ?) WHERE rn = 1""", (since,))
    runs = q("SELECT * FROM config_runs ORDER BY ts DESC LIMIT 5")
    return dict(feeds=feeds, rtt=rtt, runs=runs)


PAGE = """<!doctype html><meta charset="utf-8"><title>Spread Forward</title>
<style>
body{font:13px/1.45 -apple-system,Segoe UI,Roboto,sans-serif;background:#0e1116;color:#dbe2ea;margin:20px}
h2{font-size:15px;margin:18px 0 8px;color:#8fb3ff} table{border-collapse:collapse;width:100%}
td,th{padding:4px 8px;border-bottom:1px solid #232a35;text-align:right;white-space:nowrap}
th{color:#7b8794;font-weight:600} td:first-child,th:first-child{text-align:left}
.pos{color:#4cd28f}.neg{color:#ff7676}.card{display:inline-block;background:#161b23;
border:1px solid #232a35;border-radius:8px;padding:10px 16px;margin:4px 8px 4px 0}
.card b{font-size:17px}.muted{color:#7b8794}
</style>
<div id="cards"></div>
<h2>Открытые связки (держат слоты)</h2><table id="pos"></table>
<h2>Причины выходов</h2><table id="reasons"></table>
<h2>Последние сделки</h2><table id="trades"></table>
<h2>Решения (вкл. отказы)</h2><table id="dec"></table>
<h2>Здоровье</h2><table id="health"></table>
<script>
const f=(u)=>fetch(u).then(r=>r.json());
const pn=(x)=>`<span class="${x>=0?'pos':'neg'}">${(+x).toFixed(2)}</span>`;
const ts=(t)=>t?new Date(t).toISOString().substr(5,14).replace('T',' '):'—';
async function tick(){
 const s=await f('/api/summary');
 let eqsum=s.equity.reduce((a,x)=>a+x.cash,0);
 document.getElementById('cards').innerHTML=
  (s.note?`<div class=card style="border-color:#b8860b">⚠ ${s.note}</div>`:'')+
  `<div class=card>Эпоха <b>${s.cfg||'—'}</b><br><span class=muted>${
     s.epoch||''}<br>цифры ниже — только по ней</span></div>`+
  `<div class=card>Эквити <b>$${eqsum.toFixed(2)}</b><br><span class=muted>${
     s.equity.map(x=>x.venue+' $'+x.cash.toFixed(0)).join(' · ')}</span></div>`+
  `<div class=card>Сделок <b>${s.trades.n}</b><br><span class=muted>WR ${
     s.trades.n?(100*s.trades.wins/s.trades.n).toFixed(0):0}% · холд ${
     s.trades.avg_hold_s.toFixed(0)}с · no-fill ${s.no_fill}</span></div>`+
  `<div class=card>PnL <b>${pn(s.trades.pnl)}</b><br><span class=muted>слип ${
     pn(s.trades.slip)} · комис ${s.trades.fees.toFixed(2)} · фанд ${pn(s.trades.funding)}</span></div>`+
  `<div class=card>Leg-risk <b>${s.leg_risk}</b></div>`;
 const ps=await f('/api/positions');
 document.getElementById('pos').innerHTML=
  '<tr><th>#</th><th>схема</th><th>символ</th><th>пара</th><th>$</th><th>теор.net%</th><th>статус</th><th>висит мин</th></tr>'+
  ps.map(p=>`<tr><td>${p.trade_id}</td><td>${p.scheme}</td><td>${p.sym}</td>
   <td>${p.buy_ex}→${p.sell_ex}</td><td>${p.notional_usd.toFixed(0)}</td>
   <td>${p.theor_net_pct.toFixed(2)}</td><td>${p.status}</td>
   <td>${((Date.now()-p.t_open)/60000).toFixed(1)}</td></tr>`).join('')
  || '<tr><td class=muted>нет</td></tr>';
 document.getElementById('reasons').innerHTML='<tr><th>причина</th><th>сделок</th><th>PnL $</th></tr>'+
  s.exit_reasons.map(r=>`<tr><td>${r.exit_reason}</td><td>${r.n}</td><td>${pn(r.pnl)}</td></tr>`).join('');
 const tr=await f('/api/trades?limit=40');
 document.getElementById('trades').innerHTML=
  '<tr><th>#</th><th>схема</th><th>время</th><th>символ</th><th>пара</th><th>$</th><th>теор.net%</th><th>PnL $</th><th>слип $</th><th>комис</th><th>холд с</th><th>выход</th></tr>'+
  tr.map(t=>`<tr><td>${t.trade_id}</td><td>${t.scheme}</td><td>${ts(t.t_close)}</td><td>${t.sym}</td>
   <td>${t.buy_ex}→${t.sell_ex}</td><td>${t.notional_usd.toFixed(0)}</td>
   <td>${t.theor_net_pct.toFixed(2)}</td><td>${pn(t.pnl_usd)}</td>
   <td>${pn(-(t.entry_slip_usd+t.exit_slip_usd))}</td><td>${t.fees_usd.toFixed(2)}</td>
   <td>${(t.hold_ms/1000).toFixed(0)}</td><td>${t.exit_reason}</td></tr>`).join('');
 const d=await f('/api/decisions?limit=60');
 document.getElementById('dec').innerHTML=
  '<tr><th>время</th><th>символ</th><th>пара</th><th>gross%</th><th>net%</th><th>ёмк.$</th><th>действие</th></tr>'+
  d.map(x=>`<tr><td>${ts(x.ts)}</td><td>${x.sym}</td><td>${x.buy_ex}→${x.sell_ex}</td>
   <td>${x.gross_pct.toFixed(2)}</td><td>${x.net_pct.toFixed(2)}</td>
   <td>${x.cap_usd.toFixed(0)}</td><td>${x.action}</td></tr>`).join('');
 const h=await f('/api/health');
 document.getElementById('health').innerHTML=
  '<tr><th>биржа</th><th>сообщ/с</th><th>лаг мс</th><th>RTT мс</th></tr>'+
  h.feeds.map(x=>{const r=h.rtt.find(y=>y.venue===x.venue)||{};
   return `<tr><td>${x.venue}</td><td>${x.rate.toFixed(0)}</td><td>${x.lag_ms.toFixed(0)}</td>
    <td>${r.rtt_ms?r.rtt_ms.toFixed(0):'—'}</td></tr>`}).join('');
}
tick(); setInterval(tick, 8000);   // 3с -> 8с: на двух ядрах опрос конкурирует
                                   // с ботом за CPU, а данные так часто не меняются
</script>"""


@app.get("/", response_class=HTMLResponse)
def index():
    return PAGE


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1",
                port=int(os.environ.get("PORT", "8100")), log_level="warning")
