# -*- coding: utf-8 -*-
"""Диагностика ПОСЛЕДНЕЙ эпохи (последний запуск бота): куда утекает PnL.
Запуск на сервере: python diag.py  — вывод прислать целиком."""
import os
import sqlite3

import pandas as pd

DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "forward.db")
db = sqlite3.connect(f"file:{DB.replace(chr(92), '/')}?mode=ro", uri=True) \
    if False else sqlite3.connect(DB, timeout=5)
db.execute("PRAGMA query_only=ON")
pd.set_option("display.width", 160)

runs = pd.read_sql("SELECT * FROM config_runs ORDER BY ts", db)
run = runs.iloc[-1]
print(f"эпоха: {run.run_id}  конфиг {run.cfg_hash[:8]}  старт "
      f"{pd.Timestamp(run.ts, unit='ms', tz='UTC')}")

tr = pd.read_sql("SELECT * FROM trades WHERE run=? AND status='closed'", db,
                 params=(run.run_id,))
if not len(tr):
    print("сделок в этой эпохе нет"); raise SystemExit
hrs = (tr.t_close.max() - tr.t_open.min()) / 3.6e6
print(f"закрытых записей: {len(tr)} за {hrs:.1f}ч, PnL ИТОГО: {tr.pnl_usd.sum():+.2f}$")

# ── фильтр контаминации: сделки, входившие при ОТСТАВАНИИ фида (фантомы) ──
# feeds.lag_ms пишется каждые 30с; если на входе лаг ноги > порога — котировка
# была протухшей, спред фантомный. Дальнейшие разрезы — только по чистым.
LAG_BAD_MS = int(os.environ.get("LAG_BAD_MS", "2000"))
fe = pd.read_sql("SELECT ts, venue, lag_ms FROM feeds WHERE ts>=?", db,
                 params=(int(run.ts),)).sort_values("ts")
if len(fe):
    tr = tr.sort_values("t_open", kind="stable").reset_index(drop=True)
    for col in ("buy_ex", "sell_ex"):
        lagv = tr[["t_open", col]].rename(columns={col: "venue"})
        m = pd.merge_asof(lagv, fe.rename(columns={"ts": "fts"}),
                          left_on="t_open", right_on="fts", by="venue",
                          direction="nearest", tolerance=90_000)
        tr[col + "_lag"] = m["lag_ms"].to_numpy()
    tr["contam"] = ((tr.buy_ex_lag.fillna(0) > LAG_BAD_MS)
                    | (tr.sell_ex_lag.fillna(0) > LAG_BAD_MS))
    bad, clean = tr[tr.contam], tr[~tr.contam]
    print(f"\n── ФИЛЬТР КОНТАМИНАЦИИ (лаг фида > {LAG_BAD_MS} мс на входе) ──")
    print(f"загрязнённых сделок: {len(bad)} из {len(tr)} "
          f"({len(bad)/len(tr)*100:.0f}%), их PnL {bad.pnl_usd.sum():+.2f}$")
    print(f"ЧИСТЫЙ PnL: {clean.pnl_usd.sum():+.2f}$ на {len(clean)} сделках")
    badf = fe[fe.lag_ms > LAG_BAD_MS]
    if len(badf):
        sp = pd.to_datetime(badf.ts, unit="ms", utc=True)
        print(f"окна отставания: {sp.min():%m-%d %H:%M}..{sp.max():%H:%M} UTC "
              f"({len(badf)} снапшотов; худший лаг {badf.lag_ms.max()/1000:.0f}с)")
    tr = clean.copy()
    print(f"(дальнейшие разрезы — ТОЛЬКО чистые {len(tr)} сделок)")

print("\n── схема × исход ──")
g = tr.groupby(["scheme", "exit_reason"]).agg(
    n=("trade_id", "size"), pnl=("pnl_usd", "sum"), avg=("pnl_usd", "mean"),
    hold_med_s=("hold_ms", lambda x: x.median() / 1000))
print(g.round(3).to_string())

print("\n── атрибуция долларов ──")
att = tr.groupby("scheme")[["entry_slip_usd", "exit_slip_usd", "trim_usd",
                            "fees_usd", "funding_usd", "pnl_usd"]].sum()
print(att.round(2).to_string())

print("\n── converged под микроскопом ──")
for sch in tr.scheme.unique():
    cv = tr[(tr.scheme == sch) & (tr.exit_reason == "converged")]
    if not len(cv):
        continue
    print(f"{sch}: n={len(cv)} theor_net {cv.theor_net_pct.mean():.3f}% | "
          f"pnl_avg {cv.pnl_usd.mean():+.3f}$ | entry_slip {cv.entry_slip_usd.mean():+.3f}$ "
          f"| exit_slip {cv.exit_slip_usd.mean():+.3f}$ | fees {cv.fees_usd.mean():.3f}$ "
          f"| notional_avg {cv.notional_usd.mean():.0f}$")

print("\n── худшие связки эпохи ──")
pl = tr.groupby(["sym", "buy_ex", "sell_ex"]).agg(n=("trade_id", "size"),
                                                  pnl=("pnl_usd", "sum"))
print(pl.sort_values("pnl").head(8).round(2).to_string())

od = pd.read_sql("SELECT venue,status,otype,leg FROM orders WHERE created_ms>=?",
                 db, params=(int(run.ts),))
ент = od[(od.otype == "ioc") & od.leg.str.startswith("entry")]
if len(ент):
    mv = ент.groupby("venue").agg(sent=("status", "size"),
                                  miss=("status", lambda s: (s == "missed").mean() * 100))
    print("\n── промахи IOC по биржам, % ──")
    print(mv.round(1).to_string())

mk = pd.read_sql("SELECT * FROM markouts WHERE ts>=?", db, params=(int(run.ts),))
if len(mk):
    print(f"\nмаркауты эпохи: n={len(mk)}, медиана {mk.markout_bps.median():+.1f} б.п., "
          f"p10 {mk.markout_bps.quantile(.1):+.1f}, p90 {mk.markout_bps.quantile(.9):+.1f}")
