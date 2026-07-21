# -*- coding: utf-8 -*-
"""
FORWARD — бумажная торговля кросс-биржевых спредов поверх сканера. Фонд-грейд
телеметрия: журнал РЕШЕНИЙ (вкл. отказы с причиной), полный жизненный цикл
ордеров, атрибуция PnL по компонентам, leg-risk инциденты, RTT-пробы бирж,
эквити по биржам, здоровье фидов. Всё в data/forward.db (веб читает её же).

Модель исполнения (консервативная):
  * вход: две IOC-лимитки по увиденным ценам, «прибытие» через измеренную
    односторонку (RTT/2 из живых проб); налив решается по ПЕРВОЙ котировке
    после прибытия (тихий стакан >quiet_ms — по последней известной);
    частичный налив = размер вершины стакана;
  * несимметричные наливы: излишек большей ноги немедленно закрывается
    тейкером (trim), стоимость — в атрибуцию;
  * одна нога — авто-хедж: закрыть налитое тейкером, инцидент leg_risk;
  * выход: конвергенция/стоп/таймаут — обе ноги тейкером с той же моделью.

Запуск: python forward.py [--exchanges gate,okx,bitget] [--entry-net 0.10]
Дашборд: python web.py  ->  http://127.0.0.1:8100
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass, field

import aiohttp
import numpy as np
import yaml

import scanner as SC
from connectors import CONNECTORS

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(ROOT, "data")
DB = os.path.join(DATA, "forward.db")

RTT_PROBE = {                                      # лёгкий эндпоинт для замера RTT
    "binance": "/fapi/v1/time", "bybit": "/v5/market/time",
    "okx": "/api/v5/public/time", "gate": "/spot/time",
    "mexc": "/api/v1/contract/ping", "bitget": "/api/v2/public/time",
}
FUNDING_HOURS = (0, 8, 16)                         # приближение: начисление в 00/08/16 UTC


def now_ms() -> int:
    return int(time.time() * 1000)


# ═══════════════════ персистентность ═══════════════════
class FStore:
    def __init__(self):
        os.makedirs(DATA, exist_ok=True)
        self.db = sqlite3.connect(DB)
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS config_runs(run_id TEXT, ts INT, cfg_hash TEXT, cfg_json TEXT);
        CREATE TABLE IF NOT EXISTS decisions(ts INT, run TEXT, sym TEXT, buy_ex TEXT,
            sell_ex TEXT, gross_pct REAL, net_pct REAL, cap_usd REAL,
            fund_buy REAL, fund_sell REAL, action TEXT, trade_id INT);
        CREATE TABLE IF NOT EXISTS orders(oid INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id INT, leg TEXT, venue TEXT, sym TEXT, side TEXT, otype TEXT,
            limit_px REAL, qty REAL, decision_px REAL, created_ms INT, arrival_ms INT,
            resolved_ms INT, status TEXT, fill_px REAL, fill_qty REAL, fee_usd REAL);
        CREATE TABLE IF NOT EXISTS trades(trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run TEXT, sym TEXT, buy_ex TEXT, sell_ex TEXT, t_open INT, t_close INT,
            status TEXT, qty REAL, notional_usd REAL, theor_gross_pct REAL,
            theor_net_pct REAL, entry_slip_usd REAL DEFAULT 0, exit_slip_usd REAL DEFAULT 0,
            trim_usd REAL DEFAULT 0, fees_usd REAL DEFAULT 0, funding_usd REAL DEFAULT 0,
            pnl_usd REAL, exit_reason TEXT, hold_ms INT, scheme TEXT DEFAULT 'taker');
        CREATE TABLE IF NOT EXISTS incidents(ts INT, kind TEXT, trade_id INT, detail TEXT);
        CREATE TABLE IF NOT EXISTS markouts(ts INT, trade_id INT, sym TEXT, venue TEXT,
            fill_px REAL, ref0 REAL, ref_after REAL, markout_bps REAL);
        CREATE TABLE IF NOT EXISTS venue_rtt(ts INT, venue TEXT, rtt_ms REAL);
        CREATE TABLE IF NOT EXISTS equity(ts INT, venue TEXT, cash REAL);
        CREATE TABLE IF NOT EXISTS feeds(ts INT, venue TEXT, rate REAL, lag_ms REAL);
        CREATE INDEX IF NOT EXISTS ix_dec_ts ON decisions(ts);
        CREATE INDEX IF NOT EXISTS ix_tr_ts ON trades(t_open);
        """)
        try:
            self.db.execute("ALTER TABLE trades ADD COLUMN scheme TEXT DEFAULT 'taker'")
        except sqlite3.OperationalError:
            pass
        self.db.execute("PRAGMA journal_mode=WAL")      # читатели не блокируются
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.commit()

    def q(self, sql, args=()):
        cur = self.db.execute(sql, args)
        self.db.commit()
        return cur


# ═══════════════════ бумажный брокер ═══════════════════
@dataclass
class POrder:
    oid: int
    trade_id: int
    leg: str                      # entry_buy/entry_sell/exit_buy/exit_sell/hedge/trim
    venue: str
    sym: str
    side: str                     # buy/sell
    otype: str                    # ioc (лимит) / taker (по рынку)
    limit_px: float | None
    qty: float
    decision_px: float
    created: int
    arrival: int
    status: str = "pending"
    fill_px: float = 0.0
    fill_qty: float = 0.0
    fee_usd: float = 0.0


@dataclass
class Trade:
    trade_id: int
    sym: str
    buy_ex: str
    sell_ex: str
    t_open: int
    qty: float = 0.0              # согласованный размер (токены)
    notional: float = 0.0
    theor_gross: float = 0.0
    theor_net: float = 0.0
    status: str = "entering"      # entering/open/exiting/closed/...
    entry_orders: list = field(default_factory=list)
    exit_orders: list = field(default_factory=list)
    buy_fill: float = 0.0
    sell_fill: float = 0.0
    entry_slip: float = 0.0
    exit_slip: float = 0.0
    trim: float = 0.0
    fees: float = 0.0
    funding: float = 0.0
    last_fund_mark: int = 0
    exit_reason: str = ""
    scheme: str = "taker"


class Forward:
    def __init__(self, cfg: dict, eng: SC.Engine, store: FStore, fees: dict[str, float],
                 run_id: str):
        self.cfg = cfg
        self.eng = eng
        self.store = store
        self.fees = fees
        self.run = run_id
        self.cash: dict[str, float] = {}
        self.rtt: dict[str, float] = {}
        self.funding: dict[str, dict[str, float]] = {}          # venue -> canon -> rate
        self.pending: dict[tuple, list[POrder]] = {}            # (venue,sym) -> orders
        self.makers: dict[str, list[POrder]] = {}               # sym -> пассивные заявки
        self.trades: dict[int, Trade] = {}                      # открытые/входящие
        self.cooldown: dict[tuple, int] = {}                    # (sym,exA,exB) -> until ms
        self.last_eval: dict[tuple, int] = {}
        self.day_pnl: dict[int, float] = {}
        self.pair_hist: dict[tuple, list] = {}                  # профиль связки: t_close событий
        self.by_sym: dict[str, list] = {}                       # sym -> [trade_id] открытых
        self.mk_watch: list = []                                # маркауты: (due, tid, sym, ...)
        self.maker_fees = {}
        self.n_orders = 0
        eng.on_event_open = self._on_event
        eng.on_event_tick = self._on_event
        eng.on_event_close = self._on_event_close
        eng.on_quote_hook = self._on_quote

    # ---------- профиль связки и выбор схемы ----------
    def _on_event_close(self, ev, loc, forced):
        if forced:
            return
        pk = (ev.sym,) + tuple(sorted((ev.buy_ex, ev.sell_ex)))
        h = self.pair_hist.setdefault(pk, [])
        h.append(loc)
        w = self.cfg["scheme"]["osc_window_min"] * 60_000
        self.pair_hist[pk] = [t for t in h if t > loc - w]

    def _pick_scheme(self, sym, exA, exB, loc) -> str:
        mode = self.cfg["scheme"]["mode"]
        if mode in ("taker", "maker"):
            return mode
        pk = (sym,) + tuple(sorted((exA, exB)))
        w = self.cfg["scheme"]["osc_window_min"] * 60_000
        n = sum(1 for t in self.pair_hist.get(pk, []) if t > loc - w)
        return "maker" if n >= self.cfg["scheme"]["osc_min_events"] else "taker"

    # ---------- журнал решений ----------
    def _decide(self, ts, sym, b, s, gross, net, cap, action, trade_id=None):
        f_b = self.funding.get(b, {}).get(sym, 0.0)
        f_s = self.funding.get(s, {}).get(sym, 0.0)
        self.store.q("INSERT INTO decisions VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                     (ts, self.run, sym, b, s, gross * 100, net * 100, cap,
                      f_b, f_s, action, trade_id))

    # ---------- вход: оценка события ----------
    def _on_event(self, ev, bq, sq, loc):
        try:
            self._eval_entry(ev, bq, sq, loc)
        except Exception as e:                                  # хук не роняет горячий путь
            self.eng.log(f"eval error: {type(e).__name__}: {e}")

    def _eval_entry(self, ev, bq, sq, loc):
        key = (ev.sym, ev.buy_ex, ev.sell_ex)
        if loc - self.last_eval.get(key, 0) < self.cfg["entry"]["retry_ms"]:
            return
        self.last_eval[key] = loc
        c = self.cfg
        gross = sq.bid / bq.ask - 1.0
        fee = 2 * (self.fees[ev.buy_ex] + self.fees[ev.sell_ex])
        # полный ожидаемый захват: минус остаток на выходе и ВНУТРЕННИЕ спреды
        # обеих бирж (тейкер-выход продаёт в бид и откупает по аску своей биржи)
        exit_cost = ((bq.ask - bq.bid) / bq.ask + (sq.ask - sq.bid) / sq.bid
                     + c["exit"]["converge_gross_pct"] / 100)
        net = gross - fee - exit_cost
        cap = min(bq.aq * bq.ask, sq.bq * sq.bid) * c["paper"]["top_book_frac"]
        if any(t.sym == ev.sym and t.status != "closed" for t in self.trades.values()):
            return                                              # уже в связке по символу
        if net * 100 < c["entry"]["net_min_pct"]:
            self._decide(loc, *key, gross, net, cap, "deny:net_below_min")
            return
        if loc < self.cooldown.get(key, 0) or loc < self.cooldown.get(
                (ev.sym,) + tuple(sorted((ev.buy_ex, ev.sell_ex))), 0):
            self._decide(loc, *key, gross, net, cap, "deny:cooldown")
            return
        if sum(1 for t in self.trades.values() if t.status != "closed") >= c["risk"]["max_concurrent"]:
            self._decide(loc, *key, gross, net, cap, "deny:concurrent_limit")
            return
        day = int(loc // 86_400_000)
        if self.day_pnl.get(day, 0.0) <= -c["risk"]["daily_loss_limit_usd"]:
            self._decide(loc, *key, gross, net, cap, "deny:daily_loss_limit")
            return
        notional = min(c["paper"]["notional_usd"], cap)
        if notional < c["paper"]["min_notional_usd"]:
            self._decide(loc, *key, gross, net, cap, "deny:capacity")
            return
        for ex in (ev.buy_ex, ev.sell_ex):
            used = sum(t.notional for t in self.trades.values()
                       if t.status != "closed" and ex in (t.buy_ex, t.sell_ex))
            if used + notional > c["risk"]["per_venue_exposure_usd"]:
                self._decide(loc, *key, gross, net, cap, f"deny:venue_exposure:{ex}")
                return
        # ── выбор схемы по профилю связки ──
        scheme = self._pick_scheme(ev.sym, ev.buy_ex, ev.sell_ex, loc)
        if scheme == "maker":
            self._enter_maker(ev, bq, sq, loc, key, gross, cap, notional)
            return
        # ── TAKER-TAKER: две IOC-ноги ──
        qty = notional / bq.ask
        cur = self.store.q(
            "INSERT INTO trades(run,sym,buy_ex,sell_ex,t_open,status,qty,notional_usd,"
            "theor_gross_pct,theor_net_pct,pnl_usd,scheme) VALUES(?,?,?,?,?,?,?,?,?,?,0,'taker')",
            (self.run, ev.sym, ev.buy_ex, ev.sell_ex, loc, "entering", qty, notional,
             gross * 100, net * 100))
        tid = cur.lastrowid
        tr = Trade(tid, ev.sym, ev.buy_ex, ev.sell_ex, loc, qty=qty, notional=notional,
                   theor_gross=gross, theor_net=net, last_fund_mark=loc)
        tr.scheme = "taker"
        self.trades[tid] = tr
        self.by_sym.setdefault(ev.sym, []).append(tid)
        self._decide(loc, *key, gross, net, cap, "trade_taker", tid)
        tr.entry_orders = [
            self._submit(tid, "entry_buy", ev.buy_ex, ev.sym, "buy", "ioc",
                         bq.ask, qty, bq.ask, loc),
            self._submit(tid, "entry_sell", ev.sell_ex, ev.sym, "sell", "ioc",
                         sq.bid, qty, sq.bid, loc),
        ]
        self.eng.log(f"ВХОД[taker] #{tid} {ev.sym} buy@{ev.buy_ex} {bq.ask:.6g} / "
                     f"sell@{ev.sell_ex} {sq.bid:.6g} qty={qty:.4g} "
                     f"(${notional:.0f}, net {net*100:+.2f}%)")

    # ---------- MAKER-FIRST: пассивная нога на дешёвой бирже ----------
    def _enter_maker(self, ev, bq, sq, loc, key, gross, cap, notional):
        c = self.cfg["maker"]
        P = bq.bid                                       # встаём в лучший бид (мейкер)
        fee_cycle = (self.maker_fees.get(ev.buy_ex, 2e-4) + self.fees[ev.sell_ex]
                     + self.fees[ev.buy_ex] + self.fees[ev.sell_ex])
        exit_cost = ((bq.ask - bq.bid) / bq.ask + (sq.ask - sq.bid) / sq.bid
                     + self.cfg["exit"]["converge_gross_pct"] / 100)
        exp_net = sq.bid / P - 1.0 - fee_cycle - exit_cost
        if exp_net * 100 < c["net_min_pct"]:
            self._decide(loc, *key, gross, exp_net, cap, "deny:maker_net")
            return
        qty = notional / P
        cur = self.store.q(
            "INSERT INTO trades(run,sym,buy_ex,sell_ex,t_open,status,qty,notional_usd,"
            "theor_gross_pct,theor_net_pct,pnl_usd,scheme) VALUES(?,?,?,?,?,?,?,?,?,?,0,'maker')",
            (self.run, ev.sym, ev.buy_ex, ev.sell_ex, loc, "maker_wait", qty, notional,
             gross * 100, exp_net * 100))
        tid = cur.lastrowid
        tr = Trade(tid, ev.sym, ev.buy_ex, ev.sell_ex, loc, qty=qty, notional=notional,
                   theor_gross=gross, theor_net=exp_net, last_fund_mark=loc)
        tr.scheme = "maker"
        tr.status = "maker_wait"
        self.trades[tid] = tr
        self.by_sym.setdefault(ev.sym, []).append(tid)
        self._decide(loc, *key, gross, exp_net, cap, "trade_maker", tid)
        self.n_orders += 1
        ow = self.rtt.get(ev.buy_ex, self.cfg["latency"]["default_one_way_ms"])
        o = POrder(self.n_orders, tid, "entry_maker", ev.buy_ex, ev.sym, "buy", "maker",
                   P, qty, P, loc, int(loc + ow))
        tr.entry_orders = [o]
        self.makers.setdefault(ev.sym, []).append(o)
        self.eng.log(f"ВХОД[maker] #{tid} {ev.sym}: лимитка buy@{ev.buy_ex} {P:.6g} "
                     f"(референс sell@{ev.sell_ex} {sq.bid:.6g}, ож.net {exp_net*100:+.2f}%)")

    def _maker_tick(self, sym, loc):
        """Каждый тик символа: наливы/репрайсы/отмены пассивных заявок."""
        lst = self.makers.get(sym)
        if not lst:
            return
        c = self.cfg["maker"]
        for o in list(lst):
            if o.arrival > loc:
                continue                                 # заявка ещё летит на биржу
            tr = self.trades.get(o.trade_id)
            if tr is None:
                lst.remove(o)
                continue
            book = self.eng.books.get(sym, {})
            bq, sq = book.get(tr.buy_ex), book.get(tr.sell_ex)
            if not bq or not sq or bq.bid <= 0 or sq.bid <= 0:
                continue
            if o.leg != "entry_maker":                   # пассивные ВЫХОДНЫЕ лимитки
                self._exit_maker_tick(o, tr, lst, book, loc)
                continue
            # 1) налив: цена прошла СКВОЗЬ наш уровень (строгое правило очереди)
            if bq.ask < o.limit_px * (1 - 1e-12):
                lst.remove(o)
                o.status, o.fill_px, o.fill_qty = "filled", o.limit_px, o.qty
                o.fee_usd = o.qty * o.limit_px * self.maker_fees.get(o.venue, 2e-4)
                tr.fees += o.fee_usd
                self.cash[o.venue] = self.cash.get(o.venue, 0.0) - o.fee_usd
                self._order_row(o, loc)
                h = self._submit(tr.trade_id, "entry_sell", tr.sell_ex, sym, "sell",
                                 "taker", None, o.qty, sq.bid, loc)
                tr.entry_orders = [o, h]
                tr.status = "entering"
                self.mk_watch.append((loc + c["markout_s"] * 1000, tr.trade_id, sym,
                                      tr.sell_ex, o.limit_px, sq.bid))
                self.eng.log(f"НАЛИВ[maker] #{tr.trade_id} {sym} @{o.limit_px:.6g} — хеджирую")
                continue
            # 2) отмена: ожидаемый чистый упал ниже порога или TTL
            fee_cycle = (self.maker_fees.get(tr.buy_ex, 2e-4) + self.fees[tr.sell_ex]
                         + self.fees[tr.buy_ex] + self.fees[tr.sell_ex])
            exp_net = sq.bid / o.limit_px - 1.0 - fee_cycle
            if exp_net * 100 < c["cancel_net_pct"] or loc - o.created > c["ttl_s"] * 1000:
                lst.remove(o)
                o.status = "cancelled"
                self._order_row(o, loc)
                self._cancel_maker(tr, loc, "maker_cancel" if exp_net * 100
                                   < c["cancel_net_pct"] else "maker_ttl")
                continue
            # 3) репрайс: следуем за лучшим бидом (paper: очередь теряем, это ок)
            if abs(bq.bid / o.limit_px - 1) > 2e-4:
                o.limit_px = bq.bid

    def _exit_maker_tick(self, o: POrder, tr: Trade, lst, book, loc):
        """Пассивная закрывающая лимитка: налив по строгому проходу, репрайс за
        своей стороной, эскалация в тейкер по passive_ttl."""
        q = book.get(o.venue)
        if not q or q.bid <= 0:
            return
        through = (q.bid > o.limit_px * (1 + 1e-12)) if o.side == "sell" \
            else (q.ask < o.limit_px * (1 - 1e-12))
        if through:
            lst.remove(o)
            o.status, o.fill_px, o.fill_qty = "filled", o.limit_px, o.qty
            o.fee_usd = o.qty * o.limit_px * self.maker_fees.get(o.venue, 2e-4)
            tr.fees += o.fee_usd
            self.cash[o.venue] = self.cash.get(o.venue, 0.0) - o.fee_usd
            self._order_row(o, loc)
            if all(x.status != "pending" for x in tr.exit_orders):
                self._finish_exit(tr, loc)
            return
        if loc - o.created > self.cfg["exit"]["passive_ttl_min"] * 60_000:
            lst.remove(o)                                # эскалация в тейкер
            o.status = "cancelled"
            self._order_row(o, loc)
            dpx = q.bid if o.side == "sell" else q.ask
            t = self._submit(tr.trade_id, o.leg, o.venue, o.sym, o.side, "taker",
                             None, o.qty, dpx, loc)
            tr.exit_orders = [t if x is o else x for x in tr.exit_orders]
            self.eng.log(f"ЭСКАЛАЦИЯ #{tr.trade_id} {o.sym} {o.leg}: "
                         f"пассивный выход не налился {self.cfg['exit']['passive_ttl_min']}м — тейкер")
            return
        target = q.ask if o.side == "sell" else q.bid    # держимся у своей стороны
        if abs(target / o.limit_px - 1) > 2e-4:
            o.limit_px = target

    def _cancel_maker(self, tr: Trade, loc, reason):
        self.store.q("UPDATE trades SET t_close=?, status='closed', exit_reason=?, "
                     "pnl_usd=0, hold_ms=? WHERE trade_id=?",
                     (loc, reason, loc - tr.t_open, tr.trade_id))
        self.trades.pop(tr.trade_id, None)
        if tr.trade_id in self.by_sym.get(tr.sym, []):
            self.by_sym[tr.sym].remove(tr.trade_id)

    def _order_row(self, o: POrder, loc):
        self.store.q(
            "INSERT INTO orders(trade_id,leg,venue,sym,side,otype,limit_px,qty,"
            "decision_px,created_ms,arrival_ms,resolved_ms,status,fill_px,fill_qty,fee_usd) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (o.trade_id, o.leg, o.venue, o.sym, o.side, o.otype, o.limit_px, o.qty,
             o.decision_px, o.created, o.arrival, loc, o.status, o.fill_px, o.fill_qty,
             o.fee_usd))

    # ---------- ордера ----------
    def _submit(self, tid, leg, venue, sym, side, otype, limit_px, qty, decision_px, loc):
        self.n_orders += 1
        ow = self.rtt.get(venue, self.cfg["latency"]["default_one_way_ms"])
        o = POrder(self.n_orders, tid, leg, venue, sym, side, otype, limit_px, qty,
                   decision_px, loc, int(loc + ow))
        self.pending.setdefault((venue, sym), []).append(o)
        return o

    def _resolve(self, o: POrder, q: SC.Quote, loc):
        px = q.ask if o.side == "buy" else q.bid
        avail = q.aq if o.side == "buy" else q.bq
        filled = 0.0
        if o.otype == "taker" or (o.side == "buy" and px <= o.limit_px + 1e-15) \
                or (o.side == "sell" and px >= o.limit_px - 1e-15):
            filled = min(o.qty, avail if avail > 0 else o.qty)
        o.status = "filled" if filled >= o.qty * 0.999 else ("partial" if filled > 0 else "missed")
        o.fill_px, o.fill_qty = (px, filled) if filled > 0 else (0.0, 0.0)
        o.fee_usd = filled * px * self.fees[o.venue]
        self.store.q(
            "INSERT INTO orders(trade_id,leg,venue,sym,side,otype,limit_px,qty,"
            "decision_px,created_ms,arrival_ms,resolved_ms,status,fill_px,fill_qty,fee_usd) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (o.trade_id, o.leg, o.venue, o.sym, o.side, o.otype, o.limit_px, o.qty,
             o.decision_px, o.created, o.arrival, loc, o.status, o.fill_px, o.fill_qty,
             o.fee_usd))
        tr = self.trades.get(o.trade_id)
        if tr:
            tr.fees += o.fee_usd
            self.cash[o.venue] = self.cash.get(o.venue, 0.0) - o.fee_usd
            if tr.status == "entering" and all(x.status != "pending" for x in tr.entry_orders):
                self._finish_entry(tr, loc)
            elif tr.status == "exiting" and all(x.status != "pending" for x in tr.exit_orders):
                self._finish_exit(tr, loc)

    def _on_quote(self, exch, sym, q, loc):
        try:
            lst = self.pending.get((exch, sym))
            if lst:
                ready = [o for o in lst if o.arrival <= loc]
                for o in ready:
                    lst.remove(o)
                    self._resolve(o, q, loc)
            self._maker_tick(sym, loc)
            self._monitor_exits(exch, sym, loc)
        except Exception as e:
            self.eng.log(f"quote hook error: {type(e).__name__}: {e}")

    async def sweeper(self):
        """Тихие стаканы: пендинги старше quiet_ms решаются по последней котировке.
        Плюс таймауты позиций и фандинг-начисления."""
        quiet = self.cfg["latency"]["quiet_resolve_ms"]
        while True:
            await asyncio.sleep(0.2)
            loc = now_ms()
            for (venue, sym), lst in list(self.pending.items()):
                for o in [x for x in lst if loc - x.arrival > quiet]:
                    q = self.eng.books.get(sym, {}).get(venue)
                    if q and q.bid > 0:
                        lst.remove(o)
                        self._resolve(o, q, loc)
            for tr in list(self.trades.values()):
                if tr.status == "open":
                    if loc - tr.t_open > self.cfg["exit"]["timeout_min"] * 60_000:
                        self._start_exit(tr, loc, "timeout", style="maker")
                    else:
                        self._accrue_funding(tr, loc)
                        # carry против съел долю ожидаемого захвата -> пассивный выход
                        budget = tr.theor_net * tr.notional
                        if (tr.funding < 0 and budget > 0
                                and -tr.funding > self.cfg["exit"]["funding_eat_frac"] * budget):
                            self._start_exit(tr, loc, "funding_bleed", style="maker")
            for sym in list(self.makers):                       # TTL в тихих стаканах
                self._maker_tick(sym, loc)
            due = [m for m in self.mk_watch if m[0] <= loc]     # маркауты
            for m in due:
                self.mk_watch.remove(m)
                _, tid, sym, venue, fill_px, ref0 = m
                q = self.eng.books.get(sym, {}).get(venue)
                if q and q.bid > 0 and ref0 > 0:
                    self.store.q("INSERT INTO markouts VALUES(?,?,?,?,?,?,?,?)",
                                 (loc, tid, sym, venue, fill_px, ref0, q.bid,
                                  (q.bid / ref0 - 1) * 1e4))

    # ---------- завершение входа ----------
    def _finish_entry(self, tr: Trade, loc):
        ob, os_ = tr.entry_orders
        fb, fs = ob.fill_qty, os_.fill_qty
        if fb <= 0 and fs <= 0:
            tr.status = "closed"
            tr.exit_reason = "no_fill"
            self._close_trade(tr, loc, pnl_extra=0.0)
            return
        if fb <= 0 or fs <= 0:                                  # LEG RISK -> авто-хедж
            good = ob if fb > 0 else os_
            side = "sell" if good.side == "buy" else "buy"
            h = self._submit(tr.trade_id, "hedge", good.venue, tr.sym, side, "taker",
                             None, good.fill_qty, good.fill_px, loc)
            tr.exit_orders = [h]
            tr.status = "exiting"
            tr.exit_reason = "leg_risk"
            self.store.q("INSERT INTO incidents VALUES(?,?,?,?)",
                         (loc, "leg_risk", tr.trade_id,
                          json.dumps(dict(filled=good.leg, qty=good.fill_qty))))
            self.eng.log(f"LEG-RISK #{tr.trade_id} {tr.sym}: {good.leg} налился один — хеджирую")
            key = (tr.sym,) + tuple(sorted((tr.buy_ex, tr.sell_ex)))
            self.cooldown[key] = loc + self.cfg["risk"]["legrisk_cooldown_s"] * 1000
            return
        m = min(fb, fs)
        tr.qty = m
        tr.buy_fill, tr.sell_fill = ob.fill_px, os_.fill_px
        tr.entry_slip = (ob.fill_px - ob.decision_px) * m + (os_.decision_px - os_.fill_px) * m
        if fb > m + 1e-12 or fs > m + 1e-12:                    # трим излишка тейкером
            ex = ob if fb > fs else os_
            excess = abs(fb - fs)
            q = self.eng.books.get(tr.sym, {}).get(ex.venue)
            if q and q.bid > 0:
                px = q.bid if ex.side == "buy" else q.ask
                cost = excess * abs(px - ex.fill_px) + excess * px * self.fees[ex.venue]
                tr.trim += cost
                self.cash[ex.venue] -= cost
                self.store.q("INSERT INTO incidents VALUES(?,?,?,?)",
                             (loc, "trim", tr.trade_id,
                              json.dumps(dict(venue=ex.venue, excess=excess, cost=cost))))
        tr.status = "open"
        self.store.q("UPDATE trades SET status='open' WHERE trade_id=?", (tr.trade_id,))
        self.eng.log(f"ОТКРЫТА #{tr.trade_id} {tr.sym} qty={m:.4g} "
                     f"({tr.buy_ex} {ob.fill_px:.6g} / {tr.sell_ex} {os_.fill_px:.6g})")

    # ---------- выходы ----------
    def _monitor_exits(self, exch, sym, loc):
        tids = self.by_sym.get(sym)
        if not tids:
            return
        c = self.cfg["exit"]
        for tid in list(tids):
            tr = self.trades.get(tid)
            if tr is None or tr.status != "open" or exch not in (tr.buy_ex, tr.sell_ex):
                continue
            book = self.eng.books.get(sym, {})
            bq, sq = book.get(tr.buy_ex), book.get(tr.sell_ex)
            if not bq or not sq or bq.ask <= 0 or sq.bid <= 0:
                continue
            gross_now = sq.bid / bq.ask - 1.0
            if gross_now * 100 <= c["converge_gross_pct"]:
                self._start_exit(tr, loc, "converged", style="taker")
            elif (gross_now - tr.theor_gross) * 100 >= c["stop_widen_pct"]:
                self._start_exit(tr, loc, "stop_widen", style="taker")

    def _start_exit(self, tr: Trade, loc, reason, style: str = "taker"):
        tr.status = "exiting"
        tr.exit_reason = reason
        book = self.eng.books.get(tr.sym, {})
        bq, sq = book.get(tr.buy_ex), book.get(tr.sell_ex)
        if style == "maker" and bq and sq and bq.ask > 0 and sq.bid > 0:
            # пассивный выход: продаём лонг лимиткой в аск своей биржи,
            # откупаем шорт лимиткой в бид своей — экономим внутренние спреды
            o1 = self._mk_order(tr.trade_id, "exit_buy", tr.buy_ex, tr.sym, "sell",
                                bq.ask, tr.qty, loc)
            o2 = self._mk_order(tr.trade_id, "exit_sell", tr.sell_ex, tr.sym, "buy",
                                sq.bid, tr.qty, loc)
            tr.exit_orders = [o1, o2]
            self.eng.log(f"ВЫХОД[maker] #{tr.trade_id} {tr.sym} [{reason}]: "
                         f"sell@{tr.buy_ex} {bq.ask:.6g} / buy@{tr.sell_ex} {sq.bid:.6g}")
            return
        dpx_b = bq.bid if bq else tr.buy_fill
        dpx_s = sq.ask if sq else tr.sell_fill
        tr.exit_orders = [
            self._submit(tr.trade_id, "exit_buy", tr.buy_ex, tr.sym, "sell", "taker",
                         None, tr.qty, dpx_b, loc),
            self._submit(tr.trade_id, "exit_sell", tr.sell_ex, tr.sym, "buy", "taker",
                         None, tr.qty, dpx_s, loc),
        ]

    def _mk_order(self, tid, leg, venue, sym, side, px, qty, loc) -> POrder:
        self.n_orders += 1
        ow = self.rtt.get(venue, self.cfg["latency"]["default_one_way_ms"])
        o = POrder(self.n_orders, tid, leg, venue, sym, side, "maker", px, qty, px,
                   loc, int(loc + ow))
        self.makers.setdefault(sym, []).append(o)
        return o

    def _finish_exit(self, tr: Trade, loc):
        pnl = 0.0
        for o in tr.exit_orders:
            if o.leg == "hedge":
                pnl += (o.fill_px - tr_entry_px(tr, o)) * o.fill_qty * (1 if o.side == "sell" else -1)
            elif o.leg == "exit_buy":                            # продали лонг-ногу
                pnl += (o.fill_px - tr.buy_fill) * o.fill_qty
                tr.exit_slip += (o.decision_px - o.fill_px) * o.fill_qty
            elif o.leg == "exit_sell":                           # откупили шорт-ногу
                pnl += (tr.sell_fill - o.fill_px) * o.fill_qty
                tr.exit_slip += (o.fill_px - o.decision_px) * o.fill_qty
            self.cash[o.venue] = self.cash.get(o.venue, 0.0) + 0  # комиссии учтены в _resolve
        self._close_trade(tr, loc, pnl)

    def _close_trade(self, tr: Trade, loc, pnl_extra):
        pnl = pnl_extra - tr.fees - tr.trim + tr.funding
        tr.status = "closed"
        day = int(loc // 86_400_000)
        self.day_pnl[day] = self.day_pnl.get(day, 0.0) + pnl
        # кэш: PnL ног виртуально оседает поровну (учёт по биржам уточняет анализ ордеров)
        for v in {tr.buy_ex, tr.sell_ex}:
            self.cash[v] = self.cash.get(v, 0.0) + pnl_extra / 2 + tr.funding / 2
        self.store.q(
            "UPDATE trades SET t_close=?, status='closed', qty=?, entry_slip_usd=?, "
            "exit_slip_usd=?, trim_usd=?, fees_usd=?, funding_usd=?, pnl_usd=?, "
            "exit_reason=?, hold_ms=? WHERE trade_id=?",
            (loc, tr.qty, tr.entry_slip, tr.exit_slip, tr.trim, tr.fees, tr.funding,
             pnl, tr.exit_reason, loc - tr.t_open, tr.trade_id))
        key = (tr.sym,) + tuple(sorted((tr.buy_ex, tr.sell_ex)))
        self.cooldown.setdefault(key, 0)
        self.cooldown[key] = max(self.cooldown[key],
                                 loc + self.cfg["risk"]["pair_cooldown_s"] * 1000)
        self.trades.pop(tr.trade_id, None)
        if tr.trade_id in self.by_sym.get(tr.sym, []):
            self.by_sym[tr.sym].remove(tr.trade_id)
        self.eng.log(f"ЗАКРЫТА #{tr.trade_id} {tr.sym} [{tr.exit_reason}] "
                     f"PnL ${pnl:+.2f} (теор net {tr.theor_net*100:+.2f}%, "
                     f"слип вх ${tr.entry_slip:+.2f} / вых ${tr.exit_slip:+.2f}, "
                     f"комиссии ${tr.fees:.2f})")

    # ---------- фандинг (приближение: 00/08/16 UTC по последней ставке) ----------
    def _accrue_funding(self, tr: Trade, loc):
        last, cur = tr.last_fund_mark // 3_600_000, loc // 3_600_000
        for h in range(last + 1, cur + 1):
            if (h % 24) in FUNDING_HOURS:
                fb = self.funding.get(tr.buy_ex, {}).get(tr.sym, 0.0)
                fs = self.funding.get(tr.sell_ex, {}).get(tr.sym, 0.0)
                tr.funding += (-fb + fs) * tr.notional          # лонг платит fb, шорт получает fs
        tr.last_fund_mark = loc


def tr_entry_px(tr: Trade, hedge_o: POrder) -> float:
    return tr.entry_orders[0].fill_px if hedge_o.side == "sell" else tr.entry_orders[1].fill_px


# ═══════════════════ main ═══════════════════
async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--exchanges", type=str, default="gate,okx,bitget")
    ap.add_argument("--max-symbols", type=int, default=0)
    ap.add_argument("--minutes", type=float, default=0)
    ap.add_argument("--entry-net", type=float, default=None, help="override entry.net_min_pct")
    ap.add_argument("--scheme", choices=["auto", "taker", "maker"], default=None)
    args = ap.parse_args()

    cfg = yaml.safe_load(open(os.path.join(ROOT, "forward.yaml"), encoding="utf-8"))
    if args.entry_net is not None:
        cfg["entry"]["net_min_pct"] = args.entry_net
    if args.scheme is not None:
        cfg["scheme"]["mode"] = args.scheme
    cfg_json = json.dumps(cfg, sort_keys=True)
    cfg_hash = hashlib.sha256(cfg_json.encode()).hexdigest()[:12]
    run_id = f"{time.strftime('%Y%m%d_%H%M%S')}_{cfg_hash[:6]}"

    want = [x.strip() for x in args.exchanges.split(",") if x.strip() in CONNECTORS]
    conn = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver(), limit=64)
    async with aiohttp.ClientSession(connector=conn) as session:
        sym_maps = {}
        for ex in list(want):
            try:
                sym_maps[ex] = await CONNECTORS[ex].fetch_symbols(session)
                print(f"{ex}: {len(sym_maps[ex])} перпов", flush=True)
            except Exception as e:
                print(f"{ex}: {type(e).__name__}: {e} — пропускаю", flush=True)
                want.remove(ex)
        if len(want) < 2:
            print("нужно >=2 биржи")
            return
        common = set()
        for a in want:
            for b in want:
                if a < b:
                    common |= set(sym_maps[a]) & set(sym_maps[b])
        common_l = sorted(common)
        if args.max_symbols:
            common_l = common_l[:args.max_symbols]
        print(f"вселенная: {len(common_l)} символов, конфиг {cfg_hash}", flush=True)

        fees = {ex: CONNECTORS[ex].taker for ex in want}
        eng = SC.Engine(fees, verbose_events=False)
        sc_store = SC.Store()                          # слой-1: события/снапшоты/фандинг
        store = FStore()
        store.q("INSERT INTO config_runs VALUES(?,?,?,?)",
                (run_id, now_ms(), cfg_hash, cfg_json))
        fwd = Forward(cfg, eng, store, fees, run_id)
        fwd.maker_fees = {ex: CONNECTORS[ex].maker for ex in want}
        for ex in want:
            fwd.cash[ex] = float(cfg["paper"]["cash_per_venue_usd"])

        # сверка идентичности (как в сканере)
        for ex in want:
            try:
                pm = await CONNECTORS[ex].fetch_prices(session)
            except Exception:
                pm = {}
            sym_maps[ex + "_px"] = pm
        nban = 0
        for s in common_l:
            ps = {ex: sym_maps[ex + "_px"][s] for ex in want if s in sym_maps.get(ex + "_px", {})}
            if len(ps) < 2:
                continue
            med = float(np.median(list(ps.values())))
            for ex, p in ps.items():
                if med > 0 and abs(p / med - 1) > SC.IDENT_TOL:
                    for other in want:
                        if other != ex:
                            eng.banned.add((s,) + tuple(sorted((ex, other))))
                    nban += 1
        print(f"сверка активов: в карантине ног {nban}", flush=True)

        conns = [CONNECTORS[ex](eng, session, [s for s in common_l if s in sym_maps[ex]],
                                sym_maps[ex]) for ex in want]
        tasks = [asyncio.create_task(c.run()) for c in conns]
        tasks.append(asyncio.create_task(fwd.sweeper()))

        async def rtt_probe():
            while True:
                for ex in want:
                    try:
                        url = CONNECTORS[ex].REST + RTT_PROBE[ex]
                        t0 = time.perf_counter()
                        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                            await r.read()
                        rtt = (time.perf_counter() - t0) * 1000
                        fwd.rtt[ex] = rtt / 2                   # односторонка
                        store.q("INSERT INTO venue_rtt VALUES(?,?,?)", (now_ms(), ex, rtt))
                    except Exception:
                        pass
                await asyncio.sleep(60)

        async def funding_poll():
            while True:
                ts = now_ms()
                for ex in want:
                    try:
                        f = await CONNECTORS[ex].fetch_funding(session)
                        fwd.funding[ex] = f
                        eng.fund_buf += [(ts, ex, s, r) for s, r in f.items()
                                         if s in common]        # история в parquet
                    except Exception:
                        pass
                await asyncio.sleep(60)

        async def snapshots():
            last = {c.name: 0 for c in conns}
            while True:
                await asyncio.sleep(30)
                ts = now_ms()
                eng.snapshot()                          # быстрый снапшот книг (в лупе)
                bufs = eng.take_buffers()
                await asyncio.to_thread(sc_store.write, bufs)   # диск — в фоновом потоке
                for ex in want:
                    store.q("INSERT INTO equity VALUES(?,?,?)", (ts, ex, fwd.cash.get(ex, 0)))
                for c in conns:
                    rate = (c.n_msg - last[c.name]) / 30
                    last[c.name] = c.n_msg
                    lags = [q.loc_ts - q.exch_ts for b in eng.books.values()
                            for e, q in b.items() if e == c.name and q.exch_ts > 0]
                    store.q("INSERT INTO feeds VALUES(?,?,?,?)",
                            (ts, c.name, rate, float(np.median(lags)) if lags else -1))
                open_n = sum(1 for t in fwd.trades.values() if t.status != "closed")
                eng.log(f"эквити {sum(fwd.cash.values()):.2f} | связок открыто {open_n} | "
                        f"pending {sum(len(v) for v in fwd.pending.values())}")

        tasks += [asyncio.create_task(rtt_probe()), asyncio.create_task(funding_poll()),
                  asyncio.create_task(snapshots())]
        try:
            if args.minutes > 0:
                await asyncio.sleep(args.minutes * 60)
            else:
                await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()
            n = store.q("SELECT COUNT(*) c FROM trades WHERE status='closed'").fetchone()[0]
            d = store.q("SELECT COUNT(*) c FROM decisions").fetchone()[0]
            eng.log(f"стоп. сделок {n}, решений {d}, БД: {DB}")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        try:
            import ctypes
            ctypes.windll.winmm.timeBeginPeriod(1)       # таймер ОС 15.6мс -> 1мс
            ctypes.windll.kernel32.SetPriorityClass(     # HIGH_PRIORITY_CLASS
                ctypes.windll.kernel32.GetCurrentProcess(), 0x00000080)
        except Exception:
            pass
    else:
        try:
            import uvloop
            uvloop.install()
        except ImportError:
            pass
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
