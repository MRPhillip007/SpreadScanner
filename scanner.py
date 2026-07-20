# -*- coding: utf-8 -*-
"""
SpreadScanner — фаза 0: событийный сканер/рекордер кросс-биржевых спредов перпов.

Что делает:
  * WS bookTicker со всех подключённых бирж -> нормализованные котировки в памяти;
  * на каждом тике инкрементально пересчитывает спреды пар (только пары с
    обновившимся инструментом);
  * СОБЫТИЕ = валовый спред (bid дорогой / ask дешёвой - 1) > open_gross_pct,
    закрывается при < close_gross_pct (гистерезис). Внутри события пишется
    полная детализация котировок обеих ног;
  * каждую секунду — снапшот всех котировок (parquet, файл на час);
  * раз в 60с — фандинги всех бирж (parquet);
  * журнал событий-возможностей — SQLite (start/end/длительность/максимум/
    глубина у вершины/чистый спред после 4 тейкер-комиссий);
  * каждые 30с — счётчики сообщений и лаг до каждой биржи в консоль.

Запуск:  python scanner.py [--minutes 0] [--max-symbols 0] [--exchanges binance,bybit,gate]
Стоп:    Ctrl+C (буферы дописываются на диск).
"""
from __future__ import annotations

import argparse
import asyncio
import os
import signal
import sqlite3
import sys
import time
from dataclasses import dataclass, field

import aiohttp
import numpy as np
import pandas as pd

from connectors import CONNECTORS

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(ROOT, "data")

OPEN_GROSS = float(os.environ.get("OPEN_GROSS_PCT", "0.30")) / 100   # порог события
                                 # (на быстром сервере можно опустить: OPEN_GROSS_PCT=0.20)
CLOSE_GROSS = 0.10 / 100         # порог закрытия (гистерезис)
MAX_QUOTE_AGE_MS = 10_000        # встречная нога свежее N мс, иначе пара не считается
                                 # (bookTicker пушится на каждое изменение: в активном
                                 # рынке котировки текут постоянно; старше 10с = мёртвый
                                 # фид или замёрзший стакан — не сигнал, а фантом)
SANITY_GROSS = 15.0 / 100        # спред больше 15% = разные активы под одним тикером
                                 # (мемкоин-коллизии: AI, ANTHROPIC...) -> карантин пары
FEED_HEALTH_MS = 3_000           # коннектор жив = ЛЮБОЕ сообщение за последние 3с
                                 # (на активном соединении сотни символов -> тишина 3с
                                 # значит болен сам фид, а не «стакан не менялся»)
IDENT_TOL = 0.05                 # стартовая сверка активов: цена ноги vs медиана бирж
MIN_EVENT_MS = 0                 # события короче — тоже пишем (фильтруем потом)
SNAP_EVERY = 1.0                 # сек между снапшотами
FLUSH_EVERY = 60.0               # сек между сбросами parquet-буферов
FUNDING_EVERY = 60.0
STATS_EVERY = 30.0


def now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class Quote:
    bid: float = 0.0
    bq: float = 0.0
    ask: float = 0.0
    aq: float = 0.0
    exch_ts: int = 0
    loc_ts: int = 0


@dataclass
class Event:
    sym: str
    buy_ex: str                   # где покупаем (дешёвый ask)
    sell_ex: str                  # где продаём (дорогой bid)
    t_open: int
    max_gross: float = 0.0
    t_max: int = 0
    bid_at_max: float = 0.0
    ask_at_max: float = 0.0
    bq_at_max: float = 0.0
    aq_at_max: float = 0.0
    n_ticks: int = 0
    sum_gross: float = 0.0


class Engine:
    def __init__(self, fees: dict[str, float], verbose_events: bool = True):
        self.books: dict[str, dict[str, Quote]] = {}       # sym -> exch -> Quote
        self.fees = fees
        self.events: dict[tuple, Event] = {}               # (sym,buy,sell) -> Event
        self.banned: set[tuple] = set()                    # (sym, exA, exB) карантин
        self.conn_last: dict[str, int] = {}                # exch -> ms последнего сообщения
        # хуки форвард-слоя (не должны кидать исключения в горячий путь)
        self.on_event_open = None                          # (Event, buy_q, sell_q, loc)
        self.on_event_tick = None                          # (Event, buy_q, sell_q, loc)
        self.on_event_close = None                         # (Event, loc, forced)
        self.on_quote_hook = None                          # (exch, sym, Quote, loc)
        self.snap_buf: list = []
        self.tick_buf: list = []                           # full-res внутри событий
        self.fund_buf: list = []
        self.done_events: list = []
        self.verbose = verbose_events
        self.t0 = time.time()

    def log(self, s: str):
        print(f"[{time.strftime('%H:%M:%S')}] {s}", flush=True)

    # ── горячий путь ──
    def on_quote(self, exch, sym, bid, bq, ask, aq, exch_ts):
        loc = now_ms()
        self.conn_last[exch] = loc
        q = self.books.setdefault(sym, {}).setdefault(exch, Quote())
        q.bid, q.bq, q.ask, q.aq, q.exch_ts, q.loc_ts = bid, bq, ask, aq, exch_ts, loc
        book = self.books[sym]
        if len(book) < 2:
            return
        for other, oq in book.items():
            if other == exch or oq.ask <= 0 or oq.bid <= 0 or ask <= 0 or bid <= 0:
                continue
            if loc - self.conn_last.get(other, 0) > FEED_HEALTH_MS:
                continue                                   # фид встречной биржи болен
            if loc - oq.loc_ts > MAX_QUOTE_AGE_MS:
                continue                                   # конкретная подписка молчит
            # направление 1: купить на exch (ask), продать на other (bid)
            self._check(sym, exch, other, q, oq, loc)
            # направление 2: купить на other, продать на exch
            self._check(sym, other, exch, oq, q, loc)
        if self.on_quote_hook is not None:
            self.on_quote_hook(exch, sym, q, loc)

    def _check(self, sym, buy_ex, sell_ex, bq_, sq_, loc):
        pkey = (sym,) + tuple(sorted((buy_ex, sell_ex)))
        if pkey in self.banned:
            return
        gross = sq_.bid / bq_.ask - 1.0
        if gross > SANITY_GROSS:                           # разные активы под тикером
            self.banned.add(pkey)
            self.events.pop((sym, buy_ex, sell_ex), None)
            self.events.pop((sym, sell_ex, buy_ex), None)
            self.log(f"КАРАНТИН {sym} {buy_ex}<->{sell_ex}: спред {gross*100:+.0f}% — "
                     f"похоже, разные активы под одним тикером")
            return
        key = (sym, buy_ex, sell_ex)
        ev = self.events.get(key)
        if ev is None:
            if gross > OPEN_GROSS:
                ev = Event(sym, buy_ex, sell_ex, loc)
                self.events[key] = ev
                self._upd(ev, gross, bq_, sq_, loc)
                if self.on_event_open is not None:
                    self.on_event_open(ev, bq_, sq_, loc)
        else:
            if gross < CLOSE_GROSS:
                self._close(key, ev, loc)
            else:
                self._upd(ev, gross, bq_, sq_, loc)

    def _upd(self, ev: Event, gross, bq_, sq_, loc):
        ev.n_ticks += 1
        ev.sum_gross += gross
        if gross > ev.max_gross:
            ev.max_gross = gross
            ev.t_max = loc
            ev.bid_at_max, ev.ask_at_max = sq_.bid, bq_.ask
            ev.bq_at_max, ev.aq_at_max = sq_.bq, bq_.aq
        self.tick_buf.append((loc, ev.sym, ev.buy_ex, ev.sell_ex,
                              bq_.ask, bq_.aq, sq_.bid, sq_.bq, gross))
        if self.on_event_tick is not None:
            self.on_event_tick(ev, bq_, sq_, loc)

    def _close(self, key, ev: Event, loc, forced: bool = False):
        del self.events[key]
        dur = loc - ev.t_open
        if dur < MIN_EVENT_MS:
            return
        fee = 2 * (self.fees.get(ev.buy_ex, 5e-4) + self.fees.get(ev.sell_ex, 5e-4))
        net = ev.max_gross - fee
        rec = dict(sym=ev.sym, buy_ex=ev.buy_ex, sell_ex=ev.sell_ex, forced=int(forced),
                   t_open=ev.t_open, t_close=loc, dur_ms=dur,
                   max_gross_pct=ev.max_gross * 100, net_after_4taker_pct=net * 100,
                   mean_gross_pct=ev.sum_gross / max(ev.n_ticks, 1) * 100,
                   n_ticks=ev.n_ticks,
                   sell_bid_at_max=ev.bid_at_max, buy_ask_at_max=ev.ask_at_max,
                   sell_bidqty_at_max=ev.bq_at_max, buy_askqty_at_max=ev.aq_at_max,
                   usd_at_top=min(ev.bq_at_max * ev.bid_at_max,
                                  ev.aq_at_max * ev.ask_at_max))
        self.done_events.append(rec)
        if self.on_event_close is not None:
            self.on_event_close(ev, loc, forced)
        if self.verbose and not forced:
            self.log(f"СОБЫТИЕ {ev.sym} buy@{ev.buy_ex}/sell@{ev.sell_ex}: "
                     f"max {ev.max_gross*100:+.2f}% (net {net*100:+.2f}%), "
                     f"{dur/1000:.1f}с, ~${rec['usd_at_top']:,.0f} у вершины")

    # ── фоновые контуры ──
    def snapshot(self):
        loc = now_ms()
        for sym, book in self.books.items():
            for exch, q in book.items():
                if q.bid > 0:
                    self.snap_buf.append((loc, exch, sym, q.bid, q.bq, q.ask, q.aq,
                                          q.exch_ts, q.loc_ts))

    def take_buffers(self) -> dict:
        """Атомарно (в event-loop'е) забрать буферы для записи в фоновом потоке."""
        out = dict(snap=self.snap_buf, ticks=self.tick_buf,
                   fund=self.fund_buf, events=self.done_events)
        self.snap_buf, self.tick_buf, self.fund_buf, self.done_events = [], [], [], []
        return out


class Store:
    def __init__(self):
        os.makedirs(DATA, exist_ok=True)
        self.db = sqlite3.connect(os.path.join(DATA, "opportunities.db"),
                                  check_same_thread=False)
        self.db.execute("""CREATE TABLE IF NOT EXISTS events(
            sym TEXT, buy_ex TEXT, sell_ex TEXT, forced INT, t_open INT, t_close INT,
            dur_ms INT, max_gross_pct REAL, net_after_4taker_pct REAL,
            mean_gross_pct REAL, n_ticks INT, sell_bid_at_max REAL,
            buy_ask_at_max REAL, sell_bidqty_at_max REAL, buy_askqty_at_max REAL,
            usd_at_top REAL)""")
        try:
            self.db.execute("ALTER TABLE events ADD COLUMN forced INT DEFAULT 0")
        except sqlite3.OperationalError:
            pass                                           # колонка уже есть
        self.db.execute("CREATE INDEX IF NOT EXISTS ix_ev_t ON events(t_open)")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.commit()

    def write(self, bufs: dict):
        """Чистый I/O: зовётся из ФОНОВОГО потока. Каждый сброс — НОВЫЙ файл-кусок
        (никаких перечитываний растущих parquet — они блокировали event-loop
        на секунды и роняли все сокеты разом). Анализ читает по glob-маске."""
        tag = time.strftime("%Y%m%d_%H%M%S")
        if bufs.get("events"):
            pd.DataFrame(bufs["events"]).to_sql("events", self.db,
                                                if_exists="append", index=False)
            self.db.commit()
        if bufs.get("snap"):
            pd.DataFrame(bufs["snap"], columns=["ts", "exch", "sym", "bid", "bq",
                                                "ask", "aq", "exch_ts", "loc_ts"]
                         ).to_parquet(os.path.join(DATA, f"snap_{tag}.parquet"), index=False)
        if bufs.get("ticks"):
            pd.DataFrame(bufs["ticks"], columns=["ts", "sym", "buy_ex", "sell_ex",
                                                 "buy_ask", "buy_askqty",
                                                 "sell_bid", "sell_bidqty", "gross"]
                         ).to_parquet(os.path.join(DATA, f"event_ticks_{tag}.parquet"), index=False)
        if bufs.get("fund"):
            pd.DataFrame(bufs["fund"], columns=["ts", "exch", "sym", "funding"]
                         ).to_parquet(os.path.join(DATA, f"funding_{tag}.parquet"), index=False)

    def flush(self, eng: Engine):
        """Синхронный путь (останов/малые прогоны)."""
        self.write(eng.take_buffers())


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=0, help="0 = бесконечно")
    ap.add_argument("--max-symbols", type=int, default=0, help="кап общих символов (0=все)")
    # mexc исключён из дефолта: его тикер-канал ~1 сообщение/сек/символ с лагом
    # 1-2.5с — медленная нога рождает фантомные спреды; вернём после sub.depth
    ap.add_argument("--exchanges", type=str, default="binance,bybit,gate,okx,bitget")
    args = ap.parse_args()
    want = [x.strip() for x in args.exchanges.split(",") if x.strip() in CONNECTORS]

    conn = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver(), limit=64)
    async with aiohttp.ClientSession(connector=conn) as session:
        # ── справочники символов ──
        sym_maps: dict[str, dict[str, str]] = {}
        for ex in list(want):
            try:
                sym_maps[ex] = await CONNECTORS[ex].fetch_symbols(session)
                print(f"{ex}: {len(sym_maps[ex])} перпов", flush=True)
            except Exception as e:
                print(f"{ex}: справочник недоступен ({type(e).__name__}: {e}) — пропускаю биржу",
                      flush=True)
                want.remove(ex)
        if len(want) < 2:
            print("нужно >=2 биржи — выходим")
            return
        common: set[str] = set()
        for a in want:
            for b in want:
                if a < b:
                    common |= set(sym_maps[a]) & set(sym_maps[b])
        common_l = sorted(common)
        if args.max_symbols:
            common_l = common_l[:args.max_symbols]
        print(f"символов на >=2 биржах: {len(common)} (берём {len(common_l)})", flush=True)

        fees = {ex: CONNECTORS[ex].taker for ex in want}
        eng = Engine(fees)
        store = Store()

        # ── сверка идентичности активов (метаданные + цены, а не строка тикера) ──
        merged = sum(1 for ex in want for s in common_l
                     if s in sym_maps[ex] and sym_maps[ex][s].get("scale", 1) > 1)
        print(f"кратных контрактов склеено масштабом: {merged}", flush=True)
        price_maps = {}
        for ex in want:
            try:
                price_maps[ex] = await CONNECTORS[ex].fetch_prices(session)
            except Exception as e:
                print(f"{ex}: цены для сверки недоступны ({type(e).__name__})", flush=True)
                price_maps[ex] = {}
        nban = 0
        for s in common_l:
            ps = {ex: price_maps[ex][s] for ex in want if s in price_maps.get(ex, {})}
            if len(ps) < 2:
                continue
            med = float(np.median(list(ps.values())))
            for ex, p in ps.items():
                if med > 0 and abs(p / med - 1) > IDENT_TOL:
                    for other in want:
                        if other != ex:
                            eng.banned.add((s,) + tuple(sorted((ex, other))))
                    nban += 1
                    print(f"  идентичность: {s}@{ex} last={p:.6g} vs медиана {med:.6g} "
                          f"({(p/med-1)*100:+.1f}%) — нога в карантине", flush=True)
        print(f"сверка активов: в карантине ног {nban}", flush=True)
        conns = []
        for ex in want:
            syms = [s for s in common_l if s in sym_maps[ex]]
            conns.append(CONNECTORS[ex](eng, session, syms, sym_maps[ex]))
        tasks = [asyncio.create_task(c.run()) for c in conns]

        async def snapper():
            while True:
                await asyncio.sleep(SNAP_EVERY)
                eng.snapshot()

        async def flusher():
            while True:
                await asyncio.sleep(FLUSH_EVERY)
                bufs = eng.take_buffers()              # swap в event-loop'е — мгновенно
                await asyncio.to_thread(store.write, bufs)

        async def funder():
            while True:
                for ex in want:
                    try:
                        f = await CONNECTORS[ex].fetch_funding(session)
                        ts = now_ms()
                        eng.fund_buf += [(ts, ex, s, r) for s, r in f.items()
                                         if s in common]
                    except Exception as e:
                        eng.log(f"[{ex}] funding: {type(e).__name__}")
                await asyncio.sleep(FUNDING_EVERY)

        async def stats():
            last = {c.name: 0 for c in conns}
            while True:
                await asyncio.sleep(STATS_EVERY)
                parts = []
                for c in conns:
                    rate = (c.n_msg - last[c.name]) / STATS_EVERY
                    last[c.name] = c.n_msg
                    # лаг: медиана (loc - exch_ts) по свежим котировкам
                    lags = [q.loc_ts - q.exch_ts for b in eng.books.values()
                            for e, q in b.items() if e == c.name and q.exch_ts > 0]
                    lag = f"{np.median(lags):.0f}ms" if lags else "—"
                    parts.append(f"{c.name} {rate:,.0f}/с lag~{lag}")
                eng.log("поток: " + " | ".join(parts) +
                        f" | активных событий {len(eng.events)}")

        tasks += [asyncio.create_task(snapper()), asyncio.create_task(flusher()),
                  asyncio.create_task(funder()), asyncio.create_task(stats())]
        try:
            if args.minutes > 0:
                await asyncio.sleep(args.minutes * 60)
            else:
                await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()
            # закрыть висящие события (с пометкой forced) и дописать буферы
            for key, ev in list(eng.events.items()):
                eng._close(key, ev, now_ms(), forced=True)
            store.flush(eng)
            n = store.db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            eng.log(f"стоп. событий в журнале: {n}. данные: {DATA}")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
