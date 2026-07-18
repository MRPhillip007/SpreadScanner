# -*- coding: utf-8 -*-
"""
Коннекторы бирж: WS bookTicker (best bid/ask) + REST-справочники символов.

Общий контракт: коннектор нормализует тикер к каноническому виду (BTCUSDT),
на каждый апдейт зовёт engine.on_quote(exch, sym, bid, bq, ask, aq, exch_ts_ms).
Сопоставление символов между биржами — ТОЛЬКО точное совпадение строки
(1000PEPEUSDT != PEPEUSDT — защита от кратных контрактов).

Каждый коннектор — вечный reconnect-цикл: молчащий сокет = мёртвый сокет.
"""
from __future__ import annotations

import asyncio
import json
import time

import aiohttp

try:
    import orjson
    loads = orjson.loads
except ImportError:                                    # pragma: no cover
    loads = json.loads

WS_TIMEOUT = 60          # сек тишины -> реконнект
PING_EVERY = 15


class Connector:
    name = "base"
    taker = 0.0005

    def __init__(self, engine, session: aiohttp.ClientSession, symbols: list[str],
                 native_map: dict | None = None):
        self.engine = engine
        self.session = session
        self.symbols = symbols                          # канонические
        self.native_map = native_map or {}
        self.n_msg = 0

    async def run(self):
        backoff = 2
        while True:
            try:
                await self._connect_once()
                backoff = 2
            except asyncio.CancelledError:
                return
            except Exception as e:
                self.engine.log(f"[{self.name}] reconnect: {type(e).__name__}: {e}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

    async def _connect_once(self):                      # pragma: no cover
        raise NotImplementedError

    def emit(self, sym, bid, bq, ask, aq, exch_ts):
        self.n_msg += 1
        self.engine.on_quote(self.name, sym, bid, bq, ask, aq, exch_ts)


# ═══════════════════ BINANCE USDT-M ═══════════════════
class Binance(Connector):
    name = "binance"
    taker = 0.0005
    REST = "https://fapi.binance.com"
    WS = "wss://fstream.binance.com/stream"
    CHUNK = 80                                         # стримов на соединение

    @classmethod
    async def fetch_symbols(cls, session) -> dict[str, str]:
        async with session.get(f"{cls.REST}/fapi/v1/exchangeInfo",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            info = await r.json()
        out = {}
        for s in info.get("symbols", []):
            if (s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT"
                    and s.get("status") == "TRADING"):
                out[s["symbol"]] = s["symbol"]          # canon -> native
        return out

    async def _connect_once(self):
        chunks = [self.symbols[i:i + self.CHUNK] for i in range(0, len(self.symbols), self.CHUNK)]

        async def one(chunk):
            streams = "/".join(f"{s.lower()}@bookTicker" for s in chunk)
            async with self.session.ws_connect(f"{self.WS}?streams={streams}",
                                               heartbeat=20) as ws:
                while True:
                    msg = await asyncio.wait_for(ws.receive(), timeout=WS_TIMEOUT)
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        raise ConnectionError(f"ws type {msg.type}")
                    d = loads(msg.data).get("data", {})
                    if d.get("e") == "bookTicker":
                        self.emit(d["s"], float(d["b"]), float(d["B"]),
                                  float(d["a"]), float(d["A"]),
                                  int(d.get("T") or d.get("E") or 0))

        await asyncio.gather(*(one(c) for c in chunks))

    @classmethod
    async def fetch_funding(cls, session) -> dict[str, float]:
        async with session.get(f"{cls.REST}/fapi/v1/premiumIndex",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            rows = await r.json()
        return {x["symbol"]: float(x.get("lastFundingRate") or 0) for x in rows
                if isinstance(x, dict) and x.get("symbol")}


# ═══════════════════ BYBIT V5 LINEAR ═══════════════════
class Bybit(Connector):
    name = "bybit"
    taker = 0.00055
    REST = "https://api.bybit.com"
    WS = "wss://stream.bybit.com/v5/public/linear"

    @classmethod
    async def fetch_symbols(cls, session) -> dict[str, str]:
        out, cursor = {}, ""
        while True:
            url = (f"{cls.REST}/v5/market/instruments-info?category=linear&limit=1000"
                   + (f"&cursor={cursor}" if cursor else ""))
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                j = await r.json()
            res = j.get("result", {})
            for s in res.get("list", []):
                if (s.get("contractType") == "LinearPerpetual"
                        and s.get("settleCoin") == "USDT" and s.get("status") == "Trading"):
                    out[s["symbol"]] = s["symbol"]
            cursor = res.get("nextPageCursor") or ""
            if not cursor:
                return out

    async def _connect_once(self):
        state: dict[str, dict] = {}
        async with self.session.ws_connect(self.WS, heartbeat=None) as ws:
            for i in range(0, len(self.symbols), 10):
                await ws.send_json({"op": "subscribe",
                                    "args": [f"tickers.{s}" for s in self.symbols[i:i + 10]]})

            async def pinger():
                while True:
                    await asyncio.sleep(PING_EVERY)
                    await ws.send_json({"op": "ping"})

            ping_task = asyncio.create_task(pinger())
            try:
                while True:
                    msg = await asyncio.wait_for(ws.receive(), timeout=WS_TIMEOUT)
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        raise ConnectionError(f"ws type {msg.type}")
                    j = loads(msg.data)
                    topic = j.get("topic", "")
                    if not topic.startswith("tickers."):
                        continue
                    d = j.get("data") or {}
                    sym = d.get("symbol") or topic.split(".", 1)[1]
                    st = state.setdefault(sym, {})
                    st.update({k: v for k, v in d.items() if v not in ("", None)})
                    if all(k in st for k in ("bid1Price", "ask1Price")):
                        self.emit(sym, float(st["bid1Price"]), float(st.get("bid1Size") or 0),
                                  float(st["ask1Price"]), float(st.get("ask1Size") or 0),
                                  int(j.get("ts") or 0))
            finally:
                ping_task.cancel()

    @classmethod
    async def fetch_funding(cls, session) -> dict[str, float]:
        async with session.get(f"{cls.REST}/v5/market/tickers?category=linear",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            j = await r.json()
        return {x["symbol"]: float(x.get("fundingRate") or 0)
                for x in j.get("result", {}).get("list", [])}


# ═══════════════════ GATE USDT FUTURES ═══════════════════
class Gate(Connector):
    name = "gate"
    taker = 0.0005
    REST = "https://api.gateio.ws/api/v4"
    WS = "wss://fx-ws.gateio.ws/v4/ws/usdt"

    @classmethod
    async def fetch_symbols(cls, session) -> dict[str, str]:
        async with session.get(f"{cls.REST}/futures/usdt/contracts",
                               timeout=aiohttp.ClientTimeout(total=20)) as r:
            rows = await r.json()
        out = {}
        for s in rows:
            if s.get("in_delisting"):
                continue
            name = s.get("name", "")                    # BTC_USDT
            canon = name.replace("_", "")
            out[canon] = name
        return out

    def __init__(self, engine, session, symbols, native_map: dict[str, str] | None = None):
        super().__init__(engine, session, symbols, native_map)
        nm = self.native_map
        self.native = [nm[s] for s in symbols if s in nm]
        self.to_canon = {v: k for k, v in nm.items()}

    async def _connect_once(self):
        async with self.session.ws_connect(self.WS, heartbeat=None) as ws:
            for i in range(0, len(self.native), 50):
                await ws.send_json({"time": int(time.time()),
                                    "channel": "futures.book_ticker",
                                    "event": "subscribe",
                                    "payload": self.native[i:i + 50]})

            async def pinger():
                while True:
                    await asyncio.sleep(PING_EVERY)
                    await ws.send_json({"time": int(time.time()), "channel": "futures.ping"})

            ping_task = asyncio.create_task(pinger())
            try:
                while True:
                    msg = await asyncio.wait_for(ws.receive(), timeout=WS_TIMEOUT)
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        raise ConnectionError(f"ws type {msg.type}")
                    j = loads(msg.data)
                    if j.get("channel") != "futures.book_ticker" or j.get("event") != "update":
                        continue
                    d = j.get("result") or {}
                    sym = self.to_canon.get(d.get("s", ""))
                    if not sym:
                        continue
                    bid = float(d.get("b") or 0)
                    ask = float(d.get("a") or 0)
                    if bid <= 0 or ask <= 0:
                        continue
                    self.emit(sym, bid, float(d.get("B") or 0), ask, float(d.get("A") or 0),
                              int(d.get("t") or 0))
            finally:
                ping_task.cancel()

    @classmethod
    async def fetch_funding(cls, session) -> dict[str, float]:
        async with session.get(f"{cls.REST}/futures/usdt/contracts",
                               timeout=aiohttp.ClientTimeout(total=20)) as r:
            rows = await r.json()
        return {s["name"].replace("_", ""): float(s.get("funding_rate") or 0)
                for s in rows if s.get("name")}


# ═══════════════════ OKX SWAP ═══════════════════
class Okx(Connector):
    name = "okx"
    taker = 0.0005
    REST = "https://www.okx.com"
    WS = "wss://ws.okx.com:8443/ws/v5/public"

    @classmethod
    async def fetch_symbols(cls, session) -> dict:
        async with session.get(f"{cls.REST}/api/v5/public/instruments?instType=SWAP",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            j = await r.json()
        out = {}
        for it in j.get("data", []):
            iid = it.get("instId", "")
            if (it.get("state") == "live" and it.get("ctType") == "linear"
                    and it.get("settleCcy") == "USDT" and iid.endswith("-USDT-SWAP")):
                canon = iid[:-10].replace("-", "") + "USDT"
                out[canon] = (iid, float(it.get("ctVal") or 1.0))
        return out

    def __init__(self, engine, session, symbols, native_map=None):
        super().__init__(engine, session, symbols, native_map)
        self.items = [(s, *self.native_map[s]) for s in symbols if s in self.native_map]
        self.to_canon = {inst: (canon, ctval) for canon, inst, ctval in self.items}

    async def _connect_once(self):
        async with self.session.ws_connect(self.WS, heartbeat=None) as ws:
            insts = [inst for _, inst, _ in self.items]
            for i in range(0, len(insts), 100):
                await ws.send_json({"op": "subscribe",
                                    "args": [{"channel": "bbo-tbt", "instId": x}
                                             for x in insts[i:i + 100]]})

            async def pinger():
                while True:
                    await asyncio.sleep(20)
                    await ws.send_str("ping")

            ping_task = asyncio.create_task(pinger())
            try:
                while True:
                    msg = await asyncio.wait_for(ws.receive(), timeout=WS_TIMEOUT)
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        raise ConnectionError(f"ws type {msg.type}")
                    if msg.data == "pong":
                        continue
                    j = loads(msg.data)
                    if j.get("event"):
                        continue
                    inst = j.get("arg", {}).get("instId", "")
                    got = self.to_canon.get(inst)
                    data = j.get("data") or []
                    if not got or not data:
                        continue
                    canon, ctval = got
                    d = data[0]
                    bids, asks = d.get("bids") or [], d.get("asks") or []
                    if not bids or not asks:
                        continue
                    self.emit(canon, float(bids[0][0]), float(bids[0][1]) * ctval,
                              float(asks[0][0]), float(asks[0][1]) * ctval,
                              int(d.get("ts") or 0))
            finally:
                ping_task.cancel()

    @classmethod
    async def fetch_funding(cls, session) -> dict[str, float]:
        return {}                                       # per-instrument API — добавим позже


# ═══════════════════ MEXC FUTURES ═══════════════════
class Mexc(Connector):
    name = "mexc"
    taker = 0.0002
    REST = "https://contract.mexc.com"
    WS = "wss://contract.mexc.com/edge"

    @classmethod
    async def fetch_symbols(cls, session) -> dict[str, str]:
        async with session.get(f"{cls.REST}/api/v1/contract/detail",
                               timeout=aiohttp.ClientTimeout(total=20)) as r:
            j = await r.json()
        out = {}
        for d in j.get("data", []):
            if d.get("quoteCoin") == "USDT" and d.get("state", 0) == 0:
                out[d["symbol"].replace("_", "")] = d["symbol"]
        return out

    CHUNK = 120                                        # символов на соединение

    def __init__(self, engine, session, symbols, native_map=None):
        super().__init__(engine, session, symbols, native_map)
        self.native = [self.native_map[s] for s in symbols if s in self.native_map]
        self.to_canon = {v: k for k, v in self.native_map.items()
                         if k in set(symbols)}

    async def _connect_once(self):
        chunks = [self.native[i:i + self.CHUNK] for i in range(0, len(self.native), self.CHUNK)]

        async def one(chunk):
            async with self.session.ws_connect(self.WS, heartbeat=None) as ws:
                for i, sym in enumerate(chunk):        # per-symbol sub.ticker (bid1/ask1)
                    await ws.send_json({"method": "sub.ticker", "param": {"symbol": sym}})
                    if i % 20 == 19:
                        await asyncio.sleep(0.2)

                async def pinger():
                    while True:
                        await asyncio.sleep(PING_EVERY)
                        await ws.send_json({"method": "ping"})

                ping_task = asyncio.create_task(pinger())
                try:
                    while True:
                        msg = await asyncio.wait_for(ws.receive(), timeout=WS_TIMEOUT)
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            raise ConnectionError(f"ws type {msg.type}")
                        j = loads(msg.data)
                        if j.get("channel") != "push.ticker":
                            continue
                        d = j.get("data") or {}
                        canon = self.to_canon.get(d.get("symbol", ""))
                        if not canon:
                            continue
                        bid = float(d.get("bid1") or 0)
                        ask = float(d.get("ask1") or 0)
                        if bid > 0 and ask > 0:
                            self.emit(canon, bid, 0.0, ask, 0.0,
                                      int(d.get("timestamp") or j.get("ts") or 0))
                finally:
                    ping_task.cancel()

        await asyncio.gather(*(one(c) for c in chunks))

    @classmethod
    async def fetch_funding(cls, session) -> dict[str, float]:
        async with session.get(f"{cls.REST}/api/v1/contract/ticker",
                               timeout=aiohttp.ClientTimeout(total=20)) as r:
            j = await r.json()
        return {d["symbol"].replace("_", ""): float(d.get("fundingRate") or 0)
                for d in j.get("data", []) if d.get("symbol")}


# ═══════════════════ BITGET USDT-FUTURES ═══════════════════
class Bitget(Connector):
    name = "bitget"
    taker = 0.0006
    REST = "https://api.bitget.com"
    WS = "wss://ws.bitget.com/v2/ws/public"

    @classmethod
    async def fetch_symbols(cls, session) -> dict[str, str]:
        async with session.get(
                f"{cls.REST}/api/v2/mix/market/contracts?productType=usdt-futures",
                timeout=aiohttp.ClientTimeout(total=20)) as r:
            j = await r.json()
        out = {}
        for d in j.get("data", []):
            if d.get("quoteCoin") == "USDT" and d.get("symbolStatus") in ("normal", None):
                out[d["symbol"]] = d["symbol"]
        return out

    async def _connect_once(self):
        async with self.session.ws_connect(self.WS, heartbeat=None) as ws:
            for i in range(0, len(self.symbols), 50):
                await ws.send_json({"op": "subscribe",
                                    "args": [{"instType": "USDT-FUTURES",
                                              "channel": "books1", "instId": s}
                                             for s in self.symbols[i:i + 50]]})

            async def pinger():
                while True:
                    await asyncio.sleep(25)
                    await ws.send_str("ping")

            ping_task = asyncio.create_task(pinger())
            try:
                while True:
                    msg = await asyncio.wait_for(ws.receive(), timeout=WS_TIMEOUT)
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        raise ConnectionError(f"ws type {msg.type}")
                    if msg.data == "pong":
                        continue
                    j = loads(msg.data)
                    if j.get("event"):
                        continue
                    inst = j.get("arg", {}).get("instId", "")
                    data = j.get("data") or []
                    if not inst or not data:
                        continue
                    d = data[0]
                    bids, asks = d.get("bids") or [], d.get("asks") or []
                    if not bids or not asks:
                        continue
                    self.emit(inst, float(bids[0][0]), float(bids[0][1]),
                              float(asks[0][0]), float(asks[0][1]),
                              int(d.get("ts") or 0))
            finally:
                ping_task.cancel()

    @classmethod
    async def fetch_funding(cls, session) -> dict[str, float]:
        async with session.get(
                f"{cls.REST}/api/v2/mix/market/tickers?productType=usdt-futures",
                timeout=aiohttp.ClientTimeout(total=20)) as r:
            j = await r.json()
        return {d["symbol"]: float(d.get("fundingRate") or 0)
                for d in j.get("data", []) if d.get("symbol")}


CONNECTORS = {"binance": Binance, "bybit": Bybit, "gate": Gate,
              "okx": Okx, "mexc": Mexc, "bitget": Bitget}

