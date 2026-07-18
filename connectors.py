# -*- coding: utf-8 -*-
"""
Коннекторы бирж: WS bookTicker (best bid/ask) + REST-справочники контрактов и цен.

Сопоставление активов между биржами — по МЕТАДАННЫМ контракта, а не по строке:
  * кратные тикеры нормализуются масштабом из имени (1000PEPEUSDT -> PEPEUSDT,
    scale=1000; 1MBABYDOGE -> BABYDOGE, scale=1e6): цены делятся на масштаб,
    размеры умножаются — все котировки приводятся к цене ОДНОГО токена;
  * размеры контрактов приводятся к токенам (Gate: quanto_multiplier,
    OKX: ctVal);
  * при старте сканер сверяет цены одного актива на всех биржах (fetch_prices):
    нога, чья цена отличается от медианы >5%, уходит в карантин — ловит
    коллизии тикеров (разные проекты под одним символом) и ошибки масштаба.

Общий контракт: engine.on_quote(exch, sym, bid, bid_qty_tokens, ask,
ask_qty_tokens, exch_ts_ms) — цены за 1 токен, размеры в токенах.
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

# порядок важен: длинные префиксы первыми
_PREFIX_SCALES = (("1000000", 1e6), ("10000", 1e4), ("1000", 1e3), ("1M", 1e6))


def split_scale(sym: str) -> tuple[str, float]:
    """'1000PEPEUSDT' -> ('PEPEUSDT', 1000.0); '1INCHUSDT' -> ('1INCHUSDT', 1.0)."""
    for p, k in _PREFIX_SCALES:
        rest = sym[len(p):]
        if sym.startswith(p) and len(rest) >= 5 and rest.endswith("USDT"):
            return rest, k
    return sym, 1.0


class Connector:
    name = "base"
    taker = 0.0005

    def __init__(self, engine, session: aiohttp.ClientSession, symbols: list[str],
                 native_map: dict | None = None):
        self.engine = engine
        self.session = session
        self.symbols = symbols                          # канонические (нормализованные)
        self.native_map = native_map or {}              # canon -> dict(native, scale, ...)
        self.scales = {c: m.get("scale", 1.0) for c, m in self.native_map.items()}
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
        k = self.scales.get(sym, 1.0)
        self.n_msg += 1
        self.engine.on_quote(self.name, sym, bid / k, bq * k, ask / k, aq * k, exch_ts)


# ═══════════════════ BINANCE USDT-M ═══════════════════
class Binance(Connector):
    name = "binance"
    taker = 0.0005
    REST = "https://fapi.binance.com"
    WS = "wss://fstream.binance.com/stream"
    CHUNK = 80

    @classmethod
    async def fetch_symbols(cls, session) -> dict[str, dict]:
        async with session.get(f"{cls.REST}/fapi/v1/exchangeInfo",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            info = await r.json()
        out = {}
        for s in info.get("symbols", []):
            if (s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT"
                    and s.get("status") == "TRADING"):
                canon, scale = split_scale(s["symbol"])
                out[canon] = dict(native=s["symbol"], scale=scale)
        return out

    async def _connect_once(self):
        native = [self.native_map[s]["native"] for s in self.symbols if s in self.native_map]
        to_canon = {self.native_map[s]["native"]: s for s in self.symbols if s in self.native_map}
        chunks = [native[i:i + self.CHUNK] for i in range(0, len(native), self.CHUNK)]

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
                        canon = to_canon.get(d["s"])
                        if canon:
                            self.emit(canon, float(d["b"]), float(d["B"]),
                                      float(d["a"]), float(d["A"]),
                                      int(d.get("T") or d.get("E") or 0))

        await asyncio.gather(*(one(c) for c in chunks))

    @classmethod
    async def fetch_funding(cls, session) -> dict[str, float]:
        async with session.get(f"{cls.REST}/fapi/v1/premiumIndex",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            rows = await r.json()
        out = {}
        for x in rows:
            if isinstance(x, dict) and x.get("symbol"):
                canon, _ = split_scale(x["symbol"])
                out[canon] = float(x.get("lastFundingRate") or 0)
        return out

    @classmethod
    async def fetch_prices(cls, session) -> dict[str, float]:
        async with session.get(f"{cls.REST}/fapi/v1/ticker/price",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            rows = await r.json()
        out = {}
        for x in rows:
            canon, k = split_scale(x.get("symbol", ""))
            if canon.endswith("USDT"):
                out[canon] = float(x["price"]) / k
        return out


# ═══════════════════ BYBIT V5 LINEAR ═══════════════════
class Bybit(Connector):
    name = "bybit"
    taker = 0.00055
    REST = "https://api.bybit.com"
    WS = "wss://stream.bybit.com/v5/public/linear"

    @classmethod
    async def fetch_symbols(cls, session) -> dict[str, dict]:
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
                    canon, scale = split_scale(s["symbol"])
                    out[canon] = dict(native=s["symbol"], scale=scale)
            cursor = res.get("nextPageCursor") or ""
            if not cursor:
                return out

    async def _connect_once(self):
        to_canon = {self.native_map[s]["native"]: s for s in self.symbols if s in self.native_map}
        native = list(to_canon)
        state: dict[str, dict] = {}
        async with self.session.ws_connect(self.WS, heartbeat=None) as ws:
            for i in range(0, len(native), 10):
                await ws.send_json({"op": "subscribe",
                                    "args": [f"tickers.{s}" for s in native[i:i + 10]]})

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
                    nat = d.get("symbol") or topic.split(".", 1)[1]
                    canon = to_canon.get(nat)
                    if not canon:
                        continue
                    st = state.setdefault(nat, {})
                    st.update({k: v for k, v in d.items() if v not in ("", None)})
                    if all(k in st for k in ("bid1Price", "ask1Price")):
                        self.emit(canon, float(st["bid1Price"]), float(st.get("bid1Size") or 0),
                                  float(st["ask1Price"]), float(st.get("ask1Size") or 0),
                                  int(j.get("ts") or 0))
            finally:
                ping_task.cancel()

    @classmethod
    async def fetch_funding(cls, session) -> dict[str, float]:
        async with session.get(f"{cls.REST}/v5/market/tickers?category=linear",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            j = await r.json()
        out = {}
        for x in j.get("result", {}).get("list", []):
            canon, _ = split_scale(x.get("symbol", ""))
            out[canon] = float(x.get("fundingRate") or 0)
        return out

    @classmethod
    async def fetch_prices(cls, session) -> dict[str, float]:
        async with session.get(f"{cls.REST}/v5/market/tickers?category=linear",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            j = await r.json()
        out = {}
        for x in j.get("result", {}).get("list", []):
            canon, k = split_scale(x.get("symbol", ""))
            if x.get("lastPrice"):
                out[canon] = float(x["lastPrice"]) / k
        return out


# ═══════════════════ GATE USDT FUTURES ═══════════════════
class Gate(Connector):
    name = "gate"
    taker = 0.0005
    REST = "https://api.gateio.ws/api/v4"
    WS = "wss://fx-ws.gateio.ws/v4/ws/usdt"

    @classmethod
    async def fetch_symbols(cls, session) -> dict[str, dict]:
        async with session.get(f"{cls.REST}/futures/usdt/contracts",
                               timeout=aiohttp.ClientTimeout(total=20)) as r:
            rows = await r.json()
        out = {}
        for s in rows:
            if s.get("in_delisting"):
                continue
            name = s.get("name", "")                    # BTC_USDT
            canon, scale = split_scale(name.replace("_", ""))
            out[canon] = dict(native=name, scale=scale,
                              qmult=float(s.get("quanto_multiplier") or 1))
        return out

    def __init__(self, engine, session, symbols, native_map=None):
        super().__init__(engine, session, symbols, native_map)
        self.native = [self.native_map[s]["native"] for s in symbols if s in self.native_map]
        self.to_canon = {m["native"]: c for c, m in self.native_map.items()}
        self.qmult = {c: m.get("qmult", 1.0) for c, m in self.native_map.items()}

    CHUNK = 100                                        # подписок на WS-соединение

    async def _connect_once(self):
        chunks = [self.native[i:i + self.CHUNK] for i in range(0, len(self.native), self.CHUNK)]

        async def one(chunk):
            async with self.session.ws_connect(self.WS, heartbeat=None) as ws:
                for i in range(0, len(chunk), 50):
                    await ws.send_json({"time": int(time.time()),
                                        "channel": "futures.book_ticker",
                                        "event": "subscribe",
                                        "payload": chunk[i:i + 50]})

                async def pinger():
                    while True:
                        await asyncio.sleep(PING_EVERY)
                        await ws.send_json({"time": int(time.time()),
                                            "channel": "futures.ping"})

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
                        canon = self.to_canon.get(d.get("s", ""))
                        if not canon:
                            continue
                        bid = float(d.get("b") or 0)
                        ask = float(d.get("a") or 0)
                        if bid <= 0 or ask <= 0:
                            continue
                        qm = self.qmult.get(canon, 1.0)  # контракты -> токены
                        self.emit(canon, bid, float(d.get("B") or 0) * qm,
                                  ask, float(d.get("A") or 0) * qm, int(d.get("t") or 0))
                finally:
                    ping_task.cancel()

        await asyncio.gather(*(one(c) for c in chunks))

    @classmethod
    async def fetch_funding(cls, session) -> dict[str, float]:
        async with session.get(f"{cls.REST}/futures/usdt/contracts",
                               timeout=aiohttp.ClientTimeout(total=20)) as r:
            rows = await r.json()
        out = {}
        for s in rows:
            if s.get("name"):
                canon, _ = split_scale(s["name"].replace("_", ""))
                out[canon] = float(s.get("funding_rate") or 0)
        return out

    @classmethod
    async def fetch_prices(cls, session) -> dict[str, float]:
        async with session.get(f"{cls.REST}/futures/usdt/tickers",
                               timeout=aiohttp.ClientTimeout(total=20)) as r:
            rows = await r.json()
        out = {}
        for x in rows:
            canon, k = split_scale(x.get("contract", "").replace("_", ""))
            if x.get("last"):
                out[canon] = float(x["last"]) / k
        return out


# ═══════════════════ OKX SWAP ═══════════════════
class Okx(Connector):
    name = "okx"
    taker = 0.0005
    REST = "https://www.okx.com"
    WS = "wss://ws.okx.com:8443/ws/v5/public"

    @classmethod
    async def fetch_symbols(cls, session) -> dict[str, dict]:
        async with session.get(f"{cls.REST}/api/v5/public/instruments?instType=SWAP",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            j = await r.json()
        out = {}
        for it in j.get("data", []):
            iid = it.get("instId", "")
            if (it.get("state") == "live" and it.get("ctType") == "linear"
                    and it.get("settleCcy") == "USDT" and iid.endswith("-USDT-SWAP")):
                canon, scale = split_scale(iid[:-10].replace("-", "") + "USDT")
                out[canon] = dict(native=iid, scale=scale,
                                  ctval=float(it.get("ctVal") or 1.0))
        return out

    def __init__(self, engine, session, symbols, native_map=None):
        super().__init__(engine, session, symbols, native_map)
        self.items = [(s, self.native_map[s]["native"], self.native_map[s]["ctval"])
                      for s in symbols if s in self.native_map]
        self.to_canon = {inst: (canon, ctval) for canon, inst, ctval in self.items}

    CHUNK = 100                                        # инструментов на WS-соединение

    async def _connect_once(self):
        insts = [inst for _, inst, _ in self.items]
        chunks = [insts[i:i + self.CHUNK] for i in range(0, len(insts), self.CHUNK)]

        async def one(chunk):
            async with self.session.ws_connect(self.WS, heartbeat=None) as ws:
                await ws.send_json({"op": "subscribe",
                                    "args": [{"channel": "bbo-tbt", "instId": x}
                                             for x in chunk]})

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

        await asyncio.gather(*(one(c) for c in chunks))

    @classmethod
    async def fetch_funding(cls, session) -> dict[str, float]:
        return {}                                       # per-instrument API — добавим позже

    @classmethod
    async def fetch_prices(cls, session) -> dict[str, float]:
        async with session.get(f"{cls.REST}/api/v5/market/tickers?instType=SWAP",
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            j = await r.json()
        out = {}
        for x in j.get("data", []):
            iid = x.get("instId", "")
            if iid.endswith("-USDT-SWAP") and x.get("last"):
                canon, k = split_scale(iid[:-10].replace("-", "") + "USDT")
                out[canon] = float(x["last"]) / k
        return out


# ═══════════════════ MEXC FUTURES (вне дефолта: медленный тикер-канал) ═══════════════════
class Mexc(Connector):
    name = "mexc"
    taker = 0.0002
    REST = "https://contract.mexc.com"
    WS = "wss://contract.mexc.com/edge"
    CHUNK = 120

    @classmethod
    async def fetch_symbols(cls, session) -> dict[str, dict]:
        async with session.get(f"{cls.REST}/api/v1/contract/detail",
                               timeout=aiohttp.ClientTimeout(total=20)) as r:
            j = await r.json()
        out = {}
        for d in j.get("data", []):
            if d.get("quoteCoin") == "USDT" and d.get("state", 0) == 0:
                canon, scale = split_scale(d["symbol"].replace("_", ""))
                out[canon] = dict(native=d["symbol"], scale=scale,
                                  csize=float(d.get("contractSize") or 1))
        return out

    def __init__(self, engine, session, symbols, native_map=None):
        super().__init__(engine, session, symbols, native_map)
        self.native = [self.native_map[s]["native"] for s in symbols if s in self.native_map]
        self.to_canon = {m["native"]: c for c, m in self.native_map.items()}

    async def _connect_once(self):
        chunks = [self.native[i:i + self.CHUNK] for i in range(0, len(self.native), self.CHUNK)]

        async def one(chunk):
            async with self.session.ws_connect(self.WS, heartbeat=None) as ws:
                for i, sym in enumerate(chunk):
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
        out = {}
        for d in j.get("data", []):
            if d.get("symbol"):
                canon, _ = split_scale(d["symbol"].replace("_", ""))
                out[canon] = float(d.get("fundingRate") or 0)
        return out

    @classmethod
    async def fetch_prices(cls, session) -> dict[str, float]:
        async with session.get(f"{cls.REST}/api/v1/contract/ticker",
                               timeout=aiohttp.ClientTimeout(total=20)) as r:
            j = await r.json()
        out = {}
        for d in j.get("data", []):
            canon, k = split_scale(d.get("symbol", "").replace("_", ""))
            if d.get("lastPrice"):
                out[canon] = float(d["lastPrice"]) / k
        return out


# ═══════════════════ BITGET USDT-FUTURES ═══════════════════
class Bitget(Connector):
    name = "bitget"
    taker = 0.0006
    REST = "https://api.bitget.com"
    WS = "wss://ws.bitget.com/v2/ws/public"

    @classmethod
    async def fetch_symbols(cls, session) -> dict[str, dict]:
        async with session.get(
                f"{cls.REST}/api/v2/mix/market/contracts?productType=usdt-futures",
                timeout=aiohttp.ClientTimeout(total=20)) as r:
            j = await r.json()
        out = {}
        for d in j.get("data", []):
            if d.get("quoteCoin") == "USDT" and d.get("symbolStatus") in ("normal", None):
                canon, scale = split_scale(d["symbol"])
                out[canon] = dict(native=d["symbol"], scale=scale)
        return out

    CHUNK = 100                                        # подписок на WS-соединение

    async def _connect_once(self):
        to_canon = {self.native_map[s]["native"]: s for s in self.symbols if s in self.native_map}
        native = list(to_canon)
        chunks = [native[i:i + self.CHUNK] for i in range(0, len(native), self.CHUNK)]

        async def one(chunk):
            async with self.session.ws_connect(self.WS, heartbeat=None) as ws:
                for i in range(0, len(chunk), 50):
                    await ws.send_json({"op": "subscribe",
                                        "args": [{"instType": "USDT-FUTURES",
                                                  "channel": "books1", "instId": s}
                                                 for s in chunk[i:i + 50]]})

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
                        canon = to_canon.get(inst)
                        data = j.get("data") or []
                        if not canon or not data:
                            continue
                        d = data[0]
                        bids, asks = d.get("bids") or [], d.get("asks") or []
                        if not bids or not asks:
                            continue
                        self.emit(canon, float(bids[0][0]), float(bids[0][1]),
                                  float(asks[0][0]), float(asks[0][1]),
                                  int(d.get("ts") or 0))
                finally:
                    ping_task.cancel()

        await asyncio.gather(*(one(c) for c in chunks))

    @classmethod
    async def fetch_funding(cls, session) -> dict[str, float]:
        async with session.get(
                f"{cls.REST}/api/v2/mix/market/tickers?productType=usdt-futures",
                timeout=aiohttp.ClientTimeout(total=20)) as r:
            j = await r.json()
        out = {}
        for d in j.get("data", []):
            if d.get("symbol"):
                canon, _ = split_scale(d["symbol"])
                out[canon] = float(d.get("fundingRate") or 0)
        return out

    @classmethod
    async def fetch_prices(cls, session) -> dict[str, float]:
        async with session.get(
                f"{cls.REST}/api/v2/mix/market/tickers?productType=usdt-futures",
                timeout=aiohttp.ClientTimeout(total=20)) as r:
            j = await r.json()
        out = {}
        for d in j.get("data", []):
            canon, k = split_scale(d.get("symbol", ""))
            if d.get("lastPr"):
                out[canon] = float(d["lastPr"]) / k
        return out


CONNECTORS = {"binance": Binance, "bybit": Bybit, "gate": Gate,
              "okx": Okx, "mexc": Mexc, "bitget": Bitget}
