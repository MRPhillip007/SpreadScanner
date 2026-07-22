# -*- coding: utf-8 -*-
"""
Бенчмарк латентности до бирж С ЭТОГО СЕРВЕРА. Гонять перед форвардом на новой
машине и сравнивать между серверами (NL vs Tokyo).

Меряет по каждой бирже:
  * REST RTT: холодный (с TLS-хендшейком) и 15 тёплых запросов к /time
    (min ~ чистая сеть, median — рабочий, p99 — хвост);
  * сдвиг часов сервера к часам биржи (offset = server_time - (t0 + rtt/2)
    на самом быстром сэмпле — NTP-подход);
  * время установки WS (TCP+TLS+upgrade) — цена реконнекта;
  * лаг доставки WS-котировок С ПОПРАВКОЙ НА ЧАСЫ: (получено_локально −
    таймстемп_биржи − offset) по 40 сообщениям BTCUSDT (median/p95).

Запуск: python deploy/latency_bench.py    (~2 минуты)
"""
from __future__ import annotations

import asyncio
import json
import sys
import time

import aiohttp

try:
    import orjson
    loads = orjson.loads
except ImportError:
    loads = json.loads

N_REST = 15
N_WS = 40
WS_WAIT = 20            # сек максимум на сбор WS-сообщений

V = {
    "binance": dict(
        time="https://fapi.binance.com/fapi/v1/time",
        t=lambda j: float(j["serverTime"]),
        ws="wss://fstream.binance.com/ws/btcusdt@bookTicker", sub=None,
        ts=lambda j: float(j.get("T") or j.get("E") or 0)),
    "bybit": dict(
        time="https://api.bybit.com/v5/market/time",
        t=lambda j: float(j["result"]["timeNano"]) / 1e6,
        ws="wss://stream.bybit.com/v5/public/linear",
        sub={"op": "subscribe", "args": ["tickers.BTCUSDT"]},
        ts=lambda j: float(j.get("ts") or 0)),
    "okx": dict(
        time="https://www.okx.com/api/v5/public/time",
        t=lambda j: float(j["data"][0]["ts"]),
        ws="wss://ws.okx.com:8443/ws/v5/public",
        sub={"op": "subscribe", "args": [{"channel": "bbo-tbt", "instId": "BTC-USDT-SWAP"}]},
        ts=lambda j: float(j["data"][0]["ts"]) if j.get("data") else 0),
    "gate": dict(
        time="https://api.gateio.ws/api/v4/spot/time",
        t=lambda j: float(j["server_time"]),
        ws="wss://fx-ws.gateio.ws/v4/ws/usdt",
        sub={"time": 1, "channel": "futures.book_ticker", "event": "subscribe",
             "payload": ["BTC_USDT"]},
        ts=lambda j: float(j["result"]["t"]) if isinstance(j.get("result"), dict) else 0),
    "bitget": dict(
        time="https://api.bitget.com/api/v2/public/time",
        t=lambda j: float(j["data"]["serverTime"]),
        ws="wss://ws.bitget.com/v2/ws/public",
        sub={"op": "subscribe", "args": [{"instType": "USDT-FUTURES",
                                          "channel": "books1", "instId": "BTCUSDT"}]},
        ts=lambda j: float(j["data"][0]["ts"]) if j.get("data") else 0),
    # КАНДИДАТ на подключение: Aster (перп-DEX). API — клон Binance fapi,
    # поэтому конфиг отличается только хостом. Мерим ДО написания коннектора:
    # если задержка из Токио большая — площадка в лучшем случае пассивная.
    "aster": dict(
        time="https://fapi.asterdex.com/fapi/v1/time",
        t=lambda j: float(j["serverTime"]),
        ws="wss://fstream.asterdex.com/ws/btcusdt@bookTicker", sub=None,
        ts=lambda j: float(j.get("T") or j.get("E") or 0)),
}


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(int(len(xs) * p / 100), len(xs) - 1)]


async def bench_one(name, cfg, session):
    out = dict(venue=name)
    # ── REST: холодный + тёплые ──
    try:
        t0 = time.perf_counter()
        async with session.get(cfg["time"], timeout=aiohttp.ClientTimeout(total=10)) as r:
            await r.read()
        out["rest_cold"] = (time.perf_counter() - t0) * 1000
        rtts, offs, last_err = [], [], ""
        for _ in range(N_REST):
            await asyncio.sleep(0.25)                  # не долбить рейт-лимиты
            t0w = time.time() * 1000
            p0 = time.perf_counter()
            async with session.get(cfg["time"], timeout=aiohttp.ClientTimeout(total=10)) as r:
                body = await r.read()
            rtt = (time.perf_counter() - p0) * 1000
            try:
                j = loads(body)
                offs.append((cfg["t"](j) - (t0w + rtt / 2), rtt))
                rtts.append(rtt)
            except Exception:                          # плохой сэмпл — пропустить
                last_err = body[:80].decode(errors="replace")
        if len(rtts) < 5:
            out["rest_err"] = f"мало валидных сэмплов ({len(rtts)}): {last_err}"
            return out
        out["rest_min"] = min(rtts)
        out["rest_med"] = pct(rtts, 50)
        out["rest_p99"] = pct(rtts, 99)
        out["clock_off"] = min(offs, key=lambda x: x[1])[0]   # offset на лучшем сэмпле
    except Exception as e:
        out["rest_err"] = f"{type(e).__name__}"
        return out
    # ── WS: connect + лаг доставки с поправкой на часы ──
    try:
        p0 = time.perf_counter()
        async with session.ws_connect(cfg["ws"], timeout=aiohttp.ClientTimeout(total=10)) as ws:
            out["ws_conn"] = (time.perf_counter() - p0) * 1000
            if cfg["sub"]:
                await ws.send_json(cfg["sub"])
            lags, t_start, skipped = [], time.time(), 0
            while len(lags) < N_WS and time.time() - t_start < WS_WAIT:
                msg = await asyncio.wait_for(ws.receive(), timeout=10)
                if msg.type != aiohttp.WSMsgType.TEXT or msg.data == "pong":
                    continue
                loc = time.time() * 1000
                try:
                    ts = cfg["ts"](loads(msg.data))
                except Exception:
                    continue
                if ts > 0:
                    if skipped < 5:                    # снапшотный залп после подписки
                        skipped += 1
                        continue
                    # clock_off = часы_биржи − часы_сервера; переводим локальное
                    # время получения в часы биржи, поэтому ПЛЮС (был неверный знак:
                    # на машине с расхождением часов лаг выходил отрицательным)
                    lags.append(loc + out["clock_off"] - ts)
            if lags:
                out["ws_n"] = len(lags)
                out["ws_lag_med"] = pct(lags, 50)
                out["ws_lag_p95"] = pct(lags, 95)
    except Exception as e:
        out["ws_err"] = f"{type(e).__name__}"
    return out


async def main():
    conn = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver(), limit=16)
    async with aiohttp.ClientSession(connector=conn) as session:
        rows = []
        for name, cfg in V.items():
            print(f"меряю {name}...", flush=True)
            rows.append(await bench_one(name, cfg, session))
        print("\n" + "═" * 96)
        print(f"{'биржа':<9} {'REST хол.':>9} {'REST min':>9} {'med':>7} {'p99':>7} "
              f"{'WS conn':>8} {'часы off':>9} {'WS-лаг med':>11} {'p95':>7}")
        print("─" * 96)
        for o in rows:
            if "rest_err" in o:
                print(f"{o['venue']:<9} REST FAIL: {o['rest_err']}")
                continue
            wl = f"{o['ws_lag_med']:.0f}мс" if "ws_lag_med" in o else \
                 f"FAIL:{o.get('ws_err','?')}"
            wp = f"{o['ws_lag_p95']:.0f}" if "ws_lag_p95" in o else "—"
            wc = f"{o['ws_conn']:.0f}" if "ws_conn" in o else "—"
            print(f"{o['venue']:<9} {o['rest_cold']:>8.0f}м {o['rest_min']:>8.1f}м "
                  f"{o['rest_med']:>6.1f}м {o['rest_p99']:>6.1f}м {wc:>7}м "
                  f"{o['clock_off']:>+8.0f}м {wl:>11} {wp:>7}")
        print("─" * 96)
        print("REST min ≈ чистая сеть до биржи. WS-лаг ≈ доставка котировки (уже без сдвига часов).")
        print("Сохрани вывод: сравнение NL vs Tokyo по этим колонкам = измеренная цена географии.")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
