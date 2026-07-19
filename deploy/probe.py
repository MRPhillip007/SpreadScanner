# -*- coding: utf-8 -*-
"""Проба связности с биржами С ЭТОГО СЕРВЕРА: REST-статус + живой WS-поток.
Запуск: python deploy/probe.py — гоняется ДО деплоя, чтобы снять вопрос гео/блоков."""
import asyncio
import sys
import time

import aiohttp

REST = {
    "binance": "https://fapi.binance.com/fapi/v1/time",
    "bybit": "https://api.bybit.com/v5/market/time",
    "okx": "https://www.okx.com/api/v5/public/time",
    "gate": "https://api.gateio.ws/api/v4/futures/usdt/contracts/BTC_USDT",
    "bitget": "https://api.bitget.com/api/v2/public/time",
}
WS = {
    "binance": ("wss://fstream.binance.com/ws/btcusdt@bookTicker", None),
    "bybit": ("wss://stream.bybit.com/v5/public/linear",
              {"op": "subscribe", "args": ["tickers.BTCUSDT"]}),
    "okx": ("wss://ws.okx.com:8443/ws/v5/public",
            {"op": "subscribe", "args": [{"channel": "bbo-tbt", "instId": "BTC-USDT-SWAP"}]}),
    "gate": ("wss://fx-ws.gateio.ws/v4/ws/usdt",
             {"time": 1, "channel": "futures.book_ticker", "event": "subscribe",
              "payload": ["BTC_USDT"]}),
    "bitget": ("wss://ws.bitget.com/v2/ws/public",
               {"op": "subscribe", "args": [{"instType": "USDT-FUTURES",
                                            "channel": "books1", "instId": "BTCUSDT"}]}),
}


async def main():
    conn = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
    async with aiohttp.ClientSession(connector=conn) as s:
        print("── REST ──")
        for name, url in REST.items():
            try:
                t0 = time.perf_counter()
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    await r.read()
                    print(f"  {name:<8} HTTP {r.status}  rtt {1000*(time.perf_counter()-t0):.0f}ms")
            except Exception as e:
                print(f"  {name:<8} FAIL {type(e).__name__}: {e}")
        print("── WebSocket (5 сообщений или 10с) ──")
        for name, (url, sub) in WS.items():
            try:
                async with s.ws_connect(url, timeout=aiohttp.ClientTimeout(total=10)) as ws:
                    if sub:
                        await ws.send_json(sub)
                    n, t0 = 0, time.time()
                    while n < 5 and time.time() - t0 < 10:
                        msg = await ws.receive(timeout=9)
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            n += 1
                        else:
                            break
                    print(f"  {name:<8} {'OK' if n >= 2 else 'ТИХО'}: {n} сообщений за {time.time()-t0:.1f}с")
            except Exception as e:
                print(f"  {name:<8} FAIL {type(e).__name__}: {e}")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
