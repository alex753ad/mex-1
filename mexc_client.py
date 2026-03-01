"""
Клиент MEXC API — синхронный (для Streamlit) + асинхронный (для VPS-монитора)
"""
import asyncio
import time
import requests
import aiohttp
from typing import Optional
import config


# ═══════════════════════════════════════════════════
# Синхронный клиент (для Streamlit)
# ═══════════════════════════════════════════════════

class MexcClientSync:
    """Синхронный HTTP-клиент для MEXC (Streamlit-совместимый)"""

    def __init__(self):
        self.base_url = config.MEXC_BASE_URL
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self._req_count = 0
        self._window_start = time.time()

    def _rate_limit(self):
        now = time.time()
        if now - self._window_start < 1.0:
            self._req_count += 1
            if self._req_count > 15:
                time.sleep(1.0 - (now - self._window_start))
                self._window_start = time.time()
                self._req_count = 0
        else:
            self._window_start = now
            self._req_count = 1

    def _get(self, endpoint: str, params: dict = None):
        self._rate_limit()
        try:
            r = self.session.get(
                f"{self.base_url}{endpoint}",
                params=params,
                timeout=15,
            )
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                time.sleep(5)
                return self._get(endpoint, params)
            return None
        except Exception:
            return None

    def get_exchange_info(self):
        return self._get("/api/v3/exchangeInfo")

    def get_all_tickers_24h(self):
        return self._get("/api/v3/ticker/24hr")

    def get_order_book(self, symbol: str, limit: int = 100):
        return self._get("/api/v3/depth", {"symbol": symbol, "limit": limit})

    def get_recent_trades(self, symbol: str, limit: int = 100):
        return self._get("/api/v3/trades", {"symbol": symbol, "limit": limit})

    def get_klines(self, symbol: str, interval: str = "1h", limit: int = 100):
        return self._get("/api/v3/klines", {
            "symbol": symbol, "interval": interval, "limit": limit
        })


# ═══════════════════════════════════════════════════
# Асинхронный клиент (для ws_monitor.py на VPS)
# ═══════════════════════════════════════════════════

class MexcClientAsync:
    """Асинхронный HTTP-клиент для MEXC"""

    def __init__(self):
        self.base_url = config.MEXC_BASE_URL
        self._session: Optional[aiohttp.ClientSession] = None
        self._req_count = 0
        self._window_start = time.time()

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _request(self, endpoint: str, params: dict = None):
        session = await self._get_session()
        now = time.time()
        if now - self._window_start < 1.0:
            self._req_count += 1
            if self._req_count > 15:
                await asyncio.sleep(1.0 - (now - self._window_start))
                self._window_start = time.time()
                self._req_count = 0
        else:
            self._window_start = now
            self._req_count = 1

        try:
            async with session.get(
                f"{self.base_url}{endpoint}", params=params
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                elif resp.status == 429:
                    await asyncio.sleep(5)
                    return await self._request(endpoint, params)
                return None
        except Exception:
            return None

    async def get_exchange_info(self):
        return await self._request("/api/v3/exchangeInfo")

    async def get_all_tickers_24h(self):
        return await self._request("/api/v3/ticker/24hr")

    async def get_order_book(self, symbol: str, limit: int = 100):
        return await self._request("/api/v3/depth", {
            "symbol": symbol, "limit": limit,
        })

    async def get_recent_trades(self, symbol: str, limit: int = 100):
        return await self._request("/api/v3/trades", {
            "symbol": symbol, "limit": limit,
        })
