"""
FastAPI Webhook Receiver & Real-time Visual Trading Desk
TradingView to Hyperliquid Perpetual Exchange Bridge

Features:
- Declarative Target-State Synchronization (Delta = target - current)
- Idempotent execution (Delta == 0 -> No-Op)
- Per-Symbol Concurrency Locks to prevent race conditions
- Embedded high-density Dark Mode Trading Dashboard at GET /
- In-browser Interactive Webhook Testing Console with 1-click presets
- Real-time Server-Sent Events (SSE) stream for instant UI updates
- Dual Engine: Seamlessly switches between live Hyperliquid API and
  interactive simulation mode (when API key is unset/placeholder)
"""

import asyncio
import hmac
import json
import logging
import os
import sys
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Dict, List, Optional

from eth_account import Account
from eth_account.signers.local import LocalAccount
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils import constants
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# ==============================================================================
# 1. Configuration & Settings
# ==============================================================================

class Settings(BaseSettings):
    """Bridge environment settings."""
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    HL_PRIVATE_KEY: str = Field(
        default="0x0000000000000000000000000000000000000000000000000000000000000000",
        description="Hyperliquid private key (agent or master key)"
    )
    HL_ACCOUNT_ADDRESS: Optional[str] = Field(
        default=None,
        description="Public 0x address of main Hyperliquid account holding collateral."
    )
    WEBHOOK_SECRET: str = Field(
        default="MY_SECURE_WEBHOOK_SECRET",
        description="Shared secret for authenticating TradingView webhooks"
    )
    IS_TESTNET: bool = Field(default=False, description="Connect to Hyperliquid testnet instead of mainnet")
    DEFAULT_SLIPPAGE: float = Field(default=0.05, description="Max slippage for aggressive IOC market orders (0.05 = 5%)")
    HOST: str = Field(default="0.0.0.0", description="Server host bind address")
    PORT: int = Field(default=8000, description="Server port")
    LOG_LEVEL: str = Field(default="INFO", description="Logging level: DEBUG, INFO, WARNING, ERROR")


try:
    settings = Settings()
except Exception as exc:
    print(f"[FATAL CONFIG ERROR] Failed to load configuration: {exc}", file=sys.stderr)
    sys.exit(1)


# ==============================================================================
# 2. Structured Logging Setup
# ==============================================================================

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("HyperliquidBridge")


# ==============================================================================
# 3. Pydantic Payload Schema
# ==============================================================================

class TradingViewWebhookPayload(BaseModel):
    secret: str = Field(..., description="Webhook shared secret")
    symbol: str = Field(..., description="Perpetual market symbol (e.g. BTC, ETH)")
    exchange: str = Field(default="HYPERLIQUID", description="Exchange identifier")
    strategyId: Optional[str] = Field(default=None, description="TradingView strategy ID")
    targetPositionSize: float = Field(..., description="Absolute desired net position size (+ long, - short, 0 flat)")
    lastFillQty: Optional[float] = Field(default=None, description="Last filled quantity from strategy")
    lastFillAction: Optional[str] = Field(default=None, description="Last filled action (buy/sell)")
    orderComment: Optional[str] = Field(default=None, description="Order comment or alert description")
    price: Optional[float] = Field(default=None, description="Reference price at alert creation")
    barTime: Optional[str] = Field(default=None, description="Timestamp of strategy bar")

    def to_safe_dict(self) -> Dict[str, Any]:
        data = self.model_dump()
        data["secret"] = "***REDACTED***"
        return data


# ==============================================================================
# 4. Real-time Event Queue & Ring Buffer for UI
# ==============================================================================

recent_events: deque = deque(maxlen=100)
sse_subscribers: List[asyncio.Queue] = []


async def broadcast_event(event: Dict[str, Any]):
    """Broadcast an execution or status event to all connected SSE clients."""
    recent_events.appendleft(event)
    disconnected = []
    for queue in sse_subscribers:
        try:
            queue.put_nowait(event)
        except Exception:
            disconnected.append(queue)
    for q in disconnected:
        if q in sse_subscribers:
            sse_subscribers.remove(q)


# ==============================================================================
# 5. Hyperliquid Connector (with Simulation Fallback)
# ==============================================================================

class HyperliquidConnector:
    """Manages L1 API connections, account state, and execution deltas."""

    def __init__(self, config: Settings):
        self.config = config
        self.base_url = constants.TESTNET_API_URL if config.IS_TESTNET else constants.MAINNET_API_URL
        self.sz_decimals_cache: Dict[str, int] = {
            "BTC": 5, "ETH": 4, "SOL": 2, "HYPE": 2, "PURR": 0, "DOGE": 0, "AVAX": 2, "ARB": 1, "SUI": 1
        }
        self.symbol_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.is_simulation: bool = False

        raw_key = config.HL_PRIVATE_KEY.strip()
        is_dummy_key = (
            not raw_key
            or set(raw_key.lower().replace("0x", "")) <= {"0"}
            or "change_me" in raw_key.lower()
            or len(raw_key.replace("0x", "")) != 64
        )

        if is_dummy_key:
            self.is_simulation = True
            self.account_address = "0x742d35Cc6634C0532925a3b844Bc454e4438f44e"
            self.wallet_address = self.account_address
            self.simulated_positions: Dict[str, Dict[str, Any]] = {}
            self.simulated_balance: float = 50000.00
            self.simulated_prices: Dict[str, float] = {
                "BTC": 65420.50, "ETH": 3480.20, "SOL": 158.40, "HYPE": 24.50, "PURR": 0.185
            }
            logger.warning(
                "====================================================================\n"
                "  NOTICE: No valid Hyperliquid private key found in .env.\n"
                "  Running in INTERACTIVE SIMULATION MODE.\n"
                "  You can test full target-state logic safely on localhost!\n"
                "  (To trade live or on testnet, add HL_PRIVATE_KEY to .env)\n"
                "===================================================================="
            )
            self.info = None
            self.exchange = None
        else:
            try:
                if not raw_key.startswith("0x"):
                    raw_key = "0x" + raw_key
                self.wallet: LocalAccount = Account.from_key(raw_key)
                self.wallet_address = self.wallet.address
                self.account_address = config.HL_ACCOUNT_ADDRESS.strip() if config.HL_ACCOUNT_ADDRESS else self.wallet.address

                logger.info(
                    f"Initialized Live Connector | Network: {'TESTNET' if config.IS_TESTNET else 'MAINNET'} | "
                    f"Wallet: {self.wallet.address} | Account: {self.account_address}"
                )
                self.info = Info(self.base_url, skip_ws=True)
                self.exchange = Exchange(
                    wallet=self.wallet,
                    base_url=self.base_url,
                    account_address=self.account_address,
                )
            except Exception as e:
                logger.error(f"Failed to initialize Hyperliquid live client: {e}. Falling back to Simulation Mode.")
                self.is_simulation = True
                self.account_address = "0x742d35Cc6634C0532925a3b844Bc454e4438f44e"
                self.wallet_address = self.account_address
                self.simulated_positions = {}
                self.simulated_balance = 50000.00
                self.simulated_prices = {"BTC": 65420.50, "ETH": 3480.20, "SOL": 158.40}
                self.info = None
                self.exchange = None

    def load_metadata(self) -> None:
        """Fetch perpetual universe metadata and cache szDecimals."""
        if self.is_simulation or self.info is None:
            return
        try:
            meta = self.info.meta()
            universe = meta.get("universe", [])
            for asset in universe:
                name = asset.get("name")
                sz_dec = asset.get("szDecimals")
                if name and sz_dec is not None:
                    self.sz_decimals_cache[name] = sz_dec
            logger.info(f"Cached precision metadata for {len(self.sz_decimals_cache)} perpetual assets.")
        except Exception as e:
            logger.error(f"Failed to load perpetual universe metadata: {e}")

    def normalize_symbol(self, raw_symbol: str) -> str:
        sym = raw_symbol.upper().strip()
        if sym in self.sz_decimals_cache:
            return sym
        for suffix in ["-PERP", "PERP", "USDT", "USD", ".P", "-USD", "/USDT", "/USD"]:
            if sym.endswith(suffix):
                candidate = sym[:-len(suffix)]
                if candidate in self.sz_decimals_cache:
                    return candidate
        return sym

    def get_sz_decimals(self, symbol: str) -> int:
        if symbol not in self.sz_decimals_cache and not self.is_simulation and self.info:
            self.load_metadata()
        return self.sz_decimals_cache.get(symbol, 4)

    def fetch_current_position(self, symbol: str) -> float:
        """Query open position size for the given symbol."""
        if self.is_simulation:
            pos = self.simulated_positions.get(symbol)
            return float(pos.get("size", 0.0)) if pos else 0.0

        user_state = self.info.user_state(self.account_address)
        for item in user_state.get("assetPositions", []):
            pos = item.get("position", {})
            if pos.get("coin") == symbol:
                return float(pos.get("szi", "0"))
        return 0.0

    def fetch_account_state(self) -> Dict[str, Any]:
        """Fetch balance, margin summary, and all open positions."""
        if self.is_simulation:
            open_pos = []
            total_margin_used = 0.0
            for sym, p in self.simulated_positions.items():
                if abs(p["size"]) > 1e-6:
                    notional = abs(p["size"]) * p["currentPrice"]
                    unrealized_pnl = (p["currentPrice"] - p["entryPrice"]) * p["size"]
                    margin = notional / p.get("leverage", 10)
                    total_margin_used += margin
                    open_pos.append({
                        "symbol": sym,
                        "size": p["size"],
                        "entryPrice": p["entryPrice"],
                        "markPrice": p["currentPrice"],
                        "positionValue": round(notional, 2),
                        "unrealizedPnl": round(unrealized_pnl, 2),
                        "liquidationPrice": round(p["entryPrice"] * 0.85 if p["size"] > 0 else p["entryPrice"] * 1.15, 2),
                        "leverage": {"type": "cross", "value": 10},
                    })
            equity = self.simulated_balance + sum(p["unrealizedPnl"] for p in open_pos)
            return {
                "isSimulation": True,
                "network": "SIMULATION (MOCK DESK)",
                "accountAddress": self.account_address,
                "marginSummary": {
                    "accountValue": f"{equity:.2f}",
                    "totalMarginUsed": f"{total_margin_used:.2f}",
                    "totalNtlPos": f"{sum(p['positionValue'] for p in open_pos):.2f}",
                    "totalRawUsd": f"{self.simulated_balance:.2f}",
                },
                "withdrawable": f"{max(0.0, self.simulated_balance - total_margin_used):.2f}",
                "openPositions": open_pos,
            }

        user_state = self.info.user_state(self.account_address)
        open_pos = []
        for item in user_state.get("assetPositions", []):
            p = item.get("position", {})
            size = float(p.get("szi", "0"))
            if size != 0:
                open_pos.append({
                    "symbol": p.get("coin"),
                    "size": size,
                    "entryPrice": float(p.get("entryPx", "0")),
                    "markPrice": float(p.get("entryPx", "0")),
                    "positionValue": float(p.get("positionValue", "0")),
                    "unrealizedPnl": float(p.get("unrealizedPnl", "0")),
                    "liquidationPrice": float(p.get("liquidationPx", "0")) if p.get("liquidationPx") else None,
                    "leverage": p.get("leverage"),
                })
        return {
            "isSimulation": False,
            "network": "TESTNET" if self.config.IS_TESTNET else "MAINNET",
            "accountAddress": self.account_address,
            "marginSummary": user_state.get("marginSummary", {}),
            "withdrawable": user_state.get("withdrawable", "0"),
            "openPositions": open_pos,
        }

    def execute_market_delta(self, symbol: str, is_buy: bool, size: float, ref_price: Optional[float] = None) -> Dict[str, Any]:
        """Execute market delta on Hyperliquid or in simulation engine."""
        if self.is_simulation:
            # Simulated matching engine
            exec_price = ref_price or self.simulated_prices.get(symbol, 65000.0)
            cur = self.simulated_positions.get(symbol, {"size": 0.0, "entryPrice": exec_price, "currentPrice": exec_price})
            new_size = round(cur["size"] + (size if is_buy else -size), self.get_sz_decimals(symbol))

            if new_size == 0.0:
                self.simulated_positions.pop(symbol, None)
            else:
                self.simulated_positions[symbol] = {
                    "size": new_size,
                    "entryPrice": exec_price,
                    "currentPrice": exec_price,
                    "leverage": 10,
                }

            oid = int(time.time() * 1000)
            return {
                "status": "ok",
                "response": {
                    "type": "order",
                    "data": {
                        "statuses": [
                            {
                                "filled": {
                                    "oid": oid,
                                    "totalSz": str(size),
                                    "avgPx": str(exec_price),
                                }
                            }
                        ]
                    },
                },
            }

        return self.exchange.market_open(
            name=symbol,
            is_buy=is_buy,
            sz=size,
            slippage=self.config.DEFAULT_SLIPPAGE,
        )


connector: Optional[HyperliquidConnector] = None


# ==============================================================================
# 6. Application Lifecycle
# ==============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global connector
    logger.info("Initializing Hyperliquid TradingView Webhook Bridge...")
    connector = HyperliquidConnector(settings)
    await asyncio.to_thread(connector.load_metadata)
    yield
    logger.info("Shutting down Hyperliquid TradingView Webhook Bridge.")


app = FastAPI(
    title="Hyperliquid TradingView Webhook Bridge",
    description="Declarative target-state execution bridge for TradingView alerts to Hyperliquid perps.",
    version="2.0.0",
    lifespan=lifespan,
)


# ==============================================================================
# 7. Webhook Receiver Endpoint
# ==============================================================================

@app.post("/webhook", status_code=status.HTTP_200_OK)
async def handle_webhook(payload: TradingViewWebhookPayload, request: Request):
    """
    Process incoming TradingView alert webhooks and perform idempotent target-state execution.
    """
    start_time = time.perf_counter()
    client_host = request.client.host if request.client else "unknown"
    timestamp_iso = datetime.now(timezone.utc).strftime("%H:%M:%S")

    # 1. Authenticate payload
    if not hmac.compare_digest(payload.secret, settings.WEBHOOK_SECRET):
        logger.warning(f"Unauthorized webhook attempt from IP {client_host}")
        event = {
            "timestamp": timestamp_iso,
            "symbol": payload.symbol,
            "strategyId": payload.strategyId or "N/A",
            "action": "AUTH_FAILED",
            "status": "REJECTED",
            "message": "Invalid webhook secret",
            "current": "N/A",
            "target": payload.targetPositionSize,
            "delta": "N/A",
            "latencyMs": round((time.perf_counter() - start_time) * 1000, 2),
        }
        await broadcast_event(event)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication failed: invalid webhook secret",
        )

    if connector is None:
        raise HTTPException(status_code=503, detail="Connector not initialized")

    symbol = connector.normalize_symbol(payload.symbol)
    sz_decimals = connector.get_sz_decimals(symbol)

    # 2. Concurrency lock per symbol
    async with connector.symbol_locks[symbol]:
        try:
            current_position = await asyncio.to_thread(connector.fetch_current_position, symbol)
            target_position = payload.targetPositionSize

            raw_delta = target_position - current_position
            rounded_delta = round(raw_delta, sz_decimals)
            exec_sz = abs(rounded_delta)
            if sz_decimals == 0:
                exec_sz = int(exec_sz)

            # Idempotency Check: No-op
            if exec_sz == 0 or abs(rounded_delta) < 1e-9:
                latency_ms = round((time.perf_counter() - start_time) * 1000, 2)
                event = {
                    "timestamp": timestamp_iso,
                    "symbol": symbol,
                    "strategyId": payload.strategyId or "N/A",
                    "action": "NO_OP",
                    "status": "IDEMPOTENT",
                    "message": "Current position matches target. No order required.",
                    "current": current_position,
                    "target": target_position,
                    "delta": 0.0,
                    "latencyMs": latency_ms,
                }
                await broadcast_event(event)
                return {
                    "status": "success",
                    "action": "no_op",
                    "symbol": symbol,
                    "currentPositionSize": current_position,
                    "targetPositionSize": target_position,
                    "delta": 0.0,
                    "message": "Current position matches target position. Idempotent no-op.",
                    "latencyMs": latency_ms,
                }

            # Order dispatch
            is_buy = rounded_delta > 0
            trade_action = "BUY" if is_buy else "SELL"

            order_response = await asyncio.to_thread(
                connector.execute_market_delta,
                symbol=symbol,
                is_buy=is_buy,
                size=exec_sz,
                ref_price=payload.price,
            )

            latency_ms = round((time.perf_counter() - start_time) * 1000, 2)
            resp_status = order_response.get("status")

            if resp_status == "ok":
                event = {
                    "timestamp": timestamp_iso,
                    "symbol": symbol,
                    "strategyId": payload.strategyId or "N/A",
                    "action": trade_action,
                    "status": "FILLED",
                    "message": f"Executed Market {trade_action} {exec_sz} {symbol}",
                    "current": current_position,
                    "target": target_position,
                    "delta": rounded_delta,
                    "latencyMs": latency_ms,
                    "response": order_response,
                }
                await broadcast_event(event)
                return {
                    "status": "success",
                    "action": trade_action,
                    "symbol": symbol,
                    "executedSize": exec_sz,
                    "previousPositionSize": current_position,
                    "targetPositionSize": target_position,
                    "latencyMs": latency_ms,
                    "exchangeResponse": order_response,
                }
            else:
                event = {
                    "timestamp": timestamp_iso,
                    "symbol": symbol,
                    "strategyId": payload.strategyId or "N/A",
                    "action": trade_action,
                    "status": "ERROR",
                    "message": "Exchange rejected market order",
                    "current": current_position,
                    "target": target_position,
                    "delta": rounded_delta,
                    "latencyMs": latency_ms,
                    "response": order_response,
                }
                await broadcast_event(event)
                return JSONResponse(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    content={
                        "status": "error",
                        "symbol": symbol,
                        "error": "Hyperliquid API error",
                        "exchangeResponse": order_response,
                    },
                )

        except Exception as exc:
            logger.error(f"Error executing webhook for {symbol}: {exc}", exc_info=True)
            latency_ms = round((time.perf_counter() - start_time) * 1000, 2)
            event = {
                "timestamp": timestamp_iso,
                "symbol": symbol,
                "strategyId": payload.strategyId or "N/A",
                "action": "ERROR",
                "status": "EXCEPTION",
                "message": str(exc),
                "current": "N/A",
                "target": payload.targetPositionSize,
                "delta": "N/A",
                "latencyMs": latency_ms,
            }
            await broadcast_event(event)
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"status": "error", "symbol": symbol, "detail": str(exc)},
            )


# ==============================================================================
# 8. Real-time SSE Stream & API Endpoints
# ==============================================================================

@app.get("/api/stream")
async def event_stream(request: Request):
    """Server-Sent Events endpoint streaming live webhook executions to the dashboard."""
    queue: asyncio.Queue = asyncio.Queue()
    sse_subscribers.append(queue)

    async def event_generator() -> AsyncGenerator[str, None]:
        try:
            # Yield initial connection heartbeat
            yield f"event: ping\ndata: {json.dumps({'time': time.time()})}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield f"event: execution\ndata: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    # Heartbeat to keep connection alive
                    yield f": heartbeat\n\n"
        finally:
            if queue in sse_subscribers:
                sse_subscribers.remove(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/state")
async def get_state():
    """Returns current account metrics, balances, and open positions."""
    if connector is None:
        raise HTTPException(status_code=503, detail="Connector not ready")
    return await asyncio.to_thread(connector.fetch_account_state)


@app.get("/api/events")
async def get_events():
    """Returns the historical ring buffer of recent webhook executions."""
    return list(recent_events)


@app.get("/health")
async def health():
    if connector is None:
        return {"status": "unhealthy"}
    return {
        "status": "healthy",
        "isSimulation": connector.is_simulation,
        "network": "SIMULATION" if connector.is_simulation else ("TESTNET" if settings.IS_TESTNET else "MAINNET"),
        "accountAddress": connector.account_address,
        "webhookSecretConfigured": bool(settings.WEBHOOK_SECRET),
    }


# ==============================================================================
# 9. Embedded Visual Trading Dashboard UI (GET /)
# ==============================================================================

@app.get("/", response_class=HTMLResponse)
async def dashboard_ui():
    """Interactive dark-mode quant trading dashboard."""
    html_content = """<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Hyperliquid ⇄ TradingView Bridge</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
  <script src="https://unpkg.com/lucide@latest"></script>
  <script>
    tailwind.config = {
      darkMode: 'class',
      theme: {
        extend: {
          fontFamily: {
            sans: ['Plus Jakarta Sans', 'sans-serif'],
            mono: ['JetBrains Mono', 'monospace'],
          },
          colors: {
            hlDark: '#080B11',
            hlCard: '#101522',
            hlCardBorder: '#1A2234',
            hlGreen: '#10B981',
            hlRed: '#F43F5E',
            hlCyan: '#06B6D4',
            hlBlue: '#3B82F6',
          }
        }
      }
    }
  </script>
  <style>
    body { background-color: #080B11; color: #E2E8F0; }
    .mono-num { font-feature-settings: 'tnum' on, 'zero' on; }
    .glow-green { box-shadow: 0 0 15px rgba(16, 185, 129, 0.25); }
    .glow-red { box-shadow: 0 0 15px rgba(244, 63, 94, 0.25); }
    .glow-cyan { box-shadow: 0 0 15px rgba(6, 182, 212, 0.25); }
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: #080B11; }
    ::-webkit-scrollbar-thumb { background: #1E293B; border-radius: 3px; }
    ::-webkit-scrollbar-thumb:hover { background: #334155; }
  </style>
</head>
<body class="min-h-screen flex flex-col font-sans selection:bg-cyan-500/30 selection:text-cyan-200">

  <!-- TOP APP BAR -->
  <header class="border-b border-hlCardBorder bg-hlCard/80 backdrop-blur-md sticky top-0 z-40 px-6 py-3.5">
    <div class="max-w-7xl mx-auto flex flex-wrap items-center justify-between gap-4">
      <div class="flex items-center space-x-3">
        <div class="w-9 h-9 rounded-lg bg-gradient-to-tr from-cyan-500 to-blue-600 flex items-center justify-center font-bold text-white shadow-lg shadow-cyan-500/20">
          <i data-lucide="zap" class="w-5 h-5"></i>
        </div>
        <div>
          <div class="flex items-center space-x-2">
            <h1 class="font-bold text-base tracking-tight text-white">HYPERLIQUID <span class="text-cyan-400">⇄</span> TRADINGVIEW</h1>
            <span id="networkBadge" class="text-[11px] font-mono px-2 py-0.5 rounded bg-blue-500/10 text-blue-400 border border-blue-500/20 font-semibold uppercase">Connecting...</span>
          </div>
          <p class="text-xs text-slate-400 flex items-center gap-1.5 font-mono mt-0.5">
            Target-State Declarative Webhook Receiver
          </p>
        </div>
      </div>

      <!-- STATUS & ACTIONS -->
      <div class="flex items-center space-x-4">
        <div class="hidden sm:flex items-center space-x-2 bg-hlDark px-3 py-1.5 rounded-lg border border-hlCardBorder text-xs font-mono">
          <span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
          <span id="sseStatus" class="text-slate-300">SSE Live Stream Active</span>
        </div>

        <div class="flex items-center space-x-2 text-xs font-mono bg-hlDark/80 px-3 py-1.5 rounded-lg border border-hlCardBorder">
          <span class="text-slate-500">Acct:</span>
          <span id="headerAccount" class="text-slate-300 truncate max-w-[140px]">0x...</span>
          <button onclick="copyAccount()" class="hover:text-cyan-400 transition" title="Copy Address">
            <i data-lucide="copy" class="w-3.5 h-3.5"></i>
          </button>
        </div>

        <button onclick="fetchState()" class="p-2 bg-hlDark hover:bg-slate-800 border border-hlCardBorder rounded-lg transition text-slate-300 hover:text-white" title="Refresh State">
          <i data-lucide="refresh-cw" class="w-4 h-4"></i>
        </button>
      </div>
    </div>
  </header>

  <!-- MAIN CONTAINER -->
  <main class="flex-1 max-w-7xl w-full mx-auto p-6 space-y-6">

    <!-- METRICS RIBBON -->
    <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
      <!-- Card 1: Account Value -->
      <div class="bg-hlCard rounded-xl p-4 border border-hlCardBorder shadow-sm relative overflow-hidden">
        <div class="flex justify-between items-center text-xs font-mono text-slate-400 mb-1">
          <span>ACCOUNT VALUE</span>
          <i data-lucide="wallet" class="w-4 h-4 text-slate-500"></i>
        </div>
        <div class="text-2xl font-bold font-mono text-white mono-num" id="metricAccountValue">$0.00</div>
        <div class="text-[11px] text-slate-500 font-mono mt-1">Cross-margin collateral equity</div>
      </div>

      <!-- Card 2: Margin Utilization -->
      <div class="bg-hlCard rounded-xl p-4 border border-hlCardBorder shadow-sm relative overflow-hidden">
        <div class="flex justify-between items-center text-xs font-mono text-slate-400 mb-1">
          <span>MARGIN UTILIZED</span>
          <i data-lucide="pie-chart" class="w-4 h-4 text-slate-500"></i>
        </div>
        <div class="text-2xl font-bold font-mono text-amber-400 mono-num" id="metricMarginUsed">$0.00</div>
        <div class="text-[11px] text-slate-500 font-mono mt-1" id="metricMarginRatio">0% utilization</div>
      </div>

      <!-- Card 3: Free Collateral -->
      <div class="bg-hlCard rounded-xl p-4 border border-hlCardBorder shadow-sm relative overflow-hidden">
        <div class="flex justify-between items-center text-xs font-mono text-slate-400 mb-1">
          <span>WITHDRAWABLE</span>
          <i data-lucide="arrow-down-to-line" class="w-4 h-4 text-slate-500"></i>
        </div>
        <div class="text-2xl font-bold font-mono text-emerald-400 mono-num" id="metricWithdrawable">$0.00</div>
        <div class="text-[11px] text-slate-500 font-mono mt-1">Free buying power</div>
      </div>

      <!-- Card 4: Open Positions -->
      <div class="bg-hlCard rounded-xl p-4 border border-hlCardBorder shadow-sm relative overflow-hidden">
        <div class="flex justify-between items-center text-xs font-mono text-slate-400 mb-1">
          <span>ACTIVE PERP POSITIONS</span>
          <i data-lucide="layers" class="w-4 h-4 text-slate-500"></i>
        </div>
        <div class="text-2xl font-bold font-mono text-cyan-400 mono-num" id="metricOpenCount">0</div>
        <div class="text-[11px] text-slate-500 font-mono mt-1" id="metricTotalNotional">$0.00 total notional</div>
      </div>
    </div>

    <!-- MIDDLE SECTION: POSITIONS & WEBHOOK SIMULATOR -->
    <div class="grid grid-cols-1 lg:grid-cols-12 gap-6">

      <!-- LEFT: OPEN POSITIONS TABLE (7 cols) -->
      <div class="lg:col-span-7 bg-hlCard rounded-xl border border-hlCardBorder overflow-hidden flex flex-col">
        <div class="px-5 py-4 border-b border-hlCardBorder flex items-center justify-between">
          <div class="flex items-center space-x-2">
            <i data-lucide="bar-chart-2" class="w-4 h-4 text-cyan-400"></i>
            <h2 class="font-bold text-sm text-white tracking-wide">HYPERLIQUID OPEN POSITIONS</h2>
          </div>
          <span class="text-xs text-slate-400 font-mono" id="posCountBadge">0 assets</span>
        </div>

        <div class="flex-1 overflow-x-auto min-h-[260px]">
          <table class="w-full text-left text-xs font-mono">
            <thead class="bg-hlDark/50 text-slate-400 border-b border-hlCardBorder text-[11px] uppercase">
              <tr>
                <th class="py-3 px-4">Market</th>
                <th class="py-3 px-4">Net Size</th>
                <th class="py-3 px-4">Entry / Value</th>
                <th class="py-3 px-4">Unrealized PnL</th>
                <th class="py-3 px-4 text-right">Action</th>
              </tr>
            </thead>
            <tbody id="positionsTableBody" class="divide-y divide-hlCardBorder/60">
              <tr>
                <td colspan="5" class="py-12 text-center text-slate-500">
                  <div class="flex flex-col items-center justify-center space-y-2">
                    <i data-lucide="inbox" class="w-8 h-8 text-slate-600"></i>
                    <span>No open positions. All assets currently flat.</span>
                  </div>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <!-- RIGHT: INTERACTIVE WEBHOOK SIMULATOR CONSOLE (5 cols) -->
      <div class="lg:col-span-5 bg-hlCard rounded-xl border border-hlCardBorder p-5 flex flex-col space-y-4">
        <div class="flex items-center justify-between border-b border-hlCardBorder pb-3">
          <div class="flex items-center space-x-2">
            <i data-lucide="radio" class="w-4 h-4 text-cyan-400"></i>
            <h2 class="font-bold text-sm text-white tracking-wide">WEBHOOK TESTING CONSOLE</h2>
          </div>
          <span class="text-[11px] bg-cyan-500/10 text-cyan-400 border border-cyan-500/20 px-2 py-0.5 rounded font-mono font-medium">
            Simulate TV Alert
          </span>
        </div>

        <!-- PRESET BUTTONS -->
        <div class="space-y-1.5">
          <label class="text-[11px] font-mono uppercase text-slate-400">1-Click Test Presets:</label>
          <div class="grid grid-cols-2 gap-2 text-xs font-mono">
            <button onclick="applyPreset('BTC', 1.5, 'Long 1.5 BTC')" class="px-2.5 py-1.5 bg-hlDark hover:bg-slate-800 border border-hlCardBorder rounded-lg text-emerald-400 hover:border-emerald-500/40 transition text-left">
              + Long 1.5 BTC
            </button>
            <button onclick="applyPreset('BTC', 2.0, 'Add 0.5 BTC')" class="px-2.5 py-1.5 bg-hlDark hover:bg-slate-800 border border-hlCardBorder rounded-lg text-cyan-400 hover:border-cyan-500/40 transition text-left">
              + Add to 2.0 BTC
            </button>
            <button onclick="applyPreset('BTC', 0.5, 'Reduce to 0.5 BTC')" class="px-2.5 py-1.5 bg-hlDark hover:bg-slate-800 border border-hlCardBorder rounded-lg text-amber-400 hover:border-amber-500/40 transition text-left">
              - Reduce to 0.5 BTC
            </button>
            <button onclick="applyPreset('BTC', 0.0, 'Flatten to 0 BTC')" class="px-2.5 py-1.5 bg-hlDark hover:bg-slate-800 border border-hlCardBorder rounded-lg text-rose-400 hover:border-rose-500/40 transition text-left">
              ✕ Flatten (0.0 BTC)
            </button>
          </div>
        </div>

        <!-- SIMULATOR INPUT FORM -->
        <div class="grid grid-cols-2 gap-3 text-xs font-mono">
          <div>
            <label class="block text-slate-400 mb-1">Symbol</label>
            <input id="inputSymbol" type="text" value="BTC" class="w-full bg-hlDark border border-hlCardBorder rounded-lg px-3 py-2 text-white focus:outline-none focus:border-cyan-500 uppercase">
          </div>
          <div>
            <label class="block text-slate-400 mb-1">Target Position Size</label>
            <input id="inputTarget" type="number" step="any" value="1.5" class="w-full bg-hlDark border border-hlCardBorder rounded-lg px-3 py-2 text-white focus:outline-none focus:border-cyan-500">
          </div>
          <div class="col-span-2">
            <label class="block text-slate-400 mb-1">Webhook Shared Secret</label>
            <input id="inputSecret" type="password" value="MY_SECURE_WEBHOOK_SECRET" class="w-full bg-hlDark border border-hlCardBorder rounded-lg px-3 py-2 text-white focus:outline-none focus:border-cyan-500">
          </div>
        </div>

        <button id="btnDispatchWebhook" onclick="dispatchWebhook()" class="w-full py-2.5 px-4 bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500 text-white font-semibold font-mono rounded-lg shadow-lg shadow-cyan-500/20 flex items-center justify-center space-x-2 transition">
          <i data-lucide="send" class="w-4 h-4"></i>
          <span>DISPATCH WEBHOOK TO /webhook</span>
        </button>

        <!-- RESPONSE INSPECTOR -->
        <div id="inspectorBox" class="hidden bg-hlDark rounded-lg p-3 border border-hlCardBorder text-xs font-mono space-y-1.5">
          <div class="flex items-center justify-between">
            <span class="text-slate-400 text-[11px] uppercase font-bold">Execution Result:</span>
            <span id="inspectLatency" class="text-cyan-400 text-[11px]">0ms</span>
          </div>
          <div class="flex items-center space-x-2 text-sm">
            <span id="inspectBadge" class="px-2 py-0.5 rounded font-bold uppercase text-xs">BUY</span>
            <span id="inspectSummary" class="text-slate-200">Executing delta...</span>
          </div>
          <pre id="inspectRaw" class="text-[10px] text-slate-400 bg-black/40 p-2 rounded max-h-24 overflow-y-auto"></pre>
        </div>
      </div>
    </div>

    <!-- BOTTOM SECTION: LIVE EXECUTION FEED & SSE AUDIT TRAIL -->
    <div class="bg-hlCard rounded-xl border border-hlCardBorder overflow-hidden">
      <div class="px-5 py-4 border-b border-hlCardBorder flex items-center justify-between">
        <div class="flex items-center space-x-2">
          <i data-lucide="activity" class="w-4 h-4 text-emerald-400"></i>
          <h2 class="font-bold text-sm text-white tracking-wide">LIVE EXECUTION AUDIT FEED</h2>
          <span class="text-[10px] bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 px-1.5 py-0.5 rounded font-mono">
            REAL-TIME STREAM
          </span>
        </div>
        <button onclick="clearAuditLog()" class="text-xs text-slate-400 hover:text-white transition font-mono">
          Clear View
        </button>
      </div>

      <div class="overflow-x-auto max-h-[380px]">
        <table class="w-full text-left text-xs font-mono">
          <thead class="bg-hlDark/50 text-slate-400 border-b border-hlCardBorder text-[11px] uppercase sticky top-0">
            <tr>
              <th class="py-3 px-4">Time (UTC)</th>
              <th class="py-3 px-4">Symbol</th>
              <th class="py-3 px-4">State Transition (Current → Target)</th>
              <th class="py-3 px-4">Execution Delta</th>
              <th class="py-3 px-4">Action</th>
              <th class="py-3 px-4">Status</th>
              <th class="py-3 px-4 text-right">Latency</th>
            </tr>
          </thead>
          <tbody id="auditTableBody" class="divide-y divide-hlCardBorder/60">
            <tr>
              <td colspan="7" class="py-8 text-center text-slate-500">
                Awaiting first webhook signal... Fire a test webhook from the console above!
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </main>

  <script>
    let accountAddress = '';

    // Initialize UI
    document.addEventListener('DOMContentLoaded', () => {
      lucide.createIcons();
      fetchState();
      initSSE();
      setInterval(fetchState, 4000);
    });

    function applyPreset(sym, target, desc) {
      document.getElementById('inputSymbol').value = sym;
      document.getElementById('inputTarget').value = target;
      dispatchWebhook();
    }

    function copyAccount() {
      if (accountAddress) {
        navigator.clipboard.writeText(accountAddress);
        alert('Copied Hyperliquid address: ' + accountAddress);
      }
    }

    function clearAuditLog() {
      document.getElementById('auditTableBody').innerHTML = `
        <tr>
          <td colspan="7" class="py-8 text-center text-slate-500">Audit view cleared. Awaiting new signals...</td>
        </tr>
      `;
    }

    // Fetch account state & open positions
    async function fetchState() {
      try {
        const res = await fetch('/api/state');
        if (!res.ok) return;
        const data = await res.json();

        accountAddress = data.accountAddress || '0x...';
        document.getElementById('headerAccount').innerText = accountAddress.slice(0, 6) + '...' + accountAddress.slice(-4);

        const badge = document.getElementById('networkBadge');
        if (data.isSimulation) {
          badge.className = 'text-[11px] font-mono px-2 py-0.5 rounded bg-amber-500/10 text-amber-400 border border-amber-500/20 font-semibold uppercase';
          badge.innerText = 'SIMULATION MODE (MOCK)';
        } else {
          badge.className = 'text-[11px] font-mono px-2 py-0.5 rounded bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 font-semibold uppercase';
          badge.innerText = data.network;
        }

        // Metrics
        const margin = data.marginSummary || {};
        const accountVal = parseFloat(margin.accountValue || '0');
        const marginUsed = parseFloat(margin.totalMarginUsed || '0');
        const withdrawable = parseFloat(data.withdrawable || '0');
        const positions = data.openPositions || [];

        document.getElementById('metricAccountValue').innerText = '$' + accountVal.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        document.getElementById('metricMarginUsed').innerText = '$' + marginUsed.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        document.getElementById('metricWithdrawable').innerText = '$' + withdrawable.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        document.getElementById('metricOpenCount').innerText = positions.length;

        const ratio = accountVal > 0 ? ((marginUsed / accountVal) * 100).toFixed(1) : '0.0';
        document.getElementById('metricMarginRatio').innerText = ratio + '% margin utilization';

        const totalNotional = positions.reduce((acc, p) => acc + (parseFloat(p.positionValue) || 0), 0);
        document.getElementById('metricTotalNotional').innerText = '$' + totalNotional.toLocaleString('en-US', { minimumFractionDigits: 2 }) + ' total notional';
        document.getElementById('posCountBadge').innerText = positions.length + ' active ' + (positions.length === 1 ? 'market' : 'markets');

        // Render Positions Table
        const tbody = document.getElementById('positionsTableBody');
        if (positions.length === 0) {
          tbody.innerHTML = `
            <tr>
              <td colspan="5" class="py-12 text-center text-slate-500">
                <div class="flex flex-col items-center justify-center space-y-2">
                  <i data-lucide="inbox" class="w-8 h-8 text-slate-600"></i>
                  <span>No open positions. All assets currently flat.</span>
                </div>
              </td>
            </tr>
          `;
        } else {
          tbody.innerHTML = positions.map(p => {
            const isLong = p.size > 0;
            const pnl = p.unrealizedPnl || 0;
            const pnlClass = pnl >= 0 ? 'text-emerald-400' : 'text-rose-400';
            const sideClass = isLong ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20' : 'bg-rose-500/10 text-rose-400 border-rose-500/20';

            return `
              <tr class="hover:bg-hlDark/40 transition">
                <td class="py-3 px-4 font-bold text-white flex items-center gap-2">
                  <span>${p.symbol}-PERP</span>
                  <span class="text-[10px] px-1.5 py-0.2 rounded border font-semibold ${sideClass}">${isLong ? 'LONG' : 'SHORT'}</span>
                </td>
                <td class="py-3 px-4 text-slate-200 font-bold ${isLong ? 'text-emerald-300' : 'text-rose-300'}">
                  ${p.size > 0 ? '+' : ''}${p.size}
                </td>
                <td class="py-3 px-4">
                  <div class="text-white">$${Number(p.entryPrice).toLocaleString('en-US', { minimumFractionDigits: 2 })}</div>
                  <div class="text-[10px] text-slate-500">Val: $${Number(p.positionValue).toLocaleString('en-US', { minimumFractionDigits: 2 })}</div>
                </td>
                <td class="py-3 px-4 font-bold ${pnlClass}">
                  ${pnl >= 0 ? '+$' : '-$'}${Math.abs(pnl).toFixed(2)}
                </td>
                <td class="py-3 px-4 text-right">
                  <button onclick="applyPreset('${p.symbol}', 0.0, 'Flatten ${p.symbol}')" class="px-2.5 py-1 text-[11px] bg-rose-500/10 hover:bg-rose-500/20 text-rose-400 border border-rose-500/20 rounded transition">
                    Flatten
                  </button>
                </td>
              </tr>
            `;
          }).join('');
        }
        lucide.createIcons();
      } catch (err) {
        console.error('Failed to fetch state:', err);
      }
    }

    // Connect to Server-Sent Events
    function initSSE() {
      const sse = new EventSource('/api/stream');
      const sseStatus = document.getElementById('sseStatus');

      sse.addEventListener('execution', (e) => {
        const event = JSON.parse(e.data);
        appendAuditEvent(event);
        fetchState();
      });

      sse.onopen = () => {
        sseStatus.innerText = 'SSE Live Stream Active';
        sseStatus.className = 'text-slate-300';
      };

      sse.onerror = () => {
        sseStatus.innerText = 'Reconnecting Stream...';
        sseStatus.className = 'text-amber-400';
      };
    }

    function appendAuditEvent(ev) {
      const tbody = document.getElementById('auditTableBody');
      const emptyRow = tbody.querySelector('td[colspan="7"]');
      if (emptyRow) tbody.innerHTML = '';

      let actionBadge = '';
      if (ev.action === 'BUY') {
        actionBadge = '<span class="px-2 py-0.5 rounded bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 font-bold">BUY</span>';
      } else if (ev.action === 'SELL') {
        actionBadge = '<span class="px-2 py-0.5 rounded bg-rose-500/10 text-rose-400 border border-rose-500/20 font-bold">SELL</span>';
      } else if (ev.action === 'NO_OP') {
        actionBadge = '<span class="px-2 py-0.5 rounded bg-cyan-500/10 text-cyan-400 border border-cyan-500/20 font-bold">NO-OP</span>';
      } else {
        actionBadge = `<span class="px-2 py-0.5 rounded bg-amber-500/10 text-amber-400 border border-amber-500/20 font-bold">${ev.action}</span>`;
      }

      let statusBadge = '';
      if (ev.status === 'FILLED') {
        statusBadge = '<span class="text-emerald-400 font-semibold">● FILLED</span>';
      } else if (ev.status === 'IDEMPOTENT') {
        statusBadge = '<span class="text-cyan-400 font-semibold">● IDEMPOTENT</span>';
      } else {
        statusBadge = `<span class="text-rose-400 font-semibold">● ${ev.status}</span>`;
      }

      const row = document.createElement('tr');
      row.className = 'hover:bg-hlDark/40 transition border-b border-hlCardBorder/40 animate-pulse';
      setTimeout(() => row.classList.remove('animate-pulse'), 1000);

      row.innerHTML = `
        <td class="py-3 px-4 text-slate-400">${ev.timestamp}</td>
        <td class="py-3 px-4 font-bold text-white">${ev.symbol}</td>
        <td class="py-3 px-4 text-slate-300">
          <span class="text-slate-500">${ev.current}</span> → <span class="font-bold text-cyan-300">${ev.target}</span>
        </td>
        <td class="py-3 px-4 font-mono font-bold ${ev.delta > 0 ? 'text-emerald-400' : (ev.delta < 0 ? 'text-rose-400' : 'text-slate-400')}">
          ${ev.delta > 0 ? '+' : ''}${ev.delta}
        </td>
        <td class="py-3 px-4">${actionBadge}</td>
        <td class="py-3 px-4">${statusBadge}</td>
        <td class="py-3 px-4 text-right text-slate-400">${ev.latencyMs}ms</td>
      `;

      tbody.insertBefore(row, tbody.firstChild);
    }

    // Webhook Dispatcher
    async function dispatchWebhook() {
      const btn = document.getElementById('btnDispatchWebhook');
      const symbol = document.getElementById('inputSymbol').value.trim().toUpperCase();
      const target = parseFloat(document.getElementById('inputTarget').value);
      const secret = document.getElementById('inputSecret').value;

      if (!symbol || isNaN(target)) {
        alert('Please provide a valid symbol and target position size.');
        return;
      }

      btn.disabled = true;
      btn.innerHTML = `<i data-lucide="loader-2" class="w-4 h-4 animate-spin"></i><span>Executing Delta...</span>`;
      lucide.createIcons();

      const payload = {
        secret: secret,
        symbol: symbol,
        exchange: "HYPERLIQUID",
        strategyId: "manual-console-v1",
        targetPositionSize: target,
        price: symbol === 'BTC' ? 65400.0 : 3450.0,
        orderComment: "ConsoleTest",
        barTime: new Date().toISOString()
      };

      try {
        const res = await fetch('/webhook', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });

        const data = await res.json();
        const inspector = document.getElementById('inspectorBox');
        inspector.classList.remove('hidden');

        document.getElementById('inspectLatency').innerText = (data.latencyMs || 0) + 'ms';
        const badge = document.getElementById('inspectBadge');
        const summary = document.getElementById('inspectSummary');

        if (data.action === 'BUY') {
          badge.className = 'px-2 py-0.5 rounded font-bold uppercase text-xs bg-emerald-500/20 text-emerald-400 border border-emerald-500/40';
          badge.innerText = 'BUY ' + data.executedSize;
          summary.innerText = `Transitioned from ${data.previousPositionSize} to ${data.targetPositionSize}`;
        } else if (data.action === 'SELL') {
          badge.className = 'px-2 py-0.5 rounded font-bold uppercase text-xs bg-rose-500/20 text-rose-400 border border-rose-500/40';
          badge.innerText = 'SELL ' + data.executedSize;
          summary.innerText = `Transitioned from ${data.previousPositionSize} to ${data.targetPositionSize}`;
        } else if (data.action === 'no_op') {
          badge.className = 'px-2 py-0.5 rounded font-bold uppercase text-xs bg-cyan-500/20 text-cyan-400 border border-cyan-500/40';
          badge.innerText = 'NO-OP (IDEMPOTENT)';
          summary.innerText = `Already at target position (${data.targetPositionSize}). No trade executed.`;
        } else {
          badge.className = 'px-2 py-0.5 rounded font-bold uppercase text-xs bg-amber-500/20 text-amber-400 border border-amber-500/40';
          badge.innerText = 'ERROR';
          summary.innerText = data.error || data.detail || 'Order execution issue';
        }

        document.getElementById('inspectRaw').innerText = JSON.stringify(data, null, 2);
        fetchState();
      } catch (err) {
        alert('Webhook failed: ' + err.message);
      } finally {
        btn.disabled = false;
        btn.innerHTML = `<i data-lucide="send" class="w-4 h-4"></i><span>DISPATCH WEBHOOK TO /webhook</span>`;
        lucide.createIcons();
      }
    }
  </script>
</body>
</html>
"""
    return HTMLResponse(content=html_content)


# ==============================================================================
# 10. Entrypoint
# ==============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        log_level=settings.LOG_LEVEL.lower(),
        reload=False,
    )
