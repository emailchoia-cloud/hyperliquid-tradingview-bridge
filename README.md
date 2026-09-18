# Hyperliquid ⇄ TradingView Declarative Webhook Bridge & Visual Trading Desk

A high-performance, idempotent execution bridge and visual trading desk connecting **TradingView strategy alerts** to the **Hyperliquid Layer 1 perpetual exchange**.

---

## ⚡ Core Architecture

Unlike primitive trading bots that execute imperative actions (`buy 1 BTC`), this bridge uses a **declarative, target-state execution model**:

$$\Delta = \text{targetPositionSize} - \text{currentPositionSize}$$

1. **Strict Idempotency**: If $\Delta == 0$ (or rounded to exchange precision is 0), the bridge issues an immediate **no-op**. Duplicate webhooks or network retries never produce accidental duplicate positions.
2. **Per-Symbol Concurrency Locks**: Webhooks for the same symbol acquire an asynchronous lock to serialize state evaluation and avoid race conditions during rapid signal bursts.
3. **Asset Precision & Lot Sizing (`szDecimals`)**: Caches Hyperliquid's universe metadata on startup and rounds $\Delta$ to the asset's allowed lot precision (e.g. 5 decimals for BTC, 0 for PURR).
4. **Non-Blocking Asynchronous Loop**: Runs Hyperliquid SDK L1 signing and network requests in an offloaded thread pool (`asyncio.to_thread`), keeping the FastAPI event loop responsive.
5. **Interactive Dark-Mode Dashboard**: Embedded real-time web UI served directly on `GET /` with live account metrics, active perpetual positions table, 1-click test presets, and Server-Sent Events (SSE) streaming.

---

## 📊 Live Web Dashboard Preview

Access the dashboard at `http://localhost:8000`:
- **Account Ribbon**: Live equity, cross-margin utilized, withdrawable buying power, active positions count.
- **Open Positions**: Real-time table of all active perpetuals with PnL and 1-click **Flatten** buttons.
- **Webhook Simulator**: Test alert payloads with 1-click presets (`+ Long 1.5 BTC`, `✕ Flatten`, etc.) and instant execution latency inspection.
- **Audit Feed**: Real-time event log powered by Server-Sent Events (`/api/stream`).

---

## 🚀 Quick Start

### 1. Clone & Setup Environment
```bash
git clone git@github.com:emailchoia-cloud/hyperliquid-tradingview-bridge.git
cd hyperliquid-tradingview-bridge

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure Environment (`.env`)
```bash
cp .env.example .env
```
Edit `.env` with your settings:
```env
# Your API Agent private key or master key
HL_PRIVATE_KEY=0xYourPrivateKeyHere

# Your main 0x Hyperliquid account address holding collateral
HL_ACCOUNT_ADDRESS=0xYourAccountAddressHere

# Secret token matching your TradingView alert message
WEBHOOK_SECRET=MY_SECURE_WEBHOOK_SECRET

# Set true for Testnet, false for Mainnet
IS_TESTNET=false

# Default slippage for IOC aggressive market orders (0.05 = 5%)
DEFAULT_SLIPPAGE=0.05
```

> **Note**: If `HL_PRIVATE_KEY` is not set or left as placeholder, the application automatically runs in **Interactive Simulation Mode**, enabling you to test the visual dashboard and delta logic safely on localhost without live capital.

### 3. Run the Server
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```
Open **[http://localhost:8000](http://localhost:8000)** in your browser.

---

## 📡 TradingView Webhook Configuration

In your Pine Script strategy or TradingView alert:

### Webhook URL:
```text
https://<your-ngrok-or-server-domain>/webhook
```

### JSON Payload:
```json
{
  "secret": "MY_SECURE_WEBHOOK_SECRET",
  "symbol": "BTC",
  "exchange": "HYPERLIQUID",
  "strategyId": "btc-4h-official-v1",
  "targetPositionSize": 1.5,
  "price": 64500.50,
  "orderComment": "LongAdd",
  "barTime": "2024-05-12T14:00:00Z"
}
```

---

## 🧪 Unit Tests

Run the built-in test suite to verify the target-state delta calculation, idempotency logic, and precision rounding:
```bash
python3 test_bridge_logic.py
```

---

## 🔒 Security Best Practices
- Use an **API Agent Key** (generated at `app.hyperliquid.xyz/API`) rather than your master private key.
- Never commit your `.env` file (protected by `.gitignore`).
- All webhook requests are authenticated using constant-time string comparison (`hmac.compare_digest`) to prevent timing attacks.
