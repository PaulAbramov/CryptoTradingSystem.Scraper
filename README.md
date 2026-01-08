# CryptoTradingSystem.Scraper

A data acquisition service designed to fetch and stream real-time cryptocurrency market data.

### 🧩 Role in the Suite
The Scraper is the entry point of the system. It connects to exchange APIs to gather historical and real-time "candle" (OHLCV) data, which is then passed to the Calculator for analysis.

### ✨ Key Features
* **Exchange Integration:** Designed for REST and WebSocket connections to major exchanges.
* **Data Normalization:** Converts varying exchange API responses into a unified format defined in the General library.
* **Resiliency:** Implements basic rate-limiting and reconnection logic.

### 🛠 Tech Stack
* **Language:** C#
* **Libraries:** Newtonsoft.Json / System.Text.Json, Asynchronous Programming (TAP).
