# Kasparro Backend & ETL System 🚀

A production-grade backend system built as part of Kasparro assignment.  
This system ingests cryptocurrency data, cleans and normalizes it, stores it in PostgreSQL, exposes APIs, and runs scheduled ETL — all deployed in the cloud.

---

## 🌍 Live Deployment

API Base URL:
https://YOUR-RAILWAY-URL.up.railway.app

Useful Endpoints:
- `/` → Welcome
- `/health` → System + DB health
- `/data` → Paginated normalized crypto data
- `/stats` → ETL run analytics
- `/docs` → Swagger UI

---

## 🧠 System Architecture
**Built with**
- FastAPI
- PostgreSQL
- SQLAlchemy ORM
- Docker
- Railway Cloud
- APScheduler (cloud scheduling)
- PyTest (automated tests)

**Flow**
1️⃣ Fetch from CoinPaprika  
2️⃣ Fetch from CoinGecko  
3️⃣ Load CSV  
4️⃣ Store Raw  
5️⃣ Normalize  
6️⃣ Store final structured dataset  
7️⃣ Serve via API  
8️⃣ Repeat automatically on schedule

---

## 🗄️ Database Design
Tables:
- `RawCoinPaprika`
- `RawCSV`
- `NormalizedCoin`
- `ETLRun / ETLCheckpoint`

Supports:
- Incremental ETL
- Resume safe behavior
- Monitoring

---

## 🐳 Docker Support
