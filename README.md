# ⚡ Smart EV Network — Real-Time Monitoring & Telemetry Analytics

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![PySpark](https://img.shields.io/badge/Apache%20Spark-Structured%20Streaming-orange)
![SQL](https://img.shields.io/badge/Database-SQL-red)
![Power BI](https://img.shields.io/badge/Dashboard-Power%20BI-yellow)

An end-to-end data pipeline that simulates, processes, and visualizes telemetry data from a distributed network of electric vehicle (EV) charging stations in near real-time.

---

## 🔄 System Overview & Pipeline Flow

1. **Telemetry Simulation (`simulator.py`):** Generates real-time IoT events (power draw in kW, duration, battery state-of-charge, status codes) and writes them to `stream_input/`.
2. **Stream Processing (`spark_app.py`):** Ingests incoming streams via **PySpark Structured Streaming**, enriches events by joining with station metadata (`stations_lookup.csv`), and maintains fault tolerance using `checkpoints/`.
3. **Relational Storage (`create_tables.sql`):** Persists structured time-series and transaction metrics for SQL-based verification and reporting (`verify_queries.sql`).
4. **Analytics Dashboard (`Izvestaj_EV.pbix`):** Power BI report tracking network utilization, live power loads, and station operational health.

---

## 🛠️ Tech Stack

* **Language:** Python 3.8+
* **Stream Processing:** PySpark (Structured Streaming)
* **Database & Modeling:** Relational SQL
* **BI & Visualization:** Microsoft Power BI

---

## 🚀 How to Run

### 1. Setup Environment
```bash
python -m venv venv
# Windows: venv\Scripts\activate | Linux/macOS: source venv/bin/activate
pip install -r requirements.txt
```

### 2. Initialize Database
Execute create_tables.sql in your SQL environment to set up the relational tables.

### 3. Start Streaming Pipeline
Run both scripts (in separate terminals):
```bash
# Terminal 1: Start data simulation
python simulator.py

# Terminal 2: Start PySpark streaming engine
python spark_app.py
```

### 4. Inspect Results
Run verify_queries.sql to check data ingestion and aggregated metrics.
Open Izvestaj_EV.pbix in Power BI Desktop to view the live dashboard.


