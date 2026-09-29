-- TABELA SIROVIH I OBOGACENIH DOGADJAJA (Fact events sa alert statusom)
CREATE OR REPLACE TABLE `ev_monitoring.raw_ev_events` (
    event_id STRING NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    station_id STRING NOT NULL,
    station_name STRING,
    city STRING,
    municipality STRING,
    latitude FLOAT64,
    longitude FLOAT64,
    charger_type STRING,
    energy_kwh FLOAT64,
    charging_duration_minutes INT64,
    temperature_celsius FLOAT64,
    voltage_v FLOAT64,
    is_alert BOOLEAN,
    alert_reason STRING,
    processed_at TIMESTAMP
);

--TABELA AGREGIRANIH REZULTATA (5-minutni prozori za analitiku)
CREATE OR REPLACE TABLE `ev_monitoring.agg_ev_metrics_5min` (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    city STRING NOT NULL,
    charger_type STRING NOT NULL,
    total_energy_kwh FLOAT64,
    avg_energy_kwh FLOAT64,
    total_charging_sessions INT64,
    max_temperature_celsius FLOAT64,
    avg_temperature_celsius FLOAT64,
    alert_count INT64,
    calculated_at TIMESTAMP
);