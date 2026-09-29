-- 1 PROVERA UKUPNOG BROJA UPISANIH ZAPISA
SELECT 
    'raw_ev_events' AS table_name, 
    COUNT(*) AS total_records 
FROM `ev_monitoring.raw_ev_events`
UNION ALL
SELECT 
    'agg_ev_metrics_5min' AS table_name, 
    COUNT(*) AS total_records 
FROM `ev_monitoring.agg_ev_metrics_5min`;


-- 2 PREGLED POSLEDNJIH 10 ALARMA
SELECT 
    timestamp,
    station_id,
    station_name,
    city,
    temperature_celsius,
    error_code,
    alert_reason
FROM `ev_monitoring.raw_ev_events`
WHERE is_alert = TRUE
ORDER BY timestamp DESC
LIMIT 10;


-- 3 ANALITIKA POTROSNJE PO GRADOVIMA (Ukupna energija i prosek po sesiji)
SELECT 
    city,
    COUNT(event_id) AS total_sessions,
    ROUND(SUM(energy_kwh), 2) AS total_energy_kwh,
    ROUND(AVG(energy_kwh), 2) AS avg_energy_per_session,
    ROUND(AVG(temperature_celsius), 2) AS avg_temperature,
    COUNTIF(is_alert = TRUE) AS total_alerts
FROM `ev_monitoring.raw_ev_events`
GROUP BY city
ORDER BY total_energy_kwh DESC;


-- 4 POREDJENJE PERFORMANSI PO TIPU PUNJACA
SELECT 
    charger_type,
    COUNT(event_id) AS sessions_count,
    ROUND(SUM(energy_kwh), 2) AS total_kwh,
    ROUND(AVG(charging_duration_minutes), 1) AS avg_duration_min,
    ROUND(MAX(temperature_celsius), 2) AS max_recorded_temp
FROM `ev_monitoring.raw_ev_events`
GROUP BY charger_type
ORDER BY total_kwh DESC;


-- 5 PREGLED AGREGIRANIH 5-MINUTNIH PROZORA
SELECT 
    window_start,
    window_end,
    city,
    charger_type,
    total_energy_kwh,
    total_charging_sessions,
    max_temperature_celsius,
    alert_count
FROM `ev_monitoring.agg_ev_metrics_5min`
ORDER BY window_start DESC, city ASC
LIMIT 15;