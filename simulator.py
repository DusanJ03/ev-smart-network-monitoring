import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

OUTPUT_DIR = "./stream_input"
GENERATION_INTERVAL_SECONDS = 3

STATIONS = [
    "EV-BG-01", "EV-BG-02", "EV-BG-03",
    "EV-NS-01", "EV-NS-02",
    "EV-NI-01", "EV-NI-02", "EV-NI-03",
    "EV-KG-01", "EV-KG-02"
]


def generate_single_event():
    station_id = random.choice(STATIONS)
    
    if random.random() < 0.07:
        temperature = round(random.uniform(75.5, 92.0), 2)
    else:
        temperature = round(random.uniform(32.0, 68.0), 2)

    if random.random() < 0.06:
        voltage = round(random.choice([random.uniform(320.0, 355.0), random.uniform(435.0, 455.0)]), 1)
    else:
        voltage = round(random.uniform(380.0, 415.0), 1)

    if random.random() < 0.05:
        energy = round(random.uniform(66.0, 85.0), 2)
    else:
        energy = round(random.uniform(8.5, 55.0), 2)

    event = {
        "event_id": f"EVT-{str(uuid.uuid4())[:8].upper()}",
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "station_id": station_id,
        "energy_kwh": energy,
        "charging_duration_minutes": random.randint(12, 70),
        "temperature_celsius": temperature,
        "voltage_v": voltage
    }
    return event


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print("=========================================================")
    print("  SIMULATOR PAMETNIH STANICA ZA PUNJENJE ELEKTRICNIH VOZILA")
    print("=========================================================\n")

    batch_number = 1

    try:
        while True:
            events_count = random.randint(3, 7)
            events_batch = [generate_single_event() for _ in range(events_count)]

            file_name = f"events_batch_{int(time.time())}_{batch_number}.json"
            temp_file_path = os.path.join(OUTPUT_DIR, f"{file_name}.tmp")
            final_file_path = os.path.join(OUTPUT_DIR, file_name)

            with open(temp_file_path, "w", encoding="utf-8") as f:
                for event in events_batch:
                    f.write(json.dumps(event) + "\n")

            os.rename(temp_file_path, final_file_path)

            alerts = [
                e for e in events_batch 
                if e["temperature_celsius"] > 75.0 or e["voltage_v"] < 360.0 or e["voltage_v"] > 430.0 or e["energy_kwh"] > 65.0
            ]
            alert_msg = f" Pojavljeno anomalija: {len(alerts)}]" if alerts else ""

            print(f"[{datetime.now().strftime('%H:%M:%S')} Paket #{batch_number}: Kreiran '{file_name}' sa {events_count} događaja.{alert_msg}")

            batch_number += 1
            time.sleep(GENERATION_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nSimulator je uspešno zaustavljen.")


if __name__ == "__main__":
    main()