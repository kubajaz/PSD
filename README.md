# PSD — wykrywanie anomalii w transakcjach kartowych

## 1. Infrastruktura (Docker)

```bash
cd PSD
docker compose up -d
```

```bash
docker compose ps
```

## 2. Podgląd w przeglądarce

| Usługa | URL | Opis |
|--------|-----|------|
| **Flink** | http://localhost:8081 | UI klastra Flink |
| **Mongo Express** | http://localhost:8082 | Baza MongoDB (login: `admin` / `admin`) |
| **Kafka UI** | http://localhost:8083 | Tematy `transactions` i `alerts` |

W Mongo Express: baza `psd` → kolekcja `fraud_alerts` (alerty z detektora).

Zatrzymanie:

```bash
docker compose down
```

## 3. Uruchomienie

### Symulator transakcji

```bash
cd simulator
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python generator.py
```

Wysyła JSON do Kafki, temat **`transactions`** (~5–10 tx/s).

### Detektor anomalii (Flink)

**Linux / macOS:**
```bash
cd detector
./run.sh
```

### Podgląd transakcji (opcjonalnie)

```bash
cd consumer_test
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python logger_visualizer.py
```

### Monitor alertów

```bash
cd consumer_test
source .venv/bin/activate
python alert_monitor.py
```

## 4. Przepływ danych

```
generator.py  →  Kafka: transactions  →  fraud_detector.py  →  Kafka: alerts
                                                      ↓
                                              MongoDB: fraud_alerts
                                                      ↓
                                            alert_monitor.py (terminal)
```
