# PSD — wykrywanie anomalii w transakcjach kartowych

Krótka instrukcja uruchomienia środowiska i podglądu wyników.

**Windows od zera:** zobacz [WINDOWS.md](WINDOWS.md).

## 1. Infrastruktura (Docker)

```bash
cd PSD
docker compose up -d
```

Sprawdzenie kontenerów:

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

## 3. Uruchomienie w terminalu (4 okna)

Kolejność: najpierw Docker, potem generator, detektor, na końcu konsumenci.

### Terminal 1 — symulator transakcji

```bash
cd simulator
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python generator.py
```

Wysyła JSON do Kafki, temat **`transactions`** (~5–10 tx/s).

### Terminal 2 — detektor anomalii (Flink)

**Linux / macOS:**
```bash
cd detector
./run.sh
```

**Windows (CMD):**
```bat
cd detector
run.bat
```

**Windows (PowerShell):**
```powershell
cd detector
.\run.ps1
```

Czyta `transactions`, wysyła alerty do Kafki (**`alerts`**) i zapisuje do MongoDB (`fraud_alerts`). Wymaga **Python 3.11** i **Java** w PATH.

### Terminal 3 — podgląd transakcji (opcjonalnie)

```bash
cd consumer_test
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python logger_visualizer.py
```

Tabela transakcji; podejrzane kwoty na czerwono.

### Terminal 4 — monitor alertów

```bash
cd consumer_test
source .venv/bin/activate
python alert_monitor.py
```

Duży alert w terminalu (ASCII + czerwona ramka + dźwięk).

## 4. Przepływ danych

```
generator.py  →  Kafka: transactions  →  fraud_detector.py  →  Kafka: alerts
                                                      ↓
                                              MongoDB: fraud_alerts
                                                      ↓
                                            alert_monitor.py (terminal)
```

## 5. Typowe problemy

- **Czerwone transakcje, brak alertów w monitorze** — wizualizer (kwota ≥ 1000 PLN) to nie to samo co Flink. Musi działać **`detector/./run.sh`**.
- **Monitor milczy / Kafka UI: alerts = 0** — uruchom detektor z `detector/./run.sh` (nie z `consumer_test`). Po restarcie Kafki temat `alerts` bywa pusty — detektor musi działać na żywo.
- **Detektor nic nie wysyła** — Flink potrzebuje `python` z venv (`run.sh` to ustawia). Log: `Cannot run program "python"`.
- **`No module named 'pyflink'`** — venv detektora na Python **3.11**
