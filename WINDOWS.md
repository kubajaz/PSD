# PSD — uruchomienie od zera na Windows

## 0. Wymagania (jednorazowo)

Zainstaluj i uruchom:

| Program | Po co | Sprawdzenie |
|---------|--------|-------------|
| **Docker Desktop** | Kafka, Flink, MongoDB | `docker compose version` |
| **Python 3.11** | symulator, detektor, konsumenci | `py -3.11 --version` |
| **Java (JDK 11+)** | PyFlink | `java -version` |
| **Git** (opcjonalnie) | klon repozytorium | `git --version` |

Python pobierz z [python.org](https://www.python.org/downloads/) — zaznacz **„Add python.exe to PATH”**.

Java: np. [Eclipse Temurin 17](https://adoptium.net/).

Po instalacji **uruchom ponownie** terminal / komputer.

---

## 1. Pobranie projektu

```powershell
cd C:\Users\TwojUser\Documents
git clone https://github.com/kubajaz/PSD.git
cd PSD
```

(albo rozpakuj ZIP do folderu `PSD`)

---

## 2. Infrastruktura Docker

W **PowerShell** lub **CMD** w folderze projektu (`PSD`):

```bat
docker compose up -d
docker compose ps
```

Wszystkie kontenery powinny mieć status **running**, zwłaszcza `psd-kafka`.

### Przeglądarka

| URL | Login |
|-----|--------|
| http://localhost:8083 | Kafka UI — tematy `transactions`, `alerts` |
| http://localhost:8082 | Mongo Express — `admin` / `admin` |
| http://localhost:8081 | Flink UI |

Zatrzymanie środowiska:

```bat
docker compose down
```

---

## 3. JAR-y Flinka (jednorazowo)

Detektor potrzebuje connectorów w `detector\jars\`.

**PowerShell:**

```powershell
cd detector
New-Item -ItemType Directory -Force -Path jars
cd jars
curl.exe -LO https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar
curl.exe -LO https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/1.19.1/flink-json-1.19.1.jar
cd ..\..
```

W folderze `detector\jars` powinny być **2 pliki `.jar`**.

---

## 4. Uruchomienie aplikacji (4 okna terminala)

Kolejność ma znaczenie: **Docker → generator → detektor → konsumenci**.

### Okno 1 — symulator transakcji

**CMD lub PowerShell:**

```bat
cd C:\ścieżka\do\PSD\simulator
py -3.11 -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python generator.py
```

Zostaw włączone. Powinno być: `Simulator started... broker=localhost:9092`.

### Okno 2 — detektor anomalii (Flink)

**CMD:**

```bat
cd C:\ścieżka\do\PSD\detector
run.bat
```

**PowerShell** (jeśli blokada skryptów):

```powershell
cd C:\ścieżka\do\PSD\detector
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
.\run.ps1
```

Pierwsze uruchomienie tworzy `.venv` i instaluje PyFlink (kilka minut).  
Okno zostaje otwarte — **bez detektora temat `alerts` w Kafka UI będzie pusty**.

### Okno 3 — podgląd transakcji (opcjonalnie)

```bat
cd C:\ścieżka\do\PSD\consumer_test
py -3.11 -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python logger_visualizer.py
```

Czerwone wiersze = uproszczony filtr kwoty (≥ 1000 PLN), **to nie są alerty Flinka**.

### Okno 4 — monitor alertów

```bat
cd C:\ścieżka\do\PSD\consumer_test
.venv\Scripts\activate
python alert_monitor.py
```

Po chwili (gdy detektor działa) pojawiają się duże czerwone alerty w terminalu.

---

## 5. Sprawdzenie, że działa

1. **Kafka UI** (8083) → `transactions` — liczba wiadomości rośnie.  
2. **Kafka UI** → `alerts` — też rośnie (detektor musi być włączony).  
3. **Mongo Express** (8082) → baza `psd` → `fraud_alerts`.  
4. **Terminal 4** — alerty co jakiś czas (~5% transakcji to anomalie).

---

## 6. Typowe problemy na Windows

| Problem | Rozwiązanie |
|---------|-------------|
| `docker` nie działa | Uruchom **Docker Desktop**, poczekaj aż będzie „running”. |
| `Connection refused` na porcie 9092 | `docker compose up -d`, sprawdź `psd-kafka` w `docker compose ps`. |
| `No module named pyflink` | Użyj **Python 3.11**, usuń `detector\.venv`, uruchom `run.bat` ponownie. |
| Detektor milczy, `alerts` = 0 | Uruchom **`run.bat`** w `detector`, nie `alert_monitor` w złym folderze. |
| Czerwone transakcje, brak alertów | To normalne — wizualizer ≠ detektor. Muszą działać **oba** okna 1 i 2. |
| PowerShell: nie można uruchomić `run.ps1` | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` lub użyj **`run.bat`**. |
| Flink: `Cannot run program "python"` | Używaj **`run.bat` / `run.ps1`** — ustawiają ścieżkę do venv. |

---

## 7. Zatrzymanie

W każdym oknie Pythona: **Ctrl+C**.

```bat
cd C:\ścieżka\do\PSD
docker compose down
```

---

## Schemat

```
generator.py  →  Kafka: transactions  →  fraud_detector.py  →  Kafka: alerts
                                                      ↓
                                              MongoDB: fraud_alerts
                                                      ↓
                                            alert_monitor.py
```
