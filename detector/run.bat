@echo off
setlocal EnableExtensions
cd /d "%~dp0"

set "PY_CMD="
where py >nul 2>&1 && (
  py -3.11 -c "import sys" >nul 2>&1 && set "PY_CMD=py -3.11"
)
if not defined PY_CMD where python >nul 2>&1 && set "PY_CMD=python"

if not defined PY_CMD (
  echo Blad: nie znaleziono Pythona. Zainstaluj Python 3.11 i dodaj do PATH.
  echo Albo: py -3.11 -m venv .venv
  exit /b 1
)

if not exist ".venv\Scripts\python.exe" (
  echo Tworze venv ^(Python 3.11^)...
  %PY_CMD% -m venv .venv
  if errorlevel 1 exit /b 1
  call .venv\Scripts\pip install -r requirements.txt
  if errorlevel 1 exit /b 1
)

.venv\Scripts\python.exe -c "import pyflink" >nul 2>&1
if errorlevel 1 (
  echo Blad: brak pyflink. Usun folder .venv i uruchom ponownie run.bat
  exit /b 1
)

if not defined KAFKA_BOOTSTRAP_SERVERS set "KAFKA_BOOTSTRAP_SERVERS=localhost:9092"
set "PYFLINK_CLIENT_EXECUTABLE=%CD%\.venv\Scripts\python.exe"

echo Kafka: %KAFKA_BOOTSTRAP_SERVERS%
echo Python Flink: %PYFLINK_CLIENT_EXECUTABLE%
.venv\Scripts\python.exe fraud_detector.py
exit /b %ERRORLEVEL%
