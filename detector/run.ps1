# Uruchomienie detektora na Windows (PowerShell)
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

function Find-Python311 {
    if (Get-Command py -ErrorAction SilentlyContinue) {
        try {
            & py -3.11 -c "import sys" 2>$null
            if ($LASTEXITCODE -eq 0) { return @("py", "-3.11") }
        } catch { }
    }
    if (Get-Command python -ErrorAction SilentlyContinue) {
        return @("python")
    }
    throw "Nie znaleziono Pythona 3.11. Zainstaluj z python.org i użyj: py -3.11"
}

$py = Find-Python311
$venvPython = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $venvPython)) {
    Write-Host "Tworzę venv (Python 3.11)…"
    & @py -m venv .venv
    & $venvPython -m pip install -r requirements.txt
}

& $venvPython -c "import pyflink" 2>$null
if ($LASTEXITCODE -ne 0) {
    throw "Brak pyflink. Usuń folder .venv i uruchom ponownie: .\run.ps1"
}

if (-not $env:KAFKA_BOOTSTRAP_SERVERS) {
    $env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
}
$env:PYFLINK_CLIENT_EXECUTABLE = $venvPython

Write-Host "Kafka: $($env:KAFKA_BOOTSTRAP_SERVERS)"
Write-Host "Python Flink: $($env:PYFLINK_CLIENT_EXECUTABLE)"
& $venvPython fraud_detector.py
