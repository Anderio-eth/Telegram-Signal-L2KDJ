param(
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
)

$ErrorActionPreference = "Stop"

Set-Location -LiteralPath $RepoRoot

$python = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$envFile = Join-Path $RepoRoot ".env"
$logDir = Join-Path $RepoRoot "logs"
$logFile = Join-Path $logDir "bot.log"

if (-not (Test-Path -LiteralPath $python)) {
    throw "Python venv not found: $python. Run: python -m venv .venv; .\.venv\Scripts\python.exe -m pip install -e ."
}

if (-not (Test-Path -LiteralPath $envFile)) {
    throw ".env not found: $envFile. Copy .env.example to .env and set TELEGRAM_BOT_TOKEN."
}

New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$env:PYTHONUNBUFFERED = "1"
$startedAt = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
"[$startedAt] Starting Telegram Signal K2 from $RepoRoot" | Add-Content -Path $logFile -Encoding utf8

& $python -m telegram_signal_k2 *>> $logFile
$exitCode = $LASTEXITCODE

$stoppedAt = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
"[$stoppedAt] Telegram Signal K2 stopped with exit code $exitCode" | Add-Content -Path $logFile -Encoding utf8
exit $exitCode

