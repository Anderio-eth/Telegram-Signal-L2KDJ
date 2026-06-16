param(
    [string]$TaskName = "Telegram Signal K2",
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [switch]$AtStartup,
    [switch]$RunNow
)

$ErrorActionPreference = "Stop"

$runScript = Join-Path $RepoRoot "scripts\run_bot.ps1"
if (-not (Test-Path -LiteralPath $runScript)) {
    throw "Run script not found: $runScript"
}

$python = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $python)) {
    throw "Python venv not found: $python. Create .venv and run pip install -e . first."
}

$argument = "-NoProfile -ExecutionPolicy Bypass -File `"$runScript`" -RepoRoot `"$RepoRoot`""
$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $argument -WorkingDirectory $RepoRoot

if ($AtStartup) {
    $trigger = New-ScheduledTaskTrigger -AtStartup
} else {
    $trigger = New-ScheduledTaskTrigger -AtLogOn
}

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -RestartCount 999 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit (New-TimeSpan -Days 0)

$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel LeastPrivilege

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Force | Out-Null

Write-Host "Installed scheduled task: $TaskName"
Write-Host "Trigger: $(if ($AtStartup) { 'At startup' } else { 'At logon' })"
Write-Host "Logs: $(Join-Path $RepoRoot 'logs\bot.log')"

if ($RunNow) {
    Start-ScheduledTask -TaskName $TaskName
    Write-Host "Started scheduled task: $TaskName"
}

