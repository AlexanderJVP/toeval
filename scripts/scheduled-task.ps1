<#
.SYNOPSIS
Installs (or removes) the Windows scheduled task that runs `toeval run-due` every 5 minutes.

.DESCRIPTION
The task runs as the current user while logged on (no stored password), through the project's
.venv\Scripts\pythonw.exe so no console window flashes up every 5 minutes. Output goes to
data\logs\toeval.log; `uv run toeval status` shows the state of every stream.

- StartWhenAvailable: a start missed while the machine was off or asleep runs on wake-up.
- MultipleInstances IgnoreNew: no overlapping starts (toeval's own lock file also prevents them).
- ExecutionTimeLimit 2 h: a stuck run is stopped; the first full Elia load takes ~25 minutes.

Run `uv sync` first: the task uses the project's virtual environment.

.EXAMPLE
.\scripts\scheduled-task.ps1             # install, or update an existing task
.\scripts\scheduled-task.ps1 -Uninstall
#>
param(
    [switch]$Uninstall,
    [string]$TaskName = "toeval run-due"
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot

if ($Uninstall) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Removed scheduled task '$TaskName'."
    return
}

$pythonw = Join-Path $repo ".venv\Scripts\pythonw.exe"
if (-not (Test-Path $pythonw)) {
    throw "$pythonw not found; run 'uv sync' in $repo first."
}

$action = New-ScheduledTaskAction -Execute $pythonw -Argument "-m toeval run-due" -WorkingDirectory $repo
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 5)
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2)
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Limited

Register-ScheduledTask `
    -TaskName $TaskName `
    -Description "toeval: ingest every due data stream (https://github.com/AlexanderJVP/toeval)" `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Force | Out-Null

$repetition = (Get-ScheduledTask -TaskName $TaskName).Triggers[0].Repetition
Write-Host "Installed '$TaskName': every $($repetition.Interval), for $(if ($repetition.Duration) { $repetition.Duration } else { 'ever' })."
Write-Host "Logs: $(Join-Path $repo 'data\logs\toeval.log')"
