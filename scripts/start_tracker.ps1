$repoRoot = Split-Path -Parent $PSScriptRoot
Remove-Item (Join-Path $repoRoot "manual_stop.flag") -ErrorAction SilentlyContinue
schtasks /Run /TN "\StudyTracker-Logon"
