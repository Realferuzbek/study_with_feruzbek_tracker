$repoRoot = Split-Path -Parent $PSScriptRoot
New-Item -Path (Join-Path $repoRoot "manual_stop.flag") -ItemType File -Force | Out-Null
schtasks /End /TN "\StudyTracker-Logon" 2>$null
Get-Process python -ErrorAction SilentlyContinue |
  Where-Object { $_.Path -like "$repoRoot\*" } |
  Stop-Process -Force
