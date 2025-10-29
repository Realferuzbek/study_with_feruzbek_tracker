New-Item -Path "F:\study_with_me\manual_stop.flag" -ItemType File -Force | Out-Null
schtasks /End /TN "\StudyTracker-Logon" 2>$null
Get-Process python -ErrorAction SilentlyContinue |
  Where-Object { $_.Path -like "F:\study_with_me\*" } |
  Stop-Process -Force
