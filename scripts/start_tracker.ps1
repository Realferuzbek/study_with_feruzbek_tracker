Remove-Item "F:\study_with_me\manual_stop.flag" -ErrorAction SilentlyContinue
schtasks /Run /TN "\StudyTracker-Logon"
