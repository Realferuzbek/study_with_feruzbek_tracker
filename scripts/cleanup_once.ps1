Get-ChildItem -Path "F:\study_with_me" -Include "runner-*.log","runner.log","*.bak","*.log.*" -File -Recurse |
  Remove-Item -Force -ErrorAction SilentlyContinue
