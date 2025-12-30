$repoRoot = Split-Path -Parent $PSScriptRoot
Get-ChildItem -Path $repoRoot -Include "runner-*.log","runner.log","*.bak","*.log.*" -File -Recurse |
  Remove-Item -Force -ErrorAction SilentlyContinue
