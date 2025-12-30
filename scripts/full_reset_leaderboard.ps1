$repoRoot = Split-Path -Parent $PSScriptRoot
& (Join-Path $PSScriptRoot "stop_tracker.ps1")
& (Join-Path $repoRoot ".venv\\Scripts\\python.exe") (Join-Path $repoRoot "reset_all.py")
& (Join-Path $PSScriptRoot "start_tracker.ps1")
