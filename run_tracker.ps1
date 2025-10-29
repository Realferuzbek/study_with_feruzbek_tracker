$ErrorActionPreference = 'Continue'
function Log($m){ $ts = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss'); Write-Output "$ts $m" }

# Single-instance guard (global)
$mutex = $null
try {
  Log "wrapper start as $([System.Security.Principal.WindowsIdentity]::GetCurrent().Name)"
  $python = 'F:\study_with_me\.venv\Scripts\python.exe'
  $script = 'F:\study_with_me\study_tracker.py'
  $cwd    = 'F:\study_with_me'

  if (-not (Test-Path $python)) { Log "Python missing: $python"; exit 2 }
  if (-not (Test-Path $script)) { Log "Script missing: $script"; exit 3 }
  if (-not (Test-Path $cwd))    { Log "Working dir missing: $cwd"; exit 4 }

  $mutex = New-Object System.Threading.Mutex($false, 'Global\StudyTrackerMutex')
  if (-not $mutex.WaitOne(0)) { Log "mutex already held; exiting"; exit 0 }

  try {
    Log "launching: $python -u $script"
    $p = Start-Process -FilePath $python `
                       -ArgumentList @('-u', $script) `
                       -WorkingDirectory $cwd `
                       -WindowStyle Hidden `
                       -PassThru -ErrorAction Stop
    Log "started pid $($p.Id)"
    Wait-Process -Id $p.Id
    try { Log "python exited code $($p.ExitCode)" } catch { Log "python exited" }
  } catch {
    Log "Start-Process error: $($_.Exception.Message)"
    throw
  }
} catch {
  Log "wrapper fatal: $($_.Exception.GetType().FullName): $($_.Exception.Message)"
} finally {
  try { if ($mutex) { $mutex.ReleaseMutex(); $mutex.Dispose() } } catch {}
  Log "wrapper end"
}