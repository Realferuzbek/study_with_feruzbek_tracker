Option Explicit

' === single-instance guard (checks twice) ===
Dim svc, sh, p, cl, already
Set svc = GetObject("winmgmts:\\.\root\cimv2")
Set sh  = CreateObject("WScript.Shell")

already = False
For Each p In svc.ExecQuery("SELECT CommandLine FROM Win32_Process WHERE Name='python.exe'")
  cl = LCase(p.CommandLine & "")
  If InStr(cl, "study_tracker.py") > 0 Then already = True : Exit For
Next
If already Then WScript.Quit 0

WScript.Sleep 1200  ' small race guard

already = False
For Each p In svc.ExecQuery("SELECT CommandLine FROM Win32_Process WHERE Name='python.exe'")
  cl = LCase(p.CommandLine & "")
  If InStr(cl, "study_tracker.py") > 0 Then already = True : Exit For
Next
If already Then WScript.Quit 0

' === start silently AND WAIT ===
sh.CurrentDirectory = "F:\study_with_me"
sh.Run """F:\study_with_me\.venv\Scripts\python.exe"" -u ""F:\study_with_me\study_tracker.py""", 0, True
