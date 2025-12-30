Option Explicit

' === single-instance guard (checks twice) ===
Dim svc, sh, p, cl, already
Dim fso, root
Set svc = GetObject("winmgmts:\\.\root\cimv2")
Set sh  = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
root = fso.GetParentFolderName(WScript.ScriptFullName)

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
sh.CurrentDirectory = root
sh.Run """" & root & "\.venv\Scripts\python.exe"" -u """ & root & "\study_tracker.py""", 0, True
