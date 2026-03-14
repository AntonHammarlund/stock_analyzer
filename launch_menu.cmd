@echo off
setlocal
set "ROOT=%~dp0"
powershell -ExecutionPolicy Bypass -File "%ROOT%scripts\launcher_menu.ps1"
endlocal
