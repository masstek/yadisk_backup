@echo off
chcp 65001 >nul
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
    echo [!] Run webui.bat first to create .venv.
    pause
    exit /b 1
)
".venv\Scripts\python.exe" main.py stats
echo.
pause
