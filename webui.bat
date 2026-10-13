@echo off
chcp 65001 >nul 2>&1
title Yandex.Disk Backup
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1

echo ============================================================
echo   Yandex.Disk Backup
echo ============================================================
echo.

REM ==== Create a project environment only when one does not exist ====
if not exist ".venv" (
    where python >nul 2>&1
    if errorlevel 1 (
        echo [!] Python 3.10+ is required. Install it from https://www.python.org/downloads/
        pause
        exit /b 1
    )
    python -c "import sys; sys.exit(0 if sys.version_info >= (3,10) else 1)" >nul 2>&1
    if errorlevel 1 (
        echo [!] Python 3.10 or newer is required.
        pause
        exit /b 1
    )
    echo [setup] Creating .venv...
    python -m venv ".venv"
    if errorlevel 1 (
        echo [!] Could not create .venv. Repair Python, then remove the incomplete .venv and retry.
        pause
        exit /b 1
    )
)

if not exist ".venv\Scripts\python.exe" goto invalid_venv
".venv\Scripts\python.exe" -c "import sys; sys.exit(0 if sys.version_info >= (3,10) and sys.prefix != sys.base_prefix else 1)" >nul 2>&1
if errorlevel 1 goto invalid_venv
".venv\Scripts\python.exe" -m pip --version >nul 2>&1
if errorlevel 1 goto invalid_venv

".venv\Scripts\python.exe" -c "import fastapi, uvicorn, yadisk, tqdm, dotenv, tenacity, requests" >nul 2>&1
if errorlevel 1 (
    echo [setup] Installing dependencies. This runs ONLY ONCE, takes 1-2 min...
    echo.
    ".venv\Scripts\python.exe" -m pip install -r requirements.txt --disable-pip-version-check
    if errorlevel 1 (
        echo.
        echo [!] Failed to install dependencies.
        echo Check your internet connection.
        echo Or run manually: ".venv\Scripts\python.exe" -m pip install -r requirements.txt
        pause
        exit /b 1
    )
    echo.
    echo [OK] All dependencies installed!
    echo.
)

".venv\Scripts\python.exe" webui.py

echo.
echo ============================================================
echo   Server stopped.
echo ============================================================
pause
exit /b 0

:invalid_venv
echo [!] .venv is invalid or missing pip. Repair it or rename/remove it, then run this script again.
pause
exit /b 1
