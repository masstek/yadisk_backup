#!/usr/bin/env bash
# Yandex.Disk Backup — Linux/macOS launcher with auto-setup

set -e
cd "$(dirname "$0")"

export PYTHONIOENCODING=utf-8
export PYTHONUTF8=1

echo "============================================================"
echo "  Yandex.Disk Backup"
echo "============================================================"
echo

VENV_PY=".venv/bin/python"

# Reuse the project's environment without depending on the current host Python.
if [ ! -e .venv ]; then
    PY=""
    for cand in python3.13 python3.12 python3.11 python3.10 python3 python; do
        if command -v "$cand" >/dev/null 2>&1 &&
            "$cand" -c "import sys; sys.exit(0 if sys.version_info >= (3,10) else 1)" 2>/dev/null; then
            PY="$cand"
            break
        fi
    done
    if [ -z "$PY" ]; then
        echo "[!] Python 3.10+ not found. Install it first (macOS: brew install python)." >&2
        exit 1
    fi
    echo "[setup] Creating .venv with $PY..."
    if ! "$PY" -m venv .venv; then
        echo "[!] Could not create .venv. Fix the Python venv installation, then remove the incomplete .venv and retry." >&2
        exit 1
    fi
fi

if [ ! -x "$VENV_PY" ] ||
    ! "$VENV_PY" -c "import sys; sys.exit(0 if sys.version_info >= (3,10) and sys.prefix != sys.base_prefix else 1)" >/dev/null 2>&1 ||
    ! "$VENV_PY" -m pip --version >/dev/null 2>&1; then
    echo "[!] .venv is invalid or missing pip. Repair it or rename/remove it, then run this script again." >&2
    exit 1
fi

if ! "$VENV_PY" -c "import fastapi, uvicorn, yadisk, tqdm, dotenv, tenacity, requests" >/dev/null 2>&1; then
    echo "[setup] Installing dependencies. Runs ONLY ONCE, takes 1-2 min..."
    echo
    if ! "$VENV_PY" -m pip install -r requirements.txt; then
        echo "[!] Failed to install dependencies in .venv. Check your internet connection and retry." >&2
        exit 1
    fi
    echo
    echo "[OK] All dependencies installed!"
    echo
fi

"$VENV_PY" webui.py
