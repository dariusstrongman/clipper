#!/usr/bin/env bash
# One-shot setup for the clipper on Ubuntu. Run as ubuntu user after cloning.
set -euo pipefail

cd "$(dirname "$0")"

echo "==> Creating Python venv"
python3 -m venv venv
source venv/bin/activate

echo "==> Upgrading pip"
pip install --upgrade pip --quiet

echo "==> Installing dependencies"
pip install -r requirements.txt --quiet

echo "==> Ensuring data dirs exist on /mnt/clipper-storage/clipper"
for sub in buffers clips processed pending uploaded logs; do
    mkdir -p "/mnt/clipper-storage/clipper/$sub"
done

if [[ ! -f .env ]]; then
    echo "==> Creating .env from template"
    cp .env.example .env
    echo ""
    echo "  >>> Edit .env now:  nano .env"
    echo "  >>> Paste your credentials, then run:"
    echo "      source venv/bin/activate && python -m service.main --monitor-only"
else
    echo "==> .env already exists (not overwriting)"
fi

echo ""
echo "Setup complete."
