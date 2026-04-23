#!/usr/bin/env bash
# Install the clipper as a systemd service so it runs 24/7 and auto-restarts.
# Run from the project root: sudo bash install-systemd.sh
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "Run with sudo: sudo bash install-systemd.sh"
    exit 1
fi

SERVICE_SRC="$(dirname "$(readlink -f "$0")")/clipper.service"
SERVICE_DST="/etc/systemd/system/clipper.service"

echo "==> Installing ${SERVICE_DST}"
cp "${SERVICE_SRC}" "${SERVICE_DST}"
chmod 644 "${SERVICE_DST}"

echo "==> Reloading systemd"
systemctl daemon-reload

echo "==> Enabling clipper.service (start on boot)"
systemctl enable clipper.service

echo "==> Starting clipper.service now"
systemctl restart clipper.service
sleep 2

echo
echo "==> Status:"
systemctl status clipper.service --no-pager || true

echo
echo "Done. Useful commands:"
echo "  sudo systemctl status clipper       # status"
echo "  sudo systemctl restart clipper      # restart"
echo "  sudo systemctl stop clipper         # stop"
echo "  sudo systemctl logs -fu clipper     # tail logs"
echo "  tail -f /mnt/clipper-storage/clipper/logs/service.log"
