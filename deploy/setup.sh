#!/usr/bin/env bash
# Установка SpreadScanner forward на Ubuntu 24.04 (AWS Tokyo).
# Запуск из корня репозитория:  bash deploy/setup.sh
set -e
cd "$(dirname "$0")/.."
D=$(pwd)

sudo apt-get update -y
sudo apt-get install -y python3-venv git chrony sqlite3
python3 -m venv .venv
.venv/bin/pip install --upgrade pip -q
.venv/bin/pip install -r requirements.txt uvloop -q
mkdir -p data

sudo tee /etc/systemd/system/spread-forward.service >/dev/null <<EOF
[Unit]
Description=SpreadScanner paper forward
After=network-online.target
Wants=network-online.target

[Service]
WorkingDirectory=$D
Environment=OPEN_GROSS_PCT=0.20
ExecStart=$D/.venv/bin/python $D/forward.py --exchanges binance,bybit,gate,okx,bitget
Restart=always
RestartSec=5
User=$USER

[Install]
WantedBy=multi-user.target
EOF

sudo tee /etc/systemd/system/spread-web.service >/dev/null <<EOF
[Unit]
Description=SpreadScanner dashboard
After=network-online.target

[Service]
WorkingDirectory=$D
ExecStart=$D/.venv/bin/python $D/web.py
Restart=always
RestartSec=5
User=$USER

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable spread-forward spread-web

echo ""
echo "Готово. Дальше:"
echo "  1) проба связности:   .venv/bin/python deploy/probe.py"
echo "  2) старт:             sudo systemctl start spread-forward spread-web"
echo "  3) логи:              journalctl -u spread-forward -f"
echo "  4) дашборд с твоей машины: ssh -i ключ.pem -L 8100:127.0.0.1:8100 ubuntu@IP"
