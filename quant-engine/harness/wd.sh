#!/bin/sh
WD_RESTART=1
D=/home/chau/quant-engine
S=$D/.wd_stamp
N=$(date -u +%s)
L=$(ls -t "$D"/ing_*.log 2>/dev/null | head -1)
if [ -z "$L" ]; then
  echo "$(date -u +%FT%TZ) NO_LOG" >> "$D/watchdog.log"
  exit 0
fi
A=$(( N - $(stat -c %Y "$L") ))
[ "$A" -le 120 ] && exit 0
echo "$(date -u +%FT%TZ) STALE log=$(basename "$L") age=${A}s pids=$(pgrep -f ingester6.py | tr '\n' ' ')" >> "$D/watchdog.log"
[ "$WD_RESTART" = "1" ] || exit 0
if [ -f "$S" ] && [ $(( N - $(stat -c %Y "$S") )) -lt 600 ]; then
  echo "$(date -u +%FT%TZ) SKIP_RESTART guard" >> "$D/watchdog.log"
  exit 0
fi
touch "$S"
pkill -f ingester6.py
sleep 3
"$D/run.sh" >> "$D/watchdog.log" 2>&1
echo "$(date -u +%FT%TZ) RESTARTED" >> "$D/watchdog.log"
