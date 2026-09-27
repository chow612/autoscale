#!/bin/sh
# wd_actuator.sh - watchdog cho master_actuator.py. Khac wd.sh (worker1): khong dua vao
# do tre log (actuator im lang luc ranh, khong lien tuc ghi log moi 3s), ma kiem tra
# TRUC TIEP process co con song khong qua pgrep.
D=/home/chau/actuator
LOG=$D/watchdog.log
if ! pgrep -f "python3 master_actuator.py" >/dev/null 2>&1; then
  echo "$(date -u +%FT%TZ) DEAD - restarting" >> "$LOG"
  cd "$D" || exit 1
  . "$D/env/bin/activate"
  set -a
  . /etc/actuator/env
  set +a
  nohup python3 "$D/master_actuator.py" >> "$D/actuator.log" 2>&1 &
  disown
  echo "$(date -u +%FT%TZ) RESTARTED pid=$!" >> "$LOG"
fi
