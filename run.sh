#!/bin/sh
# ================== THAM SO TAM - 2026-08-19 ==================
# Moi gia tri trong file nay la TAM, hieu chuan tren du lieu nhiem
# netem 300ms+-100ms (bat 2026-08-04T03:18:52Z tren ledger-db-0).
# KHONG trich bat ky so nao o day vao bao cao.
# Hieu chuan that: >= 4 ngay du lieu sach ke tu INJECT_OFF.
D=/home/chau/quant-engine
cd "$D" || exit 1
TS=$(date -u +%Y%m%dT%H%M%SZ)
export JANUS_UCL=50
export JANUS_DTHR=24.9689,6.5488,6.9570,6.8484,27.9837,20.1079
export JANUS_W=120
export JANUS_LATCH=6
export JANUS_COOL=100
export JANUS_BAND=0.2
export JANUS_FRZ=1
export JANUS_FRZ_REL=8
export JANUS_FRZ_MAX=400
export JANUS_FRZ_DEP=1
export JANUS_FRZ_ACT=0
export JANUS_UNI_K=3.0
export JANUS_UNI_K2=2.0
export JANUS_FFT_KMIN=400
export JANUS_FFT_TREND_MAX=3.0
export JANUS_CALIB_TICKS=1200
export JANUS_DCALIB_TICKS=1500
export JANUS_EVAL_FILE=$D/eval.json
export JANUS_DECIDE_CSV=$D/dec_${TS}.csv
export VIRTUAL_ENV=$D/janus-env
export PATH=$D/janus-env/bin:$PATH
M=$(md5sum "$D/ingester6.py" | cut -d' ' -f1)
nohup "$D/janus-env/bin/python3" -u "$D/ingester6.py" > "$D/ing_${TS}.log" 2>&1 &
P=$!
echo "$(date -u +%FT%TZ) START pid=$P log=ing_${TS}.log csv=dec_${TS}.csv TAM=1 md5=$M ucl=50 w=120 latch=6 frz=1/8/400" >> "$D/run.log"
echo "pid=$P log=ing_${TS}.log csv=dec_${TS}.csv md5=$M"
