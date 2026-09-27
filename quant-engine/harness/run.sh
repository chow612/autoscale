#!/bin/sh
# ================== 2026-09-10: bo 4 dong export THAM SO TAM (19/08, nhiem netem) ==================
# JANUS_UCL / JANUS_DTHR / JANUS_W / JANUS_LATCH KHONG con export o day nua - code tu dung
# default calibrate 28/08 da hardcode (W=240, UCL=58.7343, DTHR=24.8372,..., latch=16).
# Neu can override tam thoi de test, export ngay truoc khi goi script nay, dung sua lai vao day.
D=/home/chau/quant-engine
cd "$D" || exit 1
TS=$(date -u +%Y%m%dT%H%M%SZ)
export JANUS_COOL=100
export JANUS_BAND=0.2
export JANUS_FRZ=1
export JANUS_FRZ_REL=8
export JANUS_FRZ_MAX=400
export JANUS_FRZ_DEP=1
export JANUS_FRZ_ACT=1
export JANUS_UNI_K=3.0
export JANUS_UNI_K2=2.0
export JANUS_FFT_KMIN=400
export JANUS_FFT_TREND_MAX=3.0
export JANUS_CALIB_TICKS=1200
export JANUS_DCALIB_TICKS=1500
export JANUS_EVAL_FILE=$D/eval.json
export JANUS_DECIDE_CSV=$D/dec_${TS}.csv
export JANUS_DEC_DEBUG=1
# --- Actuation (worker1 -> webhook tren master), them 2026-09-10 ---
export JANUS_ACT_URL=http://10.105.196.166:8080/scale
export JANUS_ACT_SECRET=039e67e2257922f51298b4a513248c327290f892d6f58e33345cc87881d4024a
export JANUS_ACT_WARMUP_TICKS=5   # TAM de test nhanh - PHAI xoa dong nay (tra ve default 720=3W) truoc go-live that

export VIRTUAL_ENV=$D/janus-env
export PATH=$D/janus-env/bin:$PATH
M=$(md5sum "$D/ingester6.py" | cut -d' ' -f1)
nohup "$D/janus-env/bin/python3" -u "$D/ingester6.py" > "$D/ing_${TS}.log" 2>&1 &
P=$!
echo "$(date -u +%FT%TZ) START pid=$P log=ing_${TS}.log csv=dec_${TS}.csv md5=$M w=default(240) ucl=default(138.8896) latch=default(16) act_warmup=5(TEST)" >> "$D/run.log"
echo "pid=$P log=ing_${TS}.log csv=dec_${TS}.csv md5=$M"
