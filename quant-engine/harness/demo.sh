#!/usr/bin/env bash
# demo.sh — JANUS-SCALE, kich ban demo read-only.
# Chay:  bash demo.sh
# Tu luu output vao ~/demo_<UTC>.out, khong can tee.
#
# KHONG restart ingester, KHONG sudo, KHONG doi cau hinh cluster.
# Moi lenh chi doc. An toan chay bat cu luc nao truoc 23/08.

set -u

DIR=/home/chau/quant-engine
LOG_SACH=ing_20260819T122931Z.log
OUT=/home/chau/demo_$(date -u +%Y%m%dT%H%M%SZ).out

cd "$DIR" || { echo "khong vao duoc $DIR"; exit 1; }
exec > >(tee -a "$OUT") 2>&1

PASS=0; FAIL=0
T0=$SECONDS

hr(){ echo; echo "================================================================"; echo "$*"; echo "================================================================"; }
note(){ echo; echo ">> $*"; }
cmd(){ echo; echo "\$ $*"; eval "$@"; }
chk(){
  local ten="$1" do_="$2" ky="$3" tol="$4"
  local ok
  ok=$(awk -v a="$do_" -v b="$ky" -v t="$tol" 'BEGIN{d=a-b; if(d<0)d=-d; print (d<=t)?"OK":"LECH"}')
  printf '  %-34s do=%-12s ky vong=%-12s %s\n' "$ten" "$do_" "$ky" "$ok"
  if [ "$ok" = OK ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi
}

TMP=$(mktemp -d /tmp/janusdemo.XXXXXX)
trap 'rm -rf "$TMP"' EXIT

if [ -x ./janus-env/bin/python3 ]; then PY=./janus-env/bin/python3; else PY=python3; fi

hr "JANUS-SCALE — DEMO $(date -u +%FT%TZ)  (gio UTC)"
echo "thu muc : $DIR"
echo "python  : $PY   ($($PY -V 2>&1))"
echo "output  : $OUT"
echo
echo "Moi lenh duoi day chi DOC. Khong restart tien trinh, khong doi cau hinh."
echo "Log dung de dem la log SACH da dong ($LOG_SACH), khong phai log dang chay,"
echo "de con so khong doi giua hai lan chay."

LIVE=$(ls -1t ing_*.log 2>/dev/null | head -1)
[ -f "$LOG_SACH" ] || { echo; echo "!! khong thay $LOG_SACH — dung."; exit 1; }
[ -n "${LIVE:-}" ] || { echo; echo "!! khong thay log dang chay — dung."; exit 1; }
echo
echo "log sach: $LOG_SACH  ($(du -h "$LOG_SACH" | cut -f1))"
echo "log live: $LIVE  ($(du -h "$LIVE" | cut -f1))"

# ---------------------------------------------------------------- 1
hr "1. HE DANG SONG — va code dang chay dung la code trong bao cao"
note "Diem can chi: lstart cua tien trinh MUON HON mtime cua file code."
note "=> tien trinh da nap ban code tren dia, khong phai ban cu con sot lai."
note "run.log tu ghi md5 moi lan khoi dong => 'log nao do code nao sinh ra' khong phai doan."

cmd "md5sum ingester6.py"
cmd "git log --oneline -1"
cmd "git status --porcelain | head -5"
cmd "stat -c 'mtime=%y  size=%s' ingester6.py"
cmd "ps -o pid=,lstart=,etime= -p \$(pgrep -f 'ingester6\\.py' | head -1)"
cmd "tail -2 run.log"

NPROC=$(pgrep -cf 'ingester6\.py' || true)
echo
echo "so tien trinh ingester dang chay: $NPROC  (phai la 1)"

# ---------------------------------------------------------------- 2
hr "2. VECTOR QUAN SAT: 3 CHIEU -> 6 CHIEU"
note "Truoc: cpu, ram, rps."
note "Nay them: err, asym (log ti le byte vao/ra), slow."
note "slow = TI LE REQUEST VUOT 1 GIAY (nguong tau=1.0s, khong gian logit)."
note "Day la chieu mang SLO cua de bai. KHONG phai P99 latency — P99 chi de log."

cmd "grep -m1 -A6 '^MEWMA' $LIVE"

# ---------------------------------------------------------------- 3
hr "3. CHI PHI TINH ~1.8 ms MOI TICK"
note "proc_us = thoi gian xu ly mot tick (micro giay). dw = nhip tick thuc do."
note "CANH BAO CACH DOC: day la CHI PHI TINH, khong phai do tre phat hien."
note "Do tre phat hien co san cung FLUSH_SEC=3s cong tre duong ong 3s => >= 6s."
note "Khong claim subsecond."

cmd "grep -o 'proc_us[^ ]*' $LIVE | tail -3"
cmd "grep -o 'dw=[0-9.]*'   $LIVE | tail -3"
cmd "grep -o 'wait_ms([^)]*)' $LIVE | tail -2"

PROC=$(grep -o 'proc_us[^ ]*' "$LIVE" | tail -1 | grep -o '[0-9.]\+' | head -1 || echo 0)
echo
echo "=> proc_us $PROC us tren nhip 3.002 s  =  duty $(awk -v p="$PROC" 'BEGIN{printf "%.3f", 100*p/3002000}') % CPU"

# ---------------------------------------------------------------- 4
hr "4. NGUONG F SAI — NGUONG HIEU CHUAN DUNG   (ket qua chinh)"
note "ucl_for(120,6) = (6*119/114) * F_0.99(6,114) = 18.565  <- nguong ly thuyet"
note "Muc tieu thiet ke: 1 % bao dong gia."
note "Bang duoi doc THANG TU LOG, khong qua replay => khong dinh gia dinh nao cua replay."
note "Chay j_replay.py ucl co the mat 30-60 giay (parse 40 MB bang Python thuan)."

$PY j_replay.py ucl "$LOG_SACH" 500 120 > "$TMP/ucl.out" 2>&1
echo; echo "\$ $PY j_replay.py ucl $LOG_SACH 500 120"; cat "$TMP/ucl.out"

note "DUONG DOC LAP — khong Python, khong thu vien, chi grep|sed|awk:"
cmd "grep '^MEWMA T2=' $LOG_SACH | sed 's/.*T2=\\([0-9.]*\\).*/\\1/' | tail -n +501 | awk '{n++; if(\$1>18.565) f++; if(\$1>50) c++} END{printf \"n=%d   nguong F 18.565 -> %.3f%%   hieu chuan 50 -> %.3f%%\\n\", n, 100*f/n, 100*c/n}'" | tee "$TMP/awk.out"

note "PHAI NOI KEM, dung doi bi hoi:"
echo "  - UCL=50 la PROVISIONAL: 14 gio du lieu, chot lai sau 23/08 khi du 4 ngay."
echo "  - CHUA TACH DUOC NGUYEN NHAN vi sao nguong F lech ~26 lan ve tan suat:"
echo "    mot phan do MEWMA khong phai Hotelling (Lowry 1992 phai xac dinh h4 bang mo phong"
echo "    vi khong co dang dong), mot phan do tai cluster tu tuong quan."
echo "    Doi chung am tren 6-vector iid Gaussian la phep do tach hai cai do — da len lich."

# ---------------------------------------------------------------- 5
hr "5. XEP HANG CHIEU  ->  TRUY VE DEPLOYMENT"
note "d_i = T2(du 6 chieu) - T2(bo chieu i)   [leave-one-out]"
note "Dung tu 'XEP HANG', KHONG dung 'dong gop':"
note "  day la dang RUT GON cua Mason-Young-Tracy, da do masking:"
note "  T2 = 40.10 nhung tong d_i = 89.9  =>  KHONG cong lai thanh T2."
note "Luat chon chieu la argmax(d_i / DTHR_i), khong phai argmax(d_i) tho."
note "  Luat tho da do duoc: 2.92 % am tinh gia, 2.49 % quy ket lech."
note "DTHR hien tai la [VOID] (hieu chuan tren du lieu nhiem netem), chot lai sau 23/08."

$PY j_replay.py d "$LOG_SACH" 142 120 > "$TMP/d.out" 2>&1
echo; echo "\$ $PY j_replay.py d $LOG_SACH 142 120"; cat "$TMP/d.out"

NATTR=$(grep -c '^ATTR' "$LOG_SACH" || true)
NMEW=$(grep -c '^MEWMA' "$LOG_SACH" || true)
echo
echo "so dong ATTR = $NATTR   so dong MEWMA = $NMEW"
if [ "$NATTR" -gt 0 ]; then
  cmd "grep '^ATTR' $LOG_SACH | grep 'sig=1' | head -5"
  cmd "grep '^ATTR' $LOG_SACH | grep -o 'dim=[a-z]*'  | sort | uniq -c | sort -rn"
  cmd "grep '^ATTR' $LOG_SACH | grep -o 'dep=[a-z-]*' | sort | uniq -c | sort -rn"
else
  echo "(khong co dong ATTR dau dong — thu lai voi dau cach dan dau)"
  cmd "grep -m5 'ATTR' $LOG_SACH"
fi

note "Vi du da bat duoc tren cluster that (phien 12/08):"
echo "  ATTR dim=slow d=16.459 thr=10.628 sig=1 dep=frontend z=+1.293   (x7 tick lien tiep)"
echo "  cung luc do cpu d=7.71 < thr=12.44  => DUOI NGUONG  => HPA se khong hanh dong."

# ---------------------------------------------------------------- 6
hr "6. BO PHAN TICH OFFLINE — hieu chuan khong can restart"
note "Harness nap chinh ingester6.py bang importlib va chay StateMEWMA that."
note "=> tang MEWMA khong the troi khoi ma production."
note "Quet W / UCL / DTHR / FRZ_REL / LATCH gio la bai toan OFFLINE."
note "Buoc nay nang nhat: parse + 3 lan replay, co the mat 2-4 phut."

$PY j_replay.py check "$LOG_SACH" 50 120 142 > "$TMP/check.out" 2>&1
echo; echo "\$ $PY j_replay.py check $LOG_SACH 50 120 142"; cat "$TMP/check.out"

note "PHAM VI CUA CON SO 0/16745 — noi dung neu bi hoi ky:"
echo "  Phu: T2, trig, va may trang thai bang."
echo "  Chua phu: luat quy ket (wi/wdep/dec) — trong harness la ban chep tay,"
echo "  chua doi chieu voi dong ATTR. Dang va."
echo "  Gia dinh replay: res lam tron 4 chu so; con so chinh doc o dong 'valid=1'."

# ---------------------------------------------------------------- 7
hr "7. KIEM THEM — ba cau hoi dang treo"
note "(a) DCALIB/UCALIB da no chua? Moc du kien ~21/08 02:30-02:40Z."
note "    Dong 'DCALIB delta' do truc tiep muc nhiem netem da day DTHR di bao nhieu %."
cmd "grep -n 'DCALIB\\|UCALIB\\|REBASELINE' $LIVE | tail -10"

note "(b) _stats() dung MAD tho hay MAD*1.4826?"
note "    Quyet dinh delta_slow la -2.53 hay -1.71 => anh huong luoi Stage C."
cmd "grep -n '1.4826' ingester6.py | head -5"
cmd "grep -n 'def _stats' ingester6.py"

note "(c) netem da tat va chua bat lai?"
cmd "tail -3 /home/chau/sdf_marks.tsv"

# ---------------------------------------------------------------- 8
hr "8. DOI CHIEU VOI GIA TRI KY VONG"
echo "Lech bat ky dong nao => DUNG, dieu tra truoc khi demo."
echo

N_UCL=$(grep -m1 'n=[0-9]*' "$TMP/ucl.out" | grep -o 'n=[0-9]*' | head -1 | cut -d= -f2 || echo 0)
T50=$(grep -m1 'UCL=50 ' "$TMP/ucl.out" | grep -o 'trig=[0-9.]*' | cut -d= -f2 || echo 0)
MAXT2=$(grep -m1 'max=' "$TMP/ucl.out" | grep -o 'max=[0-9.]*' | cut -d= -f2 || echo 0)
FPCT=$(grep -o 'nguong F 18.565 -> [0-9.]*' "$TMP/awk.out" | grep -o '[0-9.]*$' || echo 0)
CPCT=$(grep -o 'hieu chuan 50 -> [0-9.]*' "$TMP/awk.out" | grep -o '[0-9.]*$' || echo 0)
TRIGL=$(grep 'valid=1' "$TMP/check.out" | grep -o 'trig_lech=[0-9]*' | cut -d= -f2 || echo -1)
FRZL=$(grep 'FRZ may trang thai' "$TMP/check.out" | grep -o 'lech=[0-9]*' | cut -d= -f2 || echo -1)

chk "so tick (bang UCL)"        "${N_UCL:-0}"  16387    5
chk "max(T2)"                   "${MAXT2:-0}"  131.2481 0.01
chk "trig tai UCL=50 (%)"       "${T50:-0}"    1.086    0.05
chk "nguong F 18.565 (%)"       "${FPCT:-0}"   26.198   1.0
chk "hieu chuan 50, duong awk"  "${CPCT:-0}"   1.086    0.05
chk "trig lech (tick)"          "${TRIGL:--1}" 0        0
chk "FRZ lech (tick)"           "${FRZL:--1}"  0        0

hr "TONG KET"
echo "OK: $PASS      LECH: $FAIL"
echo "thoi gian chay: $((SECONDS-T0)) giay"
echo
if [ "$FAIL" -eq 0 ]; then
  echo "Tat ca khop gia tri da bao cao. San sang demo."
else
  echo "CO DONG LECH — dieu tra truoc khi demo. Dung trinh bay so chua giai thich duoc."
fi
echo
echo "TUYET DOI KHONG LAM TRONG BUOI DEMO:"
echo "  - pkill / restart ingester  => mat dong ho 4 ngay, moc 23/08 12:40Z tinh lai"
echo "  - sdf_inject.sh bat ky dang nao => tai nhiem du lieu"
echo "  - sua ingester6.py => md5 lech, moi bang mat xuat xu"
echo "  - doi replica / tham so k6 / cilium-config => dich muc trong chuoi"
echo "  - dan khoi nhieu dong co sudo o giua"
echo "Neu bat buoc restart: touch .wd_stamp TRUOC khi pkill (mua 600s doc quyen)."
echo
echo "output da luu: $OUT"
sleep 1
