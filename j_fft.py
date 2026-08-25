import sys,os,math,array,importlib.util
import numpy as np

ING=os.environ.get('ING',os.path.expanduser('~/quant-engine/ingester6.py'))
CPU15=os.environ.get('CPU15','/tmp/cpu15.tsv')
T2F=os.environ.get('T2F','/tmp/t2.tsv')
FRZF=os.environ.get('FRZF','/tmp/frz.tsv')
OUT=os.environ.get('OUT','/tmp/res_new.tsv')
NB=int(os.environ.get('NB','192'))
KMIN=int(os.environ.get('KMIN','0'))
GAPTOL=int(os.environ.get('GAPTOL','2'))
DW=float(os.environ.get('DW','3.002'))
T0OFF=float(os.environ.get('T0OFF','9273'))
EVD=int(os.environ.get('EVD','1'))
EVH0=int(os.environ.get('EVH0','3'))
EVH1=int(os.environ.get('EVH1','5'))

spec=importlib.util.spec_from_file_location('ing6',ING)
M=importlib.util.module_from_spec(spec)
spec.loader.exec_module(M)
BPD=int(round(86400.0/M.FFT_DT))
FLOOR0=M.SIGMA_FLOOR2[0]
ES=M.FFT_EVAL_SCALE[0]

def dayhour(t):
    s=T0OFF+(t-1)*DW
    return int(s//86400),int(s//3600)%24

def binof(t):
    return int((t-1)*DW//M.FFT_DT)

def load15(p):
    b=[];v=[]
    for ln in open(p):
        f=ln.split()
        if len(f)<2:continue
        try:b.append(int(f[0]));v.append(float(f[1]))
        except ValueError:continue
    return b,v

def loadtsv(p,nc):
    d={}
    for ln in open(p):
        f=ln.rstrip('\n').split('\t')
        if len(f)<nc:continue
        try:d[int(f[0])]=[float(x) for x in f[1:nc]]
        except ValueError:continue
    return d

def fit(vals,zmad,kmax,kmin):
    z0,k0,m0=M.Z_MAD,M.K_MAX,M.FFT_KMIN
    M.Z_MAD=zmad;M.K_MAX=kmax;M.FFT_KMIN=kmin
    y=array.array('d',[x/M.FLUSH_SEC*M.FFT_SCALE[0] for x in vals])
    r=M.fft_fit(y)
    M.Z_MAD,M.K_MAX,M.FFT_KMIN=z0,k0,m0
    return r

def harm(r):
    m,A,F,PH=r[0],r[1],r[2],r[3]
    k0=M.K_MAX
    M.K_MAX=max(len(A),1)
    H=M.Harmonics()
    M.K_MAX=k0
    for i in range(len(A)):
        H.a[i]=float(A[i]);H.f[i]=float(F[i]);H.p[i]=float(PH[i])
    H.mean=float(m);H.k=len(A)
    return H

def show(tag,r,n):
    m,A,F,PH,ve,nclip,sl,pk=r[:8]
    mad=float(r[8])*ES if len(r)>8 else float('nan')
    ok='NHAN' if (ve>=M.FFT_VE_MIN and len(A)) else ('LOAI ve_thap' if len(A) else 'LOAI 0_dinh')
    print(f'{tag}: k={len(A)} ve={ve:.2f}% dinh/nen={pk:.2f} clip={nclip} trend={sl:.4g}/bin MAD_tho={mad:.4f} mean_tcpu={m*ES:.4f} -> {ok}')
    for i in range(len(A)):
        per=1.0/(F[i]*3600.0) if F[i]>0 else float('inf')
        kk=int(round(F[i]*n*M.FFT_DT))
        print(f'   k={kk:<4d} chu_ky={per:9.3f}h bien_do_tcpu={A[i]*ES:+.4f} pha={PH[i]:+.4f}')

def near24(r):
    A,F=r[1],r[2]
    best=-1;bd=1e9
    for i in range(len(A)):
        if F[i]<=0:continue
        d=abs(1.0/(F[i]*3600.0)-24.0)
        if d<bd:bd=d;best=i
    return None if best<0 else (1.0/(F[best]*3600.0),float(A[best])*ES,float(r[3][best]))

def pc(a):
    v=sorted(a);n=len(v)
    g=lambda q:v[min(n-1,max(0,int(n*q)))]
    return g(0.10),g(0.50),g(0.90),v[-1]

print('== 1. NGUON ==')
print(f'ingester6 : {ING}')
print(f'hang so   : FFT_DT={M.FFT_DT} FLUSH_SEC={M.FLUSH_SEC} K_MAX={M.K_MAX} Z_MAD={M.Z_MAD} FFT_W_MAD={M.FFT_W_MAD} FFT_VE_MIN={M.FFT_VE_MIN} FFT_KMIN_module={M.FFT_KMIN}')

b,v=load15(CPU15)
if len(b)<2:
    print('cpu15 rong');sys.exit(1)
gaps=[b[i+1]-b[i] for i in range(len(b)-1)]
cont=all(g==1 for g in gaps)
print(f'cpu15     : {len(b)} bin [{b[0]}..{b[-1]}] lien_tuc={cont} max_gap={max(gaps)}')
if len(b)<NB:
    print(f'THIEU BIN: can {NB}, co {len(b)}');sys.exit(1)
if not cont:
    print('CANH BAO: chuoi khong lien tuc; rfft gia dinh lay mau deu')

k24=NB/float(BPD)
if KMIN<=0:KMIN=max(2,int(round(k24)))
b0=b[-NB];vals=list(v[-NB:])
print(f'cua so fit: {NB} bin = {NB*M.FFT_DT/3600:.2f}h, bin {b0}..{b[-1]}')
print(f'24h roi vao k={k24:.4f}  ->  KMIN dung = {KMIN}')
if abs(k24-round(k24))>1e-9:
    print('CANH BAO: 24h KHONG roi dung mot bin -> ro pho, doi NB thanh boi so cua %d'%BPD)
if KMIN>round(k24):
    print('CANH BAO: KMIN > k cua 24h -> thanh phan ngay bi cong loai bo hoan toan')
if round(k24)<2:
    print('CANH BAO: k24 < 2 -> chu ky ngay trung voi do dai cua so, dung dieu hoa gia')

t2=loadtsv(T2F,6)
frz=loadtsv(FRZF,3)
print(f't2        : {len(t2)} tick   frz: {len(frz)} tick')

print()
print('== 2. FIT CO SO ==')
rb=fit(vals,M.Z_MAD,8,KMIN)
show('co_so',rb,NB)

print()
print('== 3. QUET Z_MAD x K_MAX ==')
for zm in (3.0,5.0):
    for km in (4,8):
        r=fit(vals,zm,km,KMIN)
        n24=near24(r)
        s=f'24h: A={n24[1]:+.4f} chu_ky={n24[0]:.3f}h' if n24 else '24h: khong co'
        print(f'Z_MAD={zm} K_MAX={km}: k={len(r[1])} ve={r[4]:.2f}% {s}')

ev=[t for t in sorted(t2) if dayhour(t)[0]==EVD and EVH0<=dayhour(t)[1]<=EVH1 and t2[t][4]>0.0]
if not ev:
    print('khong co tick nao trong cua so danh gia');sys.exit(1)

print()
print('== 4. KIEM BEN VUNG (surrogate cho bin cua so danh gia) ==')
evb=set(binof(t) for t in ev)
wb=b[-NB:]
pos={bb:i for i,bb in enumerate(wb)}
bod=lambda bb:int(((T0OFF+bb*M.FFT_DT)%86400)//M.FFT_DT)
sur=list(vals);nrep=0
for bb in sorted(evb):
    if bb not in pos:continue
    tg=bod(bb)
    dn=[v[i] for i,bx in enumerate(b) if bx not in evb and bod(bx)==tg]
    if dn:
        sur[pos[bb]]=sum(dn)/len(dn);nrep+=1
print(f'so bin thay the: {nrep}/{len(evb)}')
rs=fit(sur,M.Z_MAD,8,KMIN)
n1=near24(rb);n2=near24(rs)
if n1 and n2:
    da=abs(n2[1]-n1[1])/abs(n1[1]) if n1[1] else float('inf')
    dp=abs(((n2[2]-n1[2]+math.pi)%(2*math.pi))-math.pi)
    print(f'co_so     24h: chu_ky={n1[0]:.3f}h A={n1[1]:+.4f} pha={n1[2]:+.4f}')
    print(f'surrogate 24h: chu_ky={n2[0]:.3f}h A={n2[1]:+.4f} pha={n2[2]:+.4f}')
    print(f'lech bien do={da*100:.2f}%  lech pha={dp:.4f} rad = {dp*n1[0]/6.283185307179586:.3f}h')
else:
    print('mot trong hai fit khong co dinh -> khong so duoc')

ref={};last=None;cs=None
for t in sorted(frz):
    if int(frz[t][0]):
        if last is None or t-last>GAPTOL+1:cs=t
        last=t;ref[t]=cs

print()
print('== 5. YHAT TREN CUA SO DANH GIA ==')
H=harm(rb)
yh=lambda t:H.at((t-1)*DW-b0*M.FFT_DT)*ES
Y=np.array([t2[t][2] for t in ev])
MU=np.array([t2[t][3] for t in ev])
SD=np.array([t2[t][4] for t in ev])
YH=np.array([yh(t) for t in ev])
R=Y-YH
FR=np.array([1 if t in ref else 0 for t in ev],dtype=bool)
vy=float(Y.var());vr=float(R.var())
fvar=1.0-vr/vy if vy>0 else float('nan')
def fdrift(mask):
    if not mask.any():return float('nan'),0.0,0.0
    a=float(np.median(np.abs(Y[mask]-MU[mask])))
    c=float(np.median(np.abs(R[mask])))
    return (1.0-c/a if a>0 else float('nan')),a,c
fdr,na,nc=fdrift(FR)
fda,aa,ac=fdrift(np.ones(len(ev),dtype=bool))
h0=dayhour(ev[0]);h1=dayhour(ev[-1])
print(f'cua so : tick {ev[0]}..{ev[-1]} (ngay {h0[0]} gio {h0[1]}..{h1[1]}) n={len(ev)} trong do bi bang={int(FR.sum())}')
print(f'tcpu   : med={np.median(Y):.4f} min={Y.min():.4f} max={Y.max():.4f}')
print(f'mu_log : med={np.median(MU):.4f} min={MU.min():.4f} max={MU.max():.4f}')
print(f'sd_log : med toan cua so={np.median(SD):.5f}' + (f'  med tren tick bang={np.median(SD[FR]):.5f}' if FR.any() else ''))
print(f'yhat   : med={np.median(YH):.4f} min={YH.min():.4f} max={YH.max():.4f}')
print(f'res    : med={np.median(R):+.4f} min={R.min():+.4f} max={R.max():+.4f}')
print(f'f_drift TREN TICK BANG = 1 - {nc:.4f}/{na:.4f} = {fdr:.4f}   (H doi >= 0.80)')
print(f'f_drift toan cua so     = 1 - {ac:.4f}/{aa:.4f} = {fda:.4f}   (tham khao)')
print(f'f_var   = 1 - var(res)/var(tcpu) = {fvar:.4f}   (tham khao, cua so ngan nen thap la binh thuong)')

print()
print('== 6. z_cpu DOI CHUNG ==')
zo=[];za=[];zb=[];nfr=0
fh=open(OUT,'w')
fh.write('tick\ttcpu\tmu_log\tsd_log\tyhat\tres\tz_old\tz_res\tz_chain\tfrozen\tchain_start\n')
for t in ev:
    hh,tt,cp,mc,sdc=t2[t]
    den=math.sqrt(sdc*sdc+FLOOR0)
    a=(cp-mc)/den
    r1=(cp-yh(t))/den
    rf=ref.get(t,t)
    r2=a-(yh(t)-yh(rf))/den
    on=1 if t in ref else 0
    nfr+=on
    zo.append(abs(a));za.append(abs(r1));zb.append(abs(r2))
    fh.write(f'{t}\t{cp:.6f}\t{mc:.6f}\t{sdc:.6f}\t{yh(t):.6f}\t{cp-yh(t):+.6f}\t{a:+.4f}\t{r1:+.4f}\t{r2:+.4f}\t{on}\t{rf}\n')
fh.close()
ch=sorted(set(ref[t] for t in ev if t in ref))
print(f'tick bi bang trong cua so: {nfr}/{len(ev)}   so chuoi bang (gap<={GAPTOL}): {len(ch)}')
if ch:
    L=[sum(1 for t in ev if ref.get(t)==c) for c in ch]
    print(f'do dai chuoi (tick): min={min(L)} med={sorted(L)[len(L)//2]} max={max(L)}')
for nm,arr in (('z_old  (mu tu log)',zo),('z_res  (nen res = 0)',za),('z_chain(mu - yhat tai dau chuoi)',zb)):
    p=pc(arr)
    print(f'|{nm}|: p10={p[0]:.2f} med={p[1]:.2f} p90={p[2]:.2f} max={p[3]:.2f}  >5:{sum(1 for x in arr if x>5)}  >10:{sum(1 for x in arr if x>10)}')
print(f'ghi: {OUT}')

print()
print('== KET LUAN SO BO ==')
print(f'f_drift={fdr:.4f} -> {"DAT" if fdr>=0.8 else "KHONG DAT"} nguong 0.80')
print(f'max|z| : {pc(zo)[3]:.2f} (goc) -> {pc(za)[3]:.2f} (z_res) / {pc(zb)[3]:.2f} (z_chain)')
print('REBASELINE va scale_out phai chay qua j_replay tren cot res, khong suy tu day')
