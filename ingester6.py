import struct,time,array,asyncio,math,json,os,urllib.request,urllib.parse,urllib.error
from contextlib import asynccontextmanager
from collections import deque
from fastapi import FastAPI,Request
from starlette.responses import Response
import cramjam,uvicorn,numpy as np
from scipy.stats import f as f_dist

LISTEN_HOST='0.0.0.0'
LISTEN_PORT=5001
SHARDS=16
RING_CAP=8192
FLUSH_SEC=3.0
DT_MAX=float(os.environ.get('JANUS_DT_MAX','0') or 0) or FLUSH_SEC*4.0
DT_MAX_POD=float(os.environ.get('JANUS_DT_MAX_POD','0') or 0) or 45.0
NS_FILTER='bank-of-anthos'
STALE_CYCLES=5
EPS=1e-9
EPS_L=1e-4
MEWMA_DIM=6
MEWMA_LAMBDA=0.2
MEWMA_WINDOW=int(os.environ.get('JANUS_W','0') or 0) or 120
MEWMA_ALPHA=0.01
MEWMA_MIN_SAMPLES=max(MEWMA_DIM+1,30)
MEWMA_RIDGE=1e-6
RECOMP_EVERY=1
MIN_REQ=5
MIN_BYTES=1024
SIGMA_FLOOR2=array.array('d',[v*v for v in (0.01,1.0e5,0.5,0.05,0.05,0.05)])
SD_LIVE_FRAC=0.05
SD_LIVE=array.array('d',[SD_LIVE_FRAC*math.sqrt(v) for v in SIGMA_FLOOR2])
DEP_LIST=('balancereader','contacts','frontend','ledgerwriter','transactionhistory','userservice')
NDEP=len(DEP_LIST)
DEP_IDX={n:i for i,n in enumerate(DEP_LIST)}
DEP_MIN=30
DEP_NCAP=1200.0
DCALIB_TICKS=int(os.environ.get('JANUS_DCALIB_TICKS','0') or 0)
TR_MAX=float(os.environ.get('JANUS_TR_MAX','0') or 0) or 2.0
FRZ_ON=int(os.environ.get('JANUS_FRZ','1') or '1')
FRZ_REL=int(os.environ.get('JANUS_FRZ_REL','0') or 0) or 8
FRZ_MAX=int(os.environ.get('JANUS_FRZ_MAX','0') or 0) or 400
FRZ_DEP=int(os.environ.get('JANUS_FRZ_DEP','1') or '1')
FRZ_ACT=int(os.environ.get('JANUS_FRZ_ACT','0') or 0)
DEC_LATCH=int(os.environ.get('JANUS_LATCH','16') or 16)
DEC_COOL=int(os.environ.get('JANUS_COOL','100') or 100)
DEC_BAND=float(os.environ.get('JANUS_BAND','0.2') or 0.2)
DEC_CSV=os.environ.get('JANUS_DECIDE_CSV','')
SCALE_DIM=(0,1,2,5)
MAX_REP={'frontend':20,'contacts':15,'userservice':15,'ledgerwriter':10,'balancereader':10,'transactionhistory':10}
PG_POOL={'ledgerwriter':6,'balancereader':3,'transactionhistory':3}
PG_BUDGET=91
UNI_K=float(os.environ.get('JANUS_UNI_K','3.0') or 3.0)
UNI_K2=float(os.environ.get('JANUS_UNI_K2','2.0') or 2.0)
DTHR=array.array('d',[0.0]*MEWMA_DIM)
_dts=os.environ.get('JANUS_DTHR','')
if _dts:
    _dtp=_dts.split(',')
    for _i in range(min(MEWMA_DIM,len(_dtp))):DTHR[_i]=float(_dtp[_i])
DTHR_ENV=1 if _dts else 0
FFT_N=672
FFT_DT=900.0
FFT_INTERVAL_SEC=900.0
K_MAX=8
Z_MAD=5.0
FFT_COV_MIN=FFT_N*0.85
FFT_MAX_GAP=24
FFT_VE_MIN=10.0
FFT_W_MAD=8.0
FFT_KMIN=int(os.environ.get('JANUS_FFT_KMIN','5') or 5)
FFT_TREND_MAX=float(os.environ.get('JANUS_FFT_TREND_MAX','0') or 0) or 3.0
FFT_GRP=3
GARCH_ALPHA=0.08
GARCH_BETA=0.90
GARCH_OMEGA=1.0-GARCH_ALPHA-GARCH_BETA
H_FLOOR=0.25
DCC_A=0.02
DCC_B=0.96
DCC_TCAP=300
DCC_CONST=1.0-DCC_A-DCC_B
DCC_SHRINK=0.05
SHRINK_LADDER=(0.05,0.15,0.35,0.60,0.85,0.97)
DET_FLOOR=0.02
R_CLAMP=0.99
Q_FLOOR=1e-3
X_CLIP=8.0
GARCH_MIN_TICKS=60
LATCH_TICKS=5
HIT_MASK=3

N_REL=8
POPCNT=bytes(bin(i).count('1') for i in range(256))
CHI2_99=(0.0,6.6349,9.2103,11.3449)
THR_A=float(os.environ.get('JANUS_THR_A','0') or 0)
THR_B=float(os.environ.get('JANUS_THR_B','0') or 0)
THR_ENV=1 if (THR_A>0 or THR_B>0) else 0
CALIB_TICKS=int(os.environ.get('JANUS_CALIB_TICKS','0') or 0)
CALIB_CAP=20000
if CALIB_TICKS>CALIB_CAP:CALIB_TICKS=CALIB_CAP
if DCALIB_TICKS>CALIB_CAP:DCALIB_TICKS=CALIB_CAP
CALIB_SPAN=int(os.environ.get('JANUS_CALIB_SPAN','0') or 0) or 28800
DCALIB_SPAN=int(os.environ.get('JANUS_DCALIB_SPAN','0') or 0) or 28800
CALIB_STRIDE=max(1,CALIB_SPAN//CALIB_TICKS) if CALIB_TICKS>0 else 1
DCALIB_STRIDE=max(1,DCALIB_SPAN//DCALIB_TICKS) if DCALIB_TICKS>0 else 1
EVAL_FILE=os.environ.get('JANUS_EVAL_FILE','')
EVAL_SUM=int(os.environ.get('JANUS_EVAL_SUM','0') or 0) or 1200
STATES=('Healthy','Flash Crowd','Silent Dependency Failure','L7 DDoS')
MIMIR_QUERY_URL=os.environ.get('MIMIR_QUERY_URL','http://mimir-nginx.monitoring.svc.cluster.local:80/prometheus/api/v1/query_range')
MIMIR_TENANT=os.environ.get('MIMIR_TENANT','')
CPU,RAM,NIN,NOUT=0,1,2,3
DIMS=('cpu','ram','rps','err','asym','slow')
FFT_DIM_IDX=(0,2,4,5)
FFT_DIM_NAMES=('cpu','rps','asym','slow')
FFT_TF=(0,0,1,2)
SLOW_LE=os.environ.get('SLOW_LE','1.0')
SLOW_THR=float(SLOW_LE)
FFT_SCALE=(FFT_DT,FFT_DT,1.0,1.0)
FFT_EVAL_SCALE=(FLUSH_SEC/FFT_DT,FLUSH_SEC/FFT_DT,1.0,1.0)
CADVISOR={'container_cpu_usage_seconds_total':CPU,
          'container_memory_working_set_bytes':RAM,
          'container_network_receive_bytes_total':NIN,
          'container_network_transmit_bytes_total':NOUT}
RPS_METRIC='hubble_http_requests_total'
LAT_METRIC='hubble_http_request_duration_seconds_bucket'
ENVOY_METRIC='envoy_cluster_external_upstream_rq'
LABELS=('__name__','pod','container','namespace','interface','instance','method','status','reporter','le','envoy_response_code','envoy_cluster_name','destination_workload')
LABEL_IDX={n:i for i,n in enumerate(LABELS)}
FFT_PROMQL=(
    f'sum(rate(container_cpu_usage_seconds_total{{namespace="{NS_FILTER}"}}[15m]))',
    f'sum(rate({RPS_METRIC}{{destination_namespace="{NS_FILTER}"}}[15m]))',
    f'sum(rate(container_network_receive_bytes_total{{namespace="{NS_FILTER}"}}[15m]))/(sum(rate(container_network_transmit_bytes_total{{namespace="{NS_FILTER}"}}[15m]))+{EPS})',
    f'(sum(rate({LAT_METRIC}{{le="+Inf"}}[15m]))-sum(rate({LAT_METRIC}{{le="{SLOW_LE}"}}[15m])))/(sum(rate({LAT_METRIC}{{le="+Inf"}}[15m]))+{EPS})')

def read_varint(b,p):
    r=0;s=0
    while True:
        c=b[p];p+=1
        r|=(c&0x7f)<<s
        if not c&0x80:return r,p
        s+=7

def skip(b,p,w):
    if w==0:_,p=read_varint(b,p)
    elif w==1:p+=8
    elif w==2:
        l,p=read_varint(b,p);p+=l
    elif w==5:p+=4
    return p

def parse_ts(buf,out):
    for i in range(13):out[i]=None
    value=0.0;ts=0
    p=0;n=len(buf)
    while p<n:
        tag,p=read_varint(buf,p)
        f=tag>>3;w=tag&7
        if f==1 and w==2:
            l,p=read_varint(buf,p)
            lb=buf[p:p+l];p+=l
            lp=0;ln=len(lb);k=v=None
            while lp<ln:
                lt,lp=read_varint(lb,lp)
                lf=lt>>3
                ll,lp=read_varint(lb,lp)
                s=lb[lp:lp+ll];lp+=ll
                if lf==1:k=s.decode()
                elif lf==2:v=s.decode()
            i=LABEL_IDX.get(k)
            if i is not None:out[i]=v
        elif f==2 and w==2:
            l,p=read_varint(buf,p)
            sb=buf[p:p+l];p+=l
            sp=0;sn=len(sb)
            while sp<sn:
                st,sp=read_varint(sb,sp)
                sf=st>>3;sw=st&7
                if sf==1 and sw==1:
                    value=struct.unpack_from('<d',sb,sp)[0];sp+=8
                elif sf==2 and sw==0:ts,sp=read_varint(sb,sp)
                else:sp=skip(sb,sp,sw)
        else:p=skip(buf,p,w)
    return value,ts

def dep_of(pod):
    i=pod.rfind('-')
    if i<0:return pod
    j=pod.rfind('-',0,i)
    return pod[:j] if j>=0 else pod

class Pod:
    __slots__=('m','prev','pts','rate','dep','seen')
    def __init__(self,pod,epoch):
        self.m=[{},{},{},{}]
        self.prev=[None,None,None,None]
        self.pts=[0,0,0,0]
        self.rate=[0.0,0.0,0.0,0.0]
        self.dep=dep_of(pod)
        self.seen=epoch

def upd(m,key,value,ts):
    e=m.get(key)
    if e is None:m[key]=[value,ts]
    elif ts>=e[1]:e[0]=value;e[1]=ts

def supd(m,key,value,ts,epoch):
    e=m.get(key)
    if e is None:m[key]=[value,ts,epoch];return
    e[2]=epoch
    if ts>=e[1]:e[0]=value;e[1]=ts

def msum(m):
    s=0.0
    for e in m.values():s+=e[0]
    return s

def sweep(m,prev,epoch):
    for k in [k for k,e in m.items() if epoch-e[2]>STALE_CYCLES]:
        del m[k];prev.pop(k,None)

def logit(p):
    if p<0.0:p=0.0
    elif p>1.0:p=1.0
    return math.log((p+EPS_L)/(1.0-p+EPS_L))


def slow_from_buckets(bd,thr):
    tot=bd.get('+Inf',0.0)
    if tot<=0.0:return 0.0,0.0
    lo=0.0
    for k,v in bd.items():
        if k=='+Inf':continue
        try:f=float(k)
        except ValueError:continue
        if f<=thr and v>lo:lo=v
    return (tot-lo)/tot,tot


def p99_from_buckets(bd):
    if not bd:return 0.0,1
    items=sorted(((float('inf') if k=='+Inf' else float(k)),v) for k,v in bd.items())
    total=items[-1][1]
    if total<=0:return 0.0,1
    rank=0.99*total
    low_le=0.0;low_cum=0.0
    for le,cum in items:
        if cum>=rank:
            if le==float('inf'):return low_le,1
            if cum==low_cum:return le,0
            return low_le+(rank-low_cum)/(cum-low_cum)*(le-low_le),0
        low_le=le;low_cum=cum
    return low_le,1

SF2=np.frombuffer(SIGMA_FLOOR2,dtype=np.float64)
UCL_FIX=float(os.environ.get('JANUS_UCL','0') or 0)
UCL_CACHE={}
def ucl_for(n,p):
    if p<1 or n<=p+1:return float('inf')
    k=(n,p)
    u=UCL_CACHE.get(k)
    if u is None:
        d=n-p
        u=(p*(n-1)/d)*f_dist.ppf(1-MEWMA_ALPHA,p,d)
        if UCL_FIX>0.0:
            dr=n-MEWMA_DIM
            if p==MEWMA_DIM or dr<=0:u=UCL_FIX
            else:u=UCL_FIX*u/((MEWMA_DIM*(n-1)/dr)*f_dist.ppf(1-MEWMA_ALPHA,MEWMA_DIM,dr))
        UCL_CACHE[k]=u
    return u

RAT_CACHE={}
def ucl_rat(n,p):
    k=(n,p)
    r=RAT_CACHE.get(k)
    if r is None:
        dr=n-MEWMA_DIM
        if p<1 or n<=p+1 or dr<=0:r=1.0
        else:
            d=n-p
            a=(p*(n-1)/d)*f_dist.ppf(1-MEWMA_ALPHA,p,d)
            b=(MEWMA_DIM*(n-1)/dr)*f_dist.ppf(1-MEWMA_ALPHA,MEWMA_DIM,dr)
            r=a/b if b>0 else 1.0
        RAT_CACHE[k]=r
    return r

class StateMEWMA:
    __slots__=('E','V','pos','count','t','z','x','mu','sd','live','p','tr','det',
               '_Xc','_Ec','_C','_S','_zv','_xm','_iv','_nv','_nrm','_dirty','d','_Ss','_zs')
    def __init__(self):
        self.E=np.zeros((MEWMA_WINDOW,MEWMA_DIM))
        self.V=np.zeros((MEWMA_WINDOW,MEWMA_DIM))
        self._Xc=np.zeros((MEWMA_WINDOW,MEWMA_DIM))
        self._Ec=np.zeros((MEWMA_WINDOW,MEWMA_DIM))
        self._C=np.zeros((MEWMA_DIM,MEWMA_DIM))
        self._S=np.zeros(MEWMA_DIM*MEWMA_DIM)
        self._zv=np.zeros(MEWMA_DIM)
        self._xm=np.zeros(MEWMA_DIM)
        self._nv=np.zeros(MEWMA_DIM)
        self._nrm=np.ones(MEWMA_DIM)
        self._iv=array.array('i',[0]*MEWMA_DIM)
        self.mu=np.zeros(MEWMA_DIM)
        self.sd=np.zeros(MEWMA_DIM)
        self.z=array.array('d',[0.0]*MEWMA_DIM)
        self.x=array.array('d',[0.0]*MEWMA_DIM)
        self.live=bytearray(MEWMA_DIM)
        self.pos=0;self.count=0;self.t=0;self.p=0
        self.tr=0.0;self.det=0.0
        self._dirty=0
        self.d=array.array('d',[0.0]*MEWMA_DIM)
        self._Ss=np.zeros((MEWMA_DIM,MEWMA_DIM))
        self._zs=np.zeros(MEWMA_DIM)
    def _stats(self):
        n=self.count
        E=self.E[:n];V=self.V[:n]
        np.sum(V,axis=0,out=self._nv)
        np.maximum(self._nv,1.0,out=self._nv)
        np.multiply(E,V,out=self._Ec[:n])
        np.sum(self._Ec[:n],axis=0,out=self.mu)
        np.divide(self.mu,self._nv,out=self.mu)
        np.subtract(E,self.mu,out=self._Ec[:n])
        np.multiply(self._Ec[:n],V,out=self._Ec[:n])
        np.einsum('ij,ij->j',self._Ec[:n],self._Ec[:n],out=self.sd)
        np.divide(self.sd,self._nv,out=self.sd)
        np.sqrt(self.sd,out=self.sd)
        np.multiply(self.sd,self.sd,out=self._nrm)
        np.add(self._nrm,SF2,out=self._nrm)
        np.sqrt(self._nrm,out=self._nrm)
    def update(self,e,valid,frz=0):
        n=self.count
        if n>0 and not frz:
            self._dirty+=1
            if self._dirty>=RECOMP_EVERY:
                self._stats();self._dirty=0
        x=self.x;live=self.live;nl=0
        mu=self.mu;sd=self.sd
        for i in range(MEWMA_DIM):
            ok=valid[i] and n>=MEWMA_MIN_SAMPLES and sd[i]>SD_LIVE[i]
            live[i]=1 if ok else 0
            if valid[i]:
                x[i]=(e[i]-mu[i])/math.sqrt(sd[i]*sd[i]+SIGMA_FLOOR2[i])
            else:
                x[i]=0.0
            if ok:nl+=1
        self.p=nl
        self.t+=1
        lam=MEWMA_LAMBDA;inv=1.0-lam
        z=self.z
        for i in range(MEWMA_DIM):z[i]=lam*x[i]+inv*z[i] if live[i] else 0.0
        if not frz:
            po=self.pos
            for i in range(MEWMA_DIM):
                self.E[po,i]=e[i];self.V[po,i]=1.0 if valid[i] else 0.0
            self.pos=po+1 if po+1<MEWMA_WINDOW else 0
            if self.count<MEWMA_WINDOW:self.count+=1
        n=self.count
        if n<MEWMA_MIN_SAMPLES or nl<2:
            self.tr=0.0;self.det=0.0
            return 0.0,float('inf'),False
        iv=self._iv
        k=0
        for i in range(MEWMA_DIM):
            if live[i]:iv[k]=i;k+=1
        Xw=self._Xc[:n]
        np.subtract(self.E[:n],self.mu,out=Xw)
        np.divide(Xw,self._nrm,out=Xw)
        np.multiply(Xw,self.V[:n],out=Xw)
        np.mean(Xw,axis=0,out=self._xm)
        np.subtract(Xw,self._xm,out=Xw)
        C=self._C
        np.dot(Xw.T,Xw,out=C)
        np.divide(C,n-1,out=C)
        S=self._S[:nl*nl].reshape(nl,nl)
        g=(lam/(2.0-lam))*(1.0-inv**(2*self.t))
        tr=0.0
        for a in range(nl):
            ia=iv[a]
            for c in range(nl):S[a,c]=C[ia,iv[c]]*g
            tr+=C[ia,ia]
            S[a,a]+=MEWMA_RIDGE*g
        self.tr=tr/nl
        zv=self._zv[:nl]
        for j in range(nl):zv[j]=z[iv[j]]
        try:
            sg,ld=np.linalg.slogdet(S)
            if sg<=0:
                self.det=0.0
                return 0.0,float('inf'),False
            self.det=float(ld)
            t2=float(zv@np.linalg.solve(S,zv))
        except np.linalg.LinAlgError:
            self.tr=0.0;self.det=0.0
            return 0.0,float('inf'),False
        u=ucl_for(n,nl)
        return t2,u,t2>u

    def decomp(self,t2):
        d=self.d
        for i in range(MEWMA_DIM):d[i]=0.0
        nl=self.p
        if nl<3 or t2<=0.0:return 0
        iv=self._iv;S=self._S[:nl*nl].reshape(nl,nl);zv=self._zv[:nl]
        m=nl-1
        Ss=self._Ss[:m,:m];zs=self._zs[:m]
        for a in range(nl):
            r=0
            for i in range(nl):
                if i==a:continue
                c=0
                for j in range(nl):
                    if j==a:continue
                    Ss[r,c]=S[i,j];c+=1
                zs[r]=zv[i];r+=1
            try:
                v=float(zs@np.linalg.solve(Ss,zs))
            except np.linalg.LinAlgError:
                return 0
            d[iv[a]]=t2-v
        return nl

class BlockGARCH:
    __slots__=('h','Q','Qb','xp','up','t','sh_h','sh_Q','sh_Qb','sh_xp','sh_up','sh_t',
               'u','su','xis','calm','det','frozen_for')
    def __init__(self):
        self.h=array.array('d',[1.0]*3)
        self.Q=array.array('d',[1.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,1.0])
        self.Qb=array.array('d',[1.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,1.0])
        self.xp=array.array('d',[0.0]*3)
        self.up=array.array('d',[0.0]*3)
        self.t=0
        self.sh_h=array.array('d',[1.0]*3)
        self.sh_Q=array.array('d',[1.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,1.0])
        self.sh_Qb=array.array('d',[1.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,1.0])
        self.sh_xp=array.array('d',[0.0]*3)
        self.sh_up=array.array('d',[0.0]*3)
        self.sh_t=0
        self.u=array.array('d',[0.0]*3)
        self.su=array.array('d',[0.0]*3)
        self.xis=0.0
        self.calm=0
        self.det=1.0
        self.frozen_for=0
    def _adv(self,h,Q,Qb,xp,up,t,x0,x1,x2,live):
        if x0>X_CLIP:x0=X_CLIP
        elif x0<-X_CLIP:x0=-X_CLIP
        if x1>X_CLIP:x1=X_CLIP
        elif x1<-X_CLIP:x1=-X_CLIP
        if x2>X_CLIP:x2=X_CLIP
        elif x2<-X_CLIP:x2=-X_CLIP
        h[0]=GARCH_OMEGA+GARCH_ALPHA*xp[0]*xp[0]+GARCH_BETA*h[0]
        h[1]=GARCH_OMEGA+GARCH_ALPHA*xp[1]*xp[1]+GARCH_BETA*h[1]
        h[2]=GARCH_OMEGA+GARCH_ALPHA*xp[2]*xp[2]+GARCH_BETA*h[2]
        u0=x0/math.sqrt(h[0] if h[0]>H_FLOOR else H_FLOOR)
        u1=x1/math.sqrt(h[1] if h[1]>H_FLOOR else H_FLOOR)
        u2=x2/math.sqrt(h[2] if h[2]>H_FLOOR else H_FLOOR)
        xp[0]=x0;xp[1]=x1;xp[2]=x2
        t+=1
        w=1.0/(t if t<DCC_TCAP else DCC_TCAP)
        iw=1.0-w
        uu=(u0,u1,u2)
        for i in range(3):
            b=i*3;ui=uu[i]
            for j in range(3):Qb[b+j]=iw*Qb[b+j]+w*ui*uu[j]
        for i in range(3):
            b=i*3;pi=up[i]
            for j in range(3):Q[b+j]=DCC_CONST*Qb[b+j]+DCC_A*pi*up[j]+DCC_B*Q[b+j]
        for i in (0,4,8):
            if Q[i]<Q_FLOOR:Q[i]=Q_FLOOR
            if Qb[i]<Q_FLOOR:Qb[i]=Q_FLOOR
        up[0]=u0;up[1]=u1;up[2]=u2
        return t,u0,u1,u2
    def _rho(self,Q,i,j):
        d=math.sqrt(Q[i*3+i]*Q[j*3+j])
        if d<=0.0:return 0.0
        r=Q[i*3+j]/d
        if r>R_CLAMP:return R_CLAMP
        if r<-R_CLAMP:return -R_CLAMP
        return r
    def _xi(self,Q,u,live):
        n=live[0]+live[1]+live[2]
        if n==0:
            self.det=1.0
            return 0.0,0
        if n==1:
            i=0 if live[0] else (1 if live[1] else 2)
            self.det=1.0
            return u[i]*u[i],1
        if n==2:
            a,b=(0,1) if live[0] and live[1] else ((0,2) if live[0] else (1,2))
            r0=self._rho(Q,a,b)
            ua=u[a];ub=u[b]
            for g in SHRINK_LADDER:
                r=(1.0-g)*r0
                det=1.0-r*r
                if det>=DET_FLOOR:break
            self.det=det
            return (ua*ua+ub*ub-2.0*r*ua*ub)/det,2
        a01=self._rho(Q,0,1);a02=self._rho(Q,0,2);a12=self._rho(Q,1,2)
        u0=u[0];u1=u[1];u2=u[2]
        for g in SHRINK_LADDER:
            s=1.0-g
            r01=s*a01;r02=s*a02;r12=s*a12
            c00=1.0-r12*r12
            c01=r02*r12-r01
            c02=r01*r12-r02
            det=c00+r01*c01+r02*c02
            if det>=DET_FLOOR:break
        self.det=det
        c11=1.0-r02*r02
        c12=r02*r01-r12
        c22=1.0-r01*r01
        return (u0*u0*c00+u1*u1*c11+u2*u2*c22+
                2.0*(u0*u1*c01+u0*u2*c02+u1*u2*c12))/det,3
    def update(self,x0,x1,x2,live,fz,t2_ok):
        self.sh_t,su0,su1,su2=self._adv(self.sh_h,self.sh_Q,self.sh_Qb,self.sh_xp,
                                        self.sh_up,self.sh_t,x0,x1,x2,live)
        su=self.su;su[0]=su0;su[1]=su1;su[2]=su2
        self.xis,_=self._xi(self.sh_Q,su,live)
        u=self.u
        if fz:
            self.frozen_for+=1
            u[0]=x0/math.sqrt(self.h[0] if self.h[0]>H_FLOOR else H_FLOOR)
            u[1]=x1/math.sqrt(self.h[1] if self.h[1]>H_FLOOR else H_FLOOR)
            u[2]=x2/math.sqrt(self.h[2] if self.h[2]>H_FLOOR else H_FLOOR)
            self.calm=self.calm+1 if t2_ok else 0
            if self.calm>=N_REL:
                for i in range(3):
                    self.h[i]=self.sh_h[i];self.xp[i]=self.sh_xp[i];self.up[i]=self.sh_up[i]
                for i in range(9):
                    self.Q[i]=self.sh_Q[i];self.Qb[i]=self.sh_Qb[i]
                self.t=self.sh_t
                self.calm=0;self.frozen_for=0
                u[0]=x0/math.sqrt(self.h[0] if self.h[0]>H_FLOOR else H_FLOOR)
                u[1]=x1/math.sqrt(self.h[1] if self.h[1]>H_FLOOR else H_FLOOR)
                u[2]=x2/math.sqrt(self.h[2] if self.h[2]>H_FLOOR else H_FLOOR)
        else:
            self.frozen_for=0;self.calm=0
            self.t,u[0],u[1],u[2]=self._adv(self.h,self.Q,self.Qb,self.xp,self.up,
                                            self.t,x0,x1,x2,live)
        xi,df=self._xi(self.Q,u,live)
        return xi,df

class Router:
    __slots__=('ma','mb','la','lb','out')
    def __init__(self):
        self.ma=0;self.mb=0;self.la=0;self.lb=0;self.out=0
    def step(self,xa,xb,ta,tb,ready):
        if not ready:
            self.ma=0;self.mb=0;self.la=0;self.lb=0;self.out=0
            return 0
        ma=((self.ma<<1)|(1 if xa>=ta else 0))&HIT_MASK
        mb=((self.mb<<1)|(1 if xb>=tb else 0))&HIT_MASK
        self.ma=ma;self.mb=mb
        if POPCNT[ma]>=2:self.la=LATCH_TICKS
        elif self.la>0:self.la-=1
        if POPCNT[mb]>=2:self.lb=LATCH_TICKS
        elif self.lb>0:self.lb-=1
        self.out=(1 if self.la>0 else 0)|(2 if self.lb>0 else 0)
        return self.out

class Cand:
    __slots__=('id','ucl','latch','cool','dthr','blkA','blkB','router',
               'key','run','cn','st','n_trig','n_attr','n_dec','n_act')
    def __init__(self,d):
        self.id=str(d.get('id','?'))
        self.ucl=float(d['ucl'])
        self.latch=int(d.get('latch',DEC_LATCH))
        self.cool=int(d.get('cool',DEC_COOL))
        self.dthr=array.array('d',[0.0]*MEWMA_DIM)
        _t=d.get('dthr') or []
        for _i in range(min(MEWMA_DIM,len(_t))):self.dthr[_i]=float(_t[_i])
        self.blkA=BlockGARCH();self.blkB=BlockGARCH();self.router=Router()
        self.key=-1;self.run=0;self.cn=0;self.st=0
        self.n_trig=0;self.n_attr=0;self.n_dec=0;self.n_act=0

CANDS=[]
if EVAL_FILE:
    try:
        with open(EVAL_FILE) as _fh:
            for _d in json.load(_fh):CANDS.append(Cand(_d))
        print('EVAL nap %d ung vien tu %s'%(len(CANDS),EVAL_FILE))
        for _c in CANDS:
            print('EVAL cand=%s ucl=%.4f latch=%d cool=%d dthr=%s'%(
                _c.id,_c.ucl,_c.latch,_c.cool,','.join('%.4f'%_v for _v in _c.dthr)))
    except Exception as _ex:
        print('EVAL LOI doc %s: %s: %s -- bo danh gia TAT'%(EVAL_FILE,type(_ex).__name__,_ex))
        CANDS=[]

class Harmonics:
    __slots__=('mean','sl','a','f','p','k')
    def __init__(self):
        self.a=array.array('d',[0.0]*K_MAX)
        self.f=array.array('d',[0.0]*K_MAX)
        self.p=array.array('d',[0.0]*K_MAX)
        self.mean=0.0
        self.sl=0.0
        self.k=0
    def at(self,tau):
        s=self.mean+self.sl*tau;a=self.a;f=self.f;p=self.p
        for i in range(self.k):s+=a[i]*math.cos(6.283185307179586*f[i]*tau+p[i])
        return s

class HarmonicSet:
    __slots__=('active','shadow','t0a','t0s','ve')
    def __init__(self):
        n=len(FFT_DIM_IDX)
        self.active=[Harmonics() for _ in range(n)]
        self.shadow=[Harmonics() for _ in range(n)]
        self.t0a=array.array('d',[0.0]*n)
        self.t0s=array.array('d',[0.0]*n)
        self.ve=array.array('d',[0.0]*n)
    def swap(self,fi):
        self.active[fi],self.shadow[fi]=self.shadow[fi],self.active[fi]
        self.t0a[fi],self.t0s[fi]=self.t0s[fi],self.t0a[fi]

hset=HarmonicSet()
last_valid=array.array('d',[0.0]*MEWMA_DIM)
y_buf=array.array('d',[0.0]*MEWMA_DIM)
e_buf=array.array('d',[0.0]*MEWMA_DIM)
yhat_buf=array.array('d',[0.0]*MEWMA_DIM)
valid_buf=bytearray(MEWMA_DIM)
valid_buf[0]=1;valid_buf[1]=1;valid_buf[2]=1
liveA=bytearray(3)
liveB=bytearray(3)
calib_a=array.array('d')
calib_b=array.array('d')
calib_t2=array.array('d')
calib_seen=array.array('q',[0,0,0])
calib_d=[array.array('d') for _ in range(MEWMA_DIM)]
dep_mu=array.array('d',[0.0]*(MEWMA_DIM*NDEP))
dep_m2=array.array('d',[0.0]*(MEWMA_DIM*NDEP))
dep_n=array.array('d',[0.0]*(MEWMA_DIM*NDEP))
dep_z=array.array('d',[0.0]*(MEWMA_DIM*NDEP))
dep_rp=array.array('d',[0.0]*NDEP)
dep_ro=bytearray(NDEP)
dep_pv=array.array('d',[0.0]*(MEWMA_DIM*NDEP))
dep_po=bytearray(MEWMA_DIM*NDEP)
d_rps=array.array('d',[0.0]*NDEP)
dec_rep=array.array('i',[0]*NDEP)
dec_key=-1
dec_run=0
dec_cool=0
dec_st=0
dec_fh=None
frz=array.array('i',[0,0,0])
dec_act=array.array('i',[0])
d_err=array.array('d',[0.0]*NDEP)
bdd=[{} for _ in range(NDEP)]

def dep_upd(j,v,s):
    k=s*NDEP+j
    if s!=2 and s!=3 and s!=5 and dep_po[k] and v==dep_pv[k]:return
    dep_pv[k]=v;dep_po[k]=1
    if FRZ_DEP and frz[0]:
        n=dep_n[k]
        if n>=DEP_MIN:dep_z[k]=(v-dep_mu[k])/math.sqrt(dep_m2[k]/(n-1.0)+SIGMA_FLOOR2[s])
        return
    n=dep_n[k]+1.0
    if n>DEP_NCAP:n=DEP_NCAP
    dep_n[k]=n
    dl=v-dep_mu[k]
    dep_mu[k]+=dl/n
    dep_m2[k]+=dl*(v-dep_mu[k])
    if n>=DEP_MIN:
        dep_z[k]=(v-dep_mu[k])/math.sqrt(dep_m2[k]/(n-1.0)+SIGMA_FLOOR2[s])
    else:dep_z[k]=0.0

def fft_fit(y):
    ya=np.frombuffer(y,dtype=np.float64)
    n=ya.shape[0]
    tm=np.arange(n,dtype=np.float64)
    tm-=tm.mean()
    med0=float(np.median(ya))
    sd=float(np.median(np.abs(ya-med0)))
    lim0=FFT_W_MAD*1.4826*sd
    yw=np.clip(ya,med0-lim0,med0+lim0) if lim0>0.0 else ya
    sl=float((tm*(yw-med0)).sum()/(tm*tm).sum())
    yc=ya-med0-sl*tm
    med1=float(np.median(yc))
    lim1=FFT_W_MAD*1.4826*float(np.median(np.abs(yc-med1)))
    nclip=0
    if lim1>0.0:
        nclip=int((np.abs(yc-med1)>lim1).sum())
        if nclip:yc=np.clip(yc,med1-lim1,med1+lim1)
    dc=float(yc.mean())
    yc=yc-dc
    m=med0+dc
    var=float(yc.var())
    Y=np.fft.rfft(yc,n)
    P=np.abs(Y)
    tail=P[1:]
    pm=float(np.median(tail))
    pd=float(np.median(np.abs(tail-pm)))
    pk=float(tail.max())/pm if pm>0 else 0.0
    idx=np.nonzero(tail>pm+Z_MAD*1.4826*pd)[0]+1
    idx=idx[idx>=FFT_KMIN]
    if idx.size==0:return m,np.zeros(0),np.zeros(0),np.zeros(0),0.0,nclip,sl,pk,sd
    nb=P.shape[0]
    sel=np.zeros(nb,dtype=bool)
    sel[idx]=True
    al=[];fl=[];pl=[]
    for kb in idx:
        kb=int(kb)
        if P[kb]<=P[kb-1]:continue
        if kb+1<nb and P[kb]<P[kb+1]:continue
        lo=kb
        while lo>1 and sel[lo-1] and P[lo-1]<=P[lo] and kb-lo<FFT_GRP:lo-=1
        hi=kb
        while hi+1<nb and sel[hi+1] and P[hi+1]<=P[hi] and hi-kb<FFT_GRP:hi+=1
        g=P[lo:hi+1]
        al.append(2.0*math.sqrt(float((g*g).sum()))/n)
        fl.append(kb/(n*FFT_DT))
        pl.append(math.atan2(float(Y[kb].imag),float(Y[kb].real)))
    if not al:return m,np.zeros(0),np.zeros(0),np.zeros(0),0.0,nclip,sl,pk,sd
    A=np.array(al);F=np.array(fl);PH=np.array(pl)
    if A.size>K_MAX:
        sel2=np.sort(np.argsort(A)[::-1][:K_MAX])
        A=A[sel2];F=F[sel2];PH=PH[sel2]
    ve=100.0*float((A*A).sum()/2.0)/var if var>0 else 0.0
    return m,A,F,PH,ve,nclip,sl,pk,sd

def _http_get(url):
    req=urllib.request.Request(url)
    if MIMIR_TENANT:req.add_header('X-Scope-OrgID',MIMIR_TENANT)
    try:
        with urllib.request.urlopen(req,timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f'HTTP {e.code} {e.reason}: {e.read().decode(errors="replace")[:300]}') from None

async def query_range(promql,start,end):
    q=urllib.parse.urlencode({'query':promql,'start':start,'end':end,'step':FFT_DT})
    return await asyncio.to_thread(_http_get,f'{MIMIR_QUERY_URL}?{q}')

def to_series(resp,start,n):
    out=array.array('d',[0.0]*n)
    got=bytearray(n)
    try:vals=resp['data']['result'][0]['values']
    except (KeyError,IndexError,TypeError):vals=()
    for ts,v in vals:
        i=int(round((float(ts)-start)/FFT_DT))
        if 0<=i<n:
            f=float(v)
            if f==f:out[i]=f;got[i]=1
    idx=[i for i in range(n) if got[i]]
    if not idx:return out,0,n
    first=idx[0];last=idx[-1]
    mx=first
    for a in range(len(idx)-1):
        i0=idx[a];i1=idx[a+1]
        g=i1-i0-1
        if g>mx:mx=g
        if g:
            y0=out[i0];d=(out[i1]-y0)/(i1-i0)
            for k in range(1,g+1):out[i0+k]=y0+d*k
    if n-1-last>mx:mx=n-1-last
    v0=out[first];v1=out[last]
    for i in range(first):out[i]=v0
    for i in range(last+1,n):out[i]=v1
    return out,len(idx),mx

def fft_disable(fi,start,ve):
    h=hset.shadow[fi]
    h.k=0;h.mean=0.0;h.sl=0.0
    hset.t0s[fi]=start;hset.ve[fi]=ve
    hset.swap(fi)


async def fft_slow():
    while True:
        end=time.time()
        start=end-FFT_N*FFT_DT
        start-=start%FFT_DT
        qend=start+(FFT_N-1)*FFT_DT
        for fi in range(len(FFT_DIM_IDX)):
            nm=FFT_DIM_NAMES[fi]
            try:
                y,cov,gap=to_series(await query_range(FFT_PROMQL[fi],start,qend),start,FFT_N)
                if cov<FFT_COV_MIN or gap>FFT_MAX_GAP:
                    print(f'FFT_SLOW reject dim={nm} ly_do='
                          f'{"coverage" if cov<FFT_COV_MIN else "lo_hong"} '
                          f'coverage={cov}/{FFT_N} lo_lon_nhat={gap} '
                          f'({gap*FFT_DT/3600:.1f}h, tran={FFT_MAX_GAP})')
                    fft_disable(fi,start,0.0)
                    continue
                tf=FFT_TF[fi]
                if tf==1:
                    for i in range(FFT_N):y[i]=math.log(y[i]+EPS_L)
                elif tf==2:
                    for i in range(FFT_N):y[i]=logit(y[i])
                sc=FFT_SCALE[fi]
                if sc!=1.0:
                    for i in range(FFT_N):y[i]*=sc
                m,A,F,PH,ve,nclip,sl,pk,sd=fft_fit(y)
                if abs(sl)*FFT_N>FFT_TREND_MAX*sd:
                    print(f'FFT_SLOW reject dim={nm} ly_do=trend_lon trend={sl:.4g}/bin '
                          f'sl_x_N={abs(sl)*FFT_N:.4g} sd={sd:.4g} '
                          f'ty_le={abs(sl)*FFT_N/(sd+EPS):.2f} (tran={FFT_TREND_MAX}) '
                          f'clip={nclip} coverage={cov}/{FFT_N} lo={gap}')
                    fft_disable(fi,start,ve)
                    continue
                if ve<FFT_VE_MIN:
                    print(f'FFT_SLOW reject dim={nm} ly_do=ve_thap ve={ve:.1f}% '
                          f'(toi_thieu={FFT_VE_MIN}) bins={len(A)} dinh/nen={pk:.2f} '
                          f'clip={nclip} trend={sl:.4g}/bin coverage={cov}/{FFT_N} lo={gap}')
                    fft_disable(fi,start,ve)
                    continue
                h=hset.shadow[fi];k=len(A)
                for i in range(k):h.a[i]=float(A[i]);h.f[i]=float(F[i]);h.p[i]=float(PH[i])
                h.mean=m-sl*(FFT_N-1)*0.5;h.sl=sl/FFT_DT;h.k=k
                hset.t0s[fi]=start;hset.ve[fi]=ve
                hset.swap(fi)
                per=' '.join(f'{1.0/(F[i]*3600.0):.1f}h' for i in range(min(k,4)))
                print(f'FFT_SLOW ok dim={nm} bins={k} ve={ve:.1f}% dinh/nen={pk:.2f} '
                      f'clip={nclip} trend={sl:.4g}/bin coverage={cov}/{FFT_N} lo={gap} '
                      f'chu_ky=[{per}]')
            except Exception as ex:
                print(f'FFT_SLOW error dim={nm}: {type(ex).__name__}: {ex}')
        await asyncio.sleep(FFT_INTERVAL_SEC)

pod_shards=[{} for _ in range(SHARDS)]
hub={};hub_prev={}
hist={};hist_prev={}
env={};env_prev={}
mewma=StateMEWMA()
blkA=BlockGARCH()
blkB=BlockGARCH()
router=Router()
ring=array.array('q',[0]*RING_CAP)
proc_ring=array.array('q',[0]*RING_CAP)
ring_write=0
ring_read=0
epoch=0
_lbl=[None]*13
ram_prev=0.0
ram_init=False
prev_mono=0.0
prev_tA=-1
prev_tB=-1

_gf=array.array('i',[0]*4)
_gb=array.array('i',[0]*4)
_gs=array.array('i',[0]*4)
_gx=array.array('d',[0.0]*4)
_gn=array.array('d',[0.0]*4)
_gki=array.array('i',[0]*4)
_gk=[None,None,None,None]

def pdelta(m,e,i):
    cur=0.0;ct=0
    for v in m.values():
        cur+=v[0]
        if v[1]>ct:ct=v[1]
    pv=e.prev[i];pt=e.pts[i]
    if pv is None:
        e.prev[i]=cur;e.pts[i]=ct
        return 0.0
    dt=(ct-pt)*1e-3
    if dt<=0.0:
        _gs[0]+=1
        return e.rate[i]
    e.prev[i]=cur;e.pts[i]=ct
    if dt>_gx[0]:_gx[0]=dt;_gk[0]=e.dep;_gki[0]=i
    if _gn[0]==0.0 or dt<_gn[0]:_gn[0]=dt
    if dt>DT_MAX_POD:
        _gb[0]+=1
        e.rate[i]=0.0
        return 0.0
    _gf[0]+=1
    d=cur-pv
    r=d*FLUSH_SEC/dt if d>0.0 else 0.0
    e.rate[i]=r
    return r

def series_delta(cur,prev,idx,g):
    tot=0.0;err=0.0
    for k,e in cur.items():
        ct=e[1];p=prev.get(k)
        if p is None:
            prev[k]=[e[0],ct];continue
        dt=(ct-p[1])*1e-3
        if dt<=0.0:
            _gs[g]+=1;continue
        d=e[0]-p[0]
        p[0]=e[0];p[1]=ct
        if dt>_gx[g]:_gx[g]=dt;_gk[g]=k
        if _gn[g]==0.0 or dt<_gn[g]:_gn[g]=dt
        if dt>DT_MAX:
            _gb[g]+=1;continue
        _gf[g]+=1
        if d<0.0:d=0.0
        d*=FLUSH_SEC/dt
        tot+=d
        c=k[idx]
        if c and c[0] in '45':err+=d
    return tot,err

def pct(a,q):
    if not len(a):return float('nan')
    return float(np.percentile(np.frombuffer(a,dtype=np.float64),q))

async def flush_loop():
    global ring_read,epoch,ram_prev,ram_init,prev_mono,prev_tA,prev_tB,THR_A,THR_B,dec_key,dec_run,dec_cool,dec_fh,dec_st
    while True:
        await asyncio.sleep(FLUSH_SEC)
        epoch+=1
        now=time.perf_counter_ns()
        w=ring_write;r=ring_read
        cnt=w-r
        if cnt>RING_CAP:r=w-RING_CAP;cnt=RING_CAP
        lmax=0;lsum=0;pmax=0;psum=0
        for i in range(r,w):
            k=i%RING_CAP
            d=now-ring[k]
            if d>lmax:lmax=d
            lsum+=d
            q=proc_ring[k]
            if q>pmax:pmax=q
            psum+=q
        ring_read=w
        f=1.0/cnt if cnt else 0.0
        for i in range(4):_gf[i]=0;_gb[i]=0;_gs[i]=0;_gx[i]=0.0;_gn[i]=0.0;_gk[i]=None
        dep_cpu={};dep_ram={};dep_nin={};dep_nout={}
        for sh in pod_shards:
            for pod in [p for p,e in sh.items() if epoch-e.seen>STALE_CYCLES]:del sh[pod]
            for e in sh.values():
                d=e.dep
                dep_cpu[d]=dep_cpu.get(d,0.0)+pdelta(e.m[CPU],e,CPU)
                dep_nin[d]=dep_nin.get(d,0.0)+pdelta(e.m[NIN],e,NIN)
                dep_nout[d]=dep_nout.get(d,0.0)+pdelta(e.m[NOUT],e,NOUT)
                dep_ram[d]=dep_ram.get(d,0.0)+msum(e.m[RAM])
        sweep(hub,hub_prev,epoch);sweep(hist,hist_prev,epoch);sweep(env,env_prev,epoch)
        for _i in range(NDEP):d_rps[_i]=0.0;d_err[_i]=0.0
        rps=0.0;err=0.0
        for k,e in hub.items():
            ct=e[1];p=hub_prev.get(k)
            if p is None:
                hub_prev[k]=[e[0],ct];continue
            dt=(ct-p[1])*1e-3
            if dt<=0.0:
                _gs[1]+=1;continue
            d=e[0]-p[0]
            p[0]=e[0];p[1]=ct
            if dt>_gx[1]:_gx[1]=dt;_gk[1]=k
            if _gn[1]==0.0 or dt<_gn[1]:_gn[1]=dt
            if dt>DT_MAX:
                _gb[1]+=1;continue
            _gf[1]+=1
            if d<0.0:d=0.0
            d*=FLUSH_SEC/dt
            rps+=d
            c=k[2];eb=c and c[0] in '45'
            if eb:err+=d
            j=DEP_IDX.get(k[4],-1)
            if j>=0:
                d_rps[j]+=d
                if eb:d_err[j]+=d
        err_rate=err/(rps+EPS)
        ing_tot,ing_err=series_delta(env,env_prev,1,3)
        ing_rate=ing_err/(ing_tot+EPS)
        bd={}
        for _b in bdd:_b.clear()
        for k,e in hist.items():
            ct=e[1];p=hist_prev.get(k)
            if p is None:
                hist_prev[k]=[e[0],ct];continue
            dt=(ct-p[1])*1e-3
            if dt<=0.0:
                _gs[2]+=1;continue
            d=e[0]-p[0]
            p[0]=e[0];p[1]=ct
            if dt>_gx[2]:_gx[2]=dt;_gk[2]=k
            if _gn[2]==0.0 or dt<_gn[2]:_gn[2]=dt
            if dt>DT_MAX:
                _gb[2]+=1;continue
            _gf[2]+=1
            if d<0.0:d=0.0
            le=k[4];dv2=d*FLUSH_SEC/dt
            bd[le]=bd.get(le,0.0)+dv2
            j=DEP_IDX.get(k[5],-1)
            if j>=0:
                b2=bdd[j];b2[le]=b2.get(le,0.0)+dv2
        p99,p99_sat=p99_from_buckets(bd)
        slow,p99_total=slow_from_buckets(bd,SLOW_THR)
        tcpu=0.0;tram=0.0;tnin=0.0;tnout=0.0
        for d in dep_cpu:
            tcpu+=dep_cpu[d];tram+=dep_ram.get(d,0.0)
            tnin+=dep_nin.get(d,0.0);tnout+=dep_nout.get(d,0.0)
        asym=tnin/(tnout+EPS)
        mono=time.monotonic()
        dw=(mono-prev_mono) if prev_mono>0.0 else FLUSH_SEC
        prev_mono=mono
        rd=(tram-ram_prev)*(FLUSH_SEC/dw) if ram_init and dw>0.0 else 0.0
        ram_prev=tram;ram_init=True
        for j in range(NDEP):
            nm=DEP_LIST[j]
            c=dep_cpu.get(nm)
            if c is not None:
                dep_upd(j,c,0)
                rr=dep_ram.get(nm,0.0)
                if dep_ro[j] and dw>0.0:dep_upd(j,(rr-dep_rp[j])*(FLUSH_SEC/dw),1)
                dep_rp[j]=rr;dep_ro[j]=1
                no=dep_nout.get(nm,0.0)
                if no>=MIN_BYTES:dep_upd(j,math.log(dep_nin.get(nm,0.0)/(no+EPS)+EPS_L),4)
            rj=d_rps[j]
            if rj>0.0:dep_upd(j,rj,2)
            if rj>=MIN_REQ:dep_upd(j,logit(d_err[j]/rj),3)
            sv,st2=slow_from_buckets(bdd[j],SLOW_THR)
            if st2>=MIN_REQ:dep_upd(j,logit(sv),5)
        ok0=_gb[0]==0 and (_gf[0]>0 or _gs[0]>0)
        ok1=_gf[1]>0 and _gb[1]==0
        ok2=_gf[2]>0 and _gb[2]==0
        valid_buf[0]=1 if ok0 else 0
        valid_buf[1]=1 if ok0 else 0
        valid_buf[2]=1 if ok1 else 0
        valid_buf[3]=1 if ok1 and rps>=MIN_REQ else 0
        valid_buf[4]=1 if ok0 and tnout>=MIN_BYTES else 0
        valid_buf[5]=1 if ok2 and p99_total>=MIN_REQ else 0
        y=y_buf
        y[0]=tcpu;y[1]=rd;y[2]=rps
        y[3]=logit(err_rate) if valid_buf[3] else last_valid[3]
        y[4]=math.log(asym+EPS_L) if valid_buf[4] else last_valid[4]
        y[5]=logit(slow) if valid_buf[5] else last_valid[5]
        for i in range(MEWMA_DIM):
            if valid_buf[i]:last_valid[i]=y[i]
            else:y[i]=last_valid[i]
        now_wall=time.time()
        res=e_buf;yh=yhat_buf
        for fi in range(len(FFT_DIM_IDX)):
            di=FFT_DIM_IDX[fi]
            a=hset.active[fi]
            yh[di]=a.at(now_wall-hset.t0a[fi])*FFT_EVAL_SCALE[fi] if a.k else 0.0
            res[di]=y[di]-yh[di]
        yh[1]=0.0;res[1]=y[1]
        yh[3]=0.0;res[3]=y[3]
        st=router.out
        fzA=(st&1)!=0;fzB=(st&2)!=0
        t2,ucl,trig=mewma.update(res,valid_buf,frz[0])
        lv=mewma.live
        for i in range(3):
            liveA[i]=lv[i];liveB[i]=lv[i+3]
        nA=liveA[0]+liveA[1]+liveA[2]
        nB=liveB[0]+liveB[1]+liveB[2]
        mx=mewma.x
        t2ok=not trig
        warm=mewma.count>=MEWMA_MIN_SAMPLES
        if warm:
            xiA,dfA=blkA.update(mx[0],mx[1],mx[2],liveA,fzA,t2ok)
            xiB,dfB=blkB.update(mx[3],mx[4],mx[5],liveB,fzB,t2ok)
        else:
            xiA=xiB=0.0;dfA=dfB=0
        thrA=THR_A if THR_A>0 else CHI2_99[dfA]
        thrB=THR_B if THR_B>0 else CHI2_99[dfB]
        ready=blkA.sh_t>=GARCH_MIN_TICKS and nA>0 and nB>0
        if CALIB_TICKS>0 and ready and len(calib_a)<CALIB_CAP:
            calib_seen[0]+=1
            if calib_seen[0]%CALIB_STRIDE==0:
                calib_a.append(xiA);calib_b.append(xiB)
                if len(calib_a)>=CALIB_TICKS:
                    pa=pct(calib_a,99);pb=pct(calib_b,99)
                    print(f'CALIB n={len(calib_a)} quet={calib_seen[0]} stride={CALIB_STRIDE} xiA_p99={pa:.4f} xiB_p99={pb:.4f}')
                    print(f'CALIB export: JANUS_THR_A={pa:.4f} JANUS_THR_B={pb:.4f}')
                    if THR_ENV:print(f'CALIB delta A={100.0*(pa-THR_A)/(THR_A+EPS):+.1f}% B={100.0*(pb-THR_B)/(THR_B+EPS):+.1f}% (env thang, khong ghi de)')
                    else:THR_A=pa;THR_B=pb
                    del calib_a[:];del calib_b[:];calib_seen[0]=0
        if CALIB_TICKS>0 and mewma.p==MEWMA_DIM and mewma.count>=MEWMA_WINDOW and mewma.tr<TR_MAX and len(calib_t2)<CALIB_CAP:
            calib_seen[1]+=1
            if calib_seen[1]%CALIB_STRIDE==0:
                calib_t2.append(t2)
                if len(calib_t2)>=CALIB_TICKS:
                    pt=pct(calib_t2,99)
                    print(f'UCALIB n={len(calib_t2)} quet={calib_seen[1]} stride={CALIB_STRIDE} T2_p99={pt:.4f} T2_p999={pct(calib_t2,99.9):.4f} T2_max={max(calib_t2):.4f}')
                    print(f'UCALIB export: JANUS_UCL={pt:.4f}')
                    del calib_t2[:];calib_seen[1]=0
        st=router.step(xiA,xiB,thrA,thrB,ready)
        stall=''
        if blkA.sh_t==prev_tA or blkB.sh_t==prev_tB:stall=' STALL'
        prev_tA=blkA.sh_t;prev_tB=blkB.sh_t
        uz=mewma.z;un1=0;un2=0;uw=-1;um=0.0
        if warm:
            for i in range(MEWMA_DIM):
                if not mewma.live[i]:continue
                av=uz[i]
                if av<0.0:av=-av
                if av>um:um=av;uw=i
                if av>UNI_K:un1+=1
                if av>UNI_K2:un2+=1
        nlk=mewma.decomp(t2) if warm else 0
        dv=mewma.d
        if nlk==MEWMA_DIM and not trig and mewma.tr<TR_MAX and DCALIB_TICKS>0 and len(calib_d[0])<CALIB_CAP:
            calib_seen[2]+=1
            if calib_seen[2]%DCALIB_STRIDE==0:
                for i in range(MEWMA_DIM):calib_d[i].append(dv[i])
                if len(calib_d[0])>=DCALIB_TICKS:
                    ps=[pct(calib_d[i],99) for i in range(MEWMA_DIM)]
                    print(f'DCALIB n={len(calib_d[0])} quet={calib_seen[2]} stride={DCALIB_STRIDE} '+' '.join(f'{DIMS[i]}={ps[i]:.4f}' for i in range(MEWMA_DIM)))
                    print('DCALIB export: JANUS_DTHR='+','.join(f'{v:.4f}' for v in ps))
                    if DTHR_ENV:
                        print('DCALIB delta '+' '.join(f'{DIMS[i]}={100.0*(ps[i]-DTHR[i])/(DTHR[i]+EPS):+.1f}%' for i in range(MEWMA_DIM))+' (env thang, khong ghi de)')
                    else:
                        for i in range(MEWMA_DIM):DTHR[i]=ps[i]
                    for i in range(MEWMA_DIM):del calib_d[i][:]
                    calib_seen[2]=0
        rat=1
        for i in range(MEWMA_DIM):
            if DTHR[i]<=0.0:rat=0;break
        wi=-1;wd=0.0;ws=0.0
        if rat:
            for i in range(MEWMA_DIM):
                if dv[i]>ws*DTHR[i]:ws=dv[i]/DTHR[i];wi=i;wd=dv[i]
        else:
            for i in range(MEWMA_DIM):
                if dv[i]>wd:wd=dv[i];wi=i
        sg=wi>=0 and (DTHR[wi]<=0.0 or wd>DTHR[wi])
        ah=0;bh=0
        for i in range(MEWMA_DIM):
            if DTHR[i]>0.0 and dv[i]>DTHR[i]:
                if i<3:ah=1
                else:bh=1
        tax=ah|(bh<<1)
        wdepa=-1;wz=0.0
        if wi>=0:
            for j in range(NDEP):
                az=dep_z[wi*NDEP+j]
                if az<0.0:az=-az
                if az>wz:wz=az;wdepa=j
        wdep=wdepa if sg else -1
        fftk=0
        for _h in hset.active:
            if _h.k:fftk+=1
        print(f'--- window={FLUSH_SEC:.0f}s dw={dw:.3f}s packets={cnt} fresh={_gf[0]}/{_gf[1]}/{_gf[2]}/{_gf[3]} gap={_gb[0]}/{_gb[1]}/{_gb[2]}/{_gb[3]} proc_us(avg={psum*f/1e3:.1f},max={pmax/1e3:.1f}) wait_ms(avg={lsum*f/1e6:.3f},max={lmax/1e6:.3f}) ---')
        print(f'DT cap={DT_MAX:.1f} stale={_gs[0]}/{_gs[1]}/{_gs[2]}/{_gs[3]} min={_gn[0]:.3f}/{_gn[1]:.3f}/{_gn[2]:.3f}/{_gn[3]:.3f} max={_gx[0]:.3f}/{_gx[1]:.3f}/{_gx[2]:.3f}/{_gx[3]:.3f} worst0={_gk[0]}#{_gki[0]} worst1={_gk[1]} worst2={_gk[2]} capPod={DT_MAX_POD:.0f}')
        print(f'STATE cpu={tcpu:.6f} ram={rd:+.0f} rps={rps:.2f} err={err_rate:.6f} asym={asym:.4f} slow={slow:.6f}(>{SLOW_LE}s) p99={p99:.4f}{"[TRAN]" if p99_sat else ""} n_lat={p99_total:.0f} fft_ve=['+' '.join(f'{FFT_DIM_NAMES[i]}={hset.ve[i]:.0f}%' for i in range(len(FFT_DIM_IDX)))+']')
        print(f'MEWMA T2={t2:.4f} ucl={ucl:.4f} trig={trig} n={mewma.count} p={mewma.p} tr={mewma.tr:.4f} logdet={mewma.det:.3f} ingress_err={ing_rate:.4f}')
        print(f'DCC xiA={xiA:.4f}/{thrA:.4f}(df{dfA}) xiB={xiB:.4f}/{thrB:.4f}(df{dfB}) detA={blkA.det:.4e} detB={blkB.det:.4e} fz={int(fzA)}{int(fzB)} frz={blkA.frozen_for}/{blkB.frozen_for} calm={blkA.calm}/{blkB.calm} t={blkA.t}/{blkA.sh_t} STATE={STATES[st]}{stall}')
        print('  FFT '+' '.join(f'{DIMS[i]}={y[i]:.4f}->{yh[i]:.4f}->{res[i]:+.4f}' for i in range(MEWMA_DIM)))
        print('  Z '+' '.join(f'{DIMS[i]}={mewma.x[i]:+.3f}/{mewma.z[i]:+.3f}{"" if lv[i] else "*"}' for i in range(MEWMA_DIM)))
        print('  SD '+' '.join(f'{DIMS[i]}={mewma.sd[i]:.6g}(m={mewma.mu[i]:.6g})' for i in range(MEWMA_DIM)))
        print(f'  UNI trig={int(un1>0)} n_k{UNI_K:.0f}={un1} n_k{UNI_K2:.0f}={un2} max={DIMS[uw] if uw>=0 else "-"} z={um:.3f} mewma={int(trig)} t2={t2:.3f}')
        print('  D '+' '.join(f'{DIMS[i]}={dv[i]:.3f}' for i in range(MEWMA_DIM))+f' nl={nlk} fftk={fftk} tax={tax}')
        if trig:
            print(f'  ATTR dim={DIMS[wi] if wi>=0 else "-"} d={wd:.3f} thr={DTHR[wi] if wi>=0 else 0.0:.3f} sig={int(sg)} rule={"rat" if rat else "raw"} dep={DEP_LIST[wdep] if wdep>=0 else "UNATTRIBUTED"} z={wz:+.3f}')
        for _i in range(NDEP):dec_rep[_i]=0
        for _sh in pod_shards:
            for _pd in _sh.values():
                _j=DEP_IDX.get(_pd.dep,-1)
                if _j>=0:dec_rep[_j]+=1
        print('  REP '+' '.join(f'{DEP_LIST[_j]}={dec_rep[_j]}' for _j in range(NDEP))+' MU '+' '.join(f'{dep_mu[2*NDEP+_j]:.3f}' for _j in range(NDEP)))
        if CANDS:
            ur=ucl_rat(mewma.count,mewma.p)
            for c in CANDS:
                uc=c.ucl*ur
                tg=t2>uc
                if warm:
                    xa,da=c.blkA.update(mewma.x[0],mewma.x[1],mewma.x[2],liveA,(c.router.out&1)!=0,not tg)
                    xb,db=c.blkB.update(mewma.x[3],mewma.x[4],mewma.x[5],liveB,(c.router.out&2)!=0,not tg)
                else:
                    xa=xb=0.0;da=db=0
                c.router.step(xa,xb,THR_A if THR_A>0 else CHI2_99[da],
                              THR_B if THR_B>0 else CHI2_99[db],
                              c.blkA.sh_t>=GARCH_MIN_TICKS and nA>0 and nB>0)
                ca=0;cb=0
                for i in range(MEWMA_DIM):
                    if c.dthr[i]>0.0 and dv[i]>c.dthr[i]:
                        if i<3:ca=1
                        else:cb=1
                tx=ca|(cb<<1)
                if tg:c.n_trig+=1
                cr=1
                for i in range(MEWMA_DIM):
                    if c.dthr[i]<=0.0:cr=0;break
                ci=-1;cw=0.0;cs=0.0
                if cr:
                    for i in range(MEWMA_DIM):
                        if dv[i]>cs*c.dthr[i]:cs=dv[i]/c.dthr[i];ci=i;cw=dv[i]
                else:
                    for i in range(MEWMA_DIM):
                        if dv[i]>cw:cw=dv[i];ci=i
                sgc=ci>=0 and (c.dthr[ci]<=0.0 or cw>c.dthr[ci])
                cdep=-1;cz=0.0
                if ci>=0:
                    for j in range(NDEP):
                        az=dep_z[ci*NDEP+j]
                        if az<0.0:az=-az
                        if az>cz:cz=az;cdep=j
                dk=ci*NDEP+cdep if (tg and sgc and cdep>=0) else -1
                if dk>=0:
                    c.n_attr+=1
                    if dk==c.key:
                        c.run+=1;c.st|=tx
                    else:
                        c.key=dk;c.run=1;c.st=tx
                else:
                    c.key=-1;c.run=0;c.st=0
                if c.cn>0:c.cn-=1
                if c.run>=c.latch and c.cn==0:
                    c.n_dec+=1
                    n2=DEP_LIST[cdep];c2=dec_rep[cdep];k2=2*NDEP+cdep;q2=dep_n[k2]
                    if ci not in SCALE_DIM:
                        a2='alert';d2=c2;m2=0;r2=0.0
                    elif c2<1 or q2<DEP_MIN or d_rps[cdep]<MIN_REQ:
                        a2='nodata';d2=c2;m2=0;r2=0.0
                    else:
                        r2=dep_mu[k2]/c2
                        d2=int(math.ceil(d_rps[cdep]/r2)) if r2>0.0 else c2
                        m2=MAX_REP.get(n2,10)
                        if n2 in PG_POOL:
                            u2=0
                            for n3 in PG_POOL:
                                j3=DEP_IDX[n3]
                                if j3!=cdep:u2+=dec_rep[j3]*PG_POOL[n3]
                            b2=(PG_BUDGET-u2)//PG_POOL[n2]
                            if b2<m2:m2=b2
                        w2=d2
                        if d2>m2:d2=m2
                        if d2<1:d2=1
                        if w2>m2:a2='capped'
                        elif d2<c2 and wi!=2:d2=c2;a2='hold_dim'
                        elif d2!=c2 and abs(d2-c2)/c2>=DEC_BAND:a2='scale_out' if d2>c2 else 'scale_in'
                        else:a2='hold'
                    if c.st==2:a2='alert_sdf';d2=c2
                    elif c.st==3:a2='alert_ddos';d2=c2
                    if a2 in ('scale_out','scale_in','capped'):c.cn=c.cool;c.n_act+=1
                    print(f'  EVALDEC c={c.id} dep={n2} dim={DIMS[ci]} rule={"rat" if cr else "raw"} cur={c2} desired={d2} max={m2} capr={r2:.2f} rps={d_rps[cdep]:.1f} run={c.run} act={a2} tax={c.st} dcc={c.router.out} ucl={uc:.4f} SHADOW')
                    if DEC_CSV:
                        if dec_fh is None:
                            dec_fh=open(DEC_CSV,'a',buffering=1)
                            dec_fh.write('ts,cand,dep,dim,d,thr,cur,desired,max,capr,rps,run,act,tax,dcc\n')
                        dec_fh.write(f'{int(time.time())},{c.id},{n2},{DIMS[ci]},{cw:.3f},{c.dthr[ci]:.3f},{c2},{d2},{m2},{r2:.2f},{d_rps[cdep]:.1f},{c.run},{a2},{c.st},{c.router.out}\n')
            if epoch%EVAL_SUM==0:
                for c in CANDS:
                    print(f'  EVALSUM c={c.id} ucl={c.ucl:.4f} latch={c.latch} trig={c.n_trig} attr={c.n_attr} dec={c.n_dec} act={c.n_act} epoch={epoch}')
        if dec_cool>0:dec_cool-=1
        dkey=wi*NDEP+wdep if (trig and sg and wdep>=0) else -1
        if dkey>=0 and dkey==dec_key:
            dec_run+=1;dec_st|=tax
        else:
            dec_key=dkey;dec_run=1 if dkey>=0 else 0;dec_st=tax if dkey>=0 else 0
        if dec_run>=DEC_LATCH and dec_cool==0:
            nm=DEP_LIST[wdep];cur=dec_rep[wdep];kk=2*NDEP+wdep;nn=dep_n[kk]
            if wi not in SCALE_DIM:
                act='alert';des=cur;mx=0;capr=0.0
            elif cur<1 or nn<DEP_MIN or d_rps[wdep]<MIN_REQ:
                act='nodata';des=cur;mx=0;capr=0.0
            else:
                capr=dep_mu[kk]/cur
                des=int(math.ceil(d_rps[wdep]/capr)) if capr>0.0 else cur
                mx=MAX_REP.get(nm,10)
                if nm in PG_POOL:
                    used=0
                    for nm2 in PG_POOL:
                        j2=DEP_IDX[nm2]
                        if j2!=wdep:used+=dec_rep[j2]*PG_POOL[nm2]
                    m2=(PG_BUDGET-used)//PG_POOL[nm]
                    if m2<mx:mx=m2
                want=des
                if des>mx:des=mx
                if des<1:des=1
                if want>mx:act='capped'
                elif des<cur and wi!=2:des=cur;act='hold_dim'
                elif des!=cur and abs(des-cur)/cur>=DEC_BAND:act='scale_out' if des>cur else 'scale_in'
                else:act='hold'
            if dec_st==2:act='alert_sdf';des=cur
            elif dec_st==3:act='alert_ddos';des=cur
            if act in ('scale_out','scale_in','capped'):dec_cool=DEC_COOL
            if FRZ_ACT and act in ('scale_out','scale_in'):dec_act[0]=1
            print(f'  DECIDE dep={nm} dim={DIMS[wi]} cur={cur} desired={des} max={mx} capr={capr:.2f} rps={d_rps[wdep]:.1f} run={dec_run} act={act} tax={dec_st} dcc={router.out} DRYRUN')
            if DEC_CSV:
                if dec_fh is None:
                    dec_fh=open(DEC_CSV,'a',buffering=1)
                    dec_fh.write('ts,cand,dep,dim,d,thr,cur,desired,max,capr,rps,run,act,tax,dcc\n')
                dec_fh.write(f'{int(time.time())},A,{nm},{DIMS[wi]},{wd:.3f},{DTHR[wi]:.3f},{cur},{des},{mx},{capr:.2f},{d_rps[wdep]:.1f},{dec_run},{act},{dec_st},{router.out}\n')
        if FRZ_ON:
            if frz[0]:
                frz[2]+=1
                frz[1]=frz[1]+1 if not trig else 0
                if frz[1]>=FRZ_REL:
                    print(f'  FRZ nha ly_do=calm calm={frz[1]} tuoi={frz[2]}')
                    frz[0]=0;frz[1]=0;frz[2]=0
                elif dec_act[0]:
                    print(f'  FRZ nha ly_do=tu_hanh_dong tuoi={frz[2]}')
                    frz[0]=0;frz[1]=0;frz[2]=0
                elif frz[2]>=FRZ_MAX:
                    print(f'  REBASELINE tuoi={frz[2]} tran={FRZ_MAX} trig={trig} t2={t2:.4f} ucl={ucl:.4f}')
                    frz[0]=0;frz[1]=0;frz[2]=0
            elif trig and mewma.count>=MEWMA_WINDOW:
                frz[0]=1;frz[1]=0;frz[2]=1
                print(f'  FRZ bat t2={t2:.4f} ucl={ucl:.4f}')
        dec_act[0]=0
        print(f'  FRZstate on={frz[0]} calm={frz[1]} tuoi={frz[2]} n={mewma.count} W={MEWMA_WINDOW}')
        print('  H  A='+' '.join(f'{v:.4g}' for v in blkA.h)+' B='+' '.join(f'{v:.4g}' for v in blkB.h))
        for d in dep_cpu:
            _j=DEP_IDX.get(d,-1)
            print(f'  {d} cpu={dep_cpu[d]:.6f} ram={dep_ram.get(d,0.0):.0f} nin={dep_nin.get(d,0.0):.0f} nout={dep_nout.get(d,0.0):.0f}'+('' if _j<0 else ' rps=%.1f err=%.4f z='%(d_rps[_j],d_err[_j]/(d_rps[_j]+EPS))+'/'.join(f'{dep_z[i*NDEP+_j]:+.4f}' for i in range(MEWMA_DIM))))

@asynccontextmanager
async def lifespan(app):
    t1=asyncio.create_task(flush_loop())
    t2=asyncio.create_task(fft_slow())
    yield
    t1.cancel();t2.cancel()

app=FastAPI(lifespan=lifespan)

@app.post('/')
async def ingest(request:Request):
    global ring_write
    body=await request.body()
    recv=time.perf_counter_ns()
    raw=bytes(cramjam.snappy.decompress_raw(body))
    L=_lbl
    p=0;ln=len(raw)
    while p<ln:
        tag,p=read_varint(raw,p)
        f=tag>>3;w=tag&7
        if f==1 and w==2:
            l,p=read_varint(raw,p)
            value,ts=parse_ts(raw[p:p+l],L);p+=l
            name=L[0]
            idx=CADVISOR.get(name)
            if idx is not None:
                pod=L[1]
                if pod and L[3]==NS_FILTER:
                    sub=L[2] if idx<NIN else L[4]
                    if sub:
                        sh=pod_shards[hash(pod)%SHARDS]
                        e=sh.get(pod)
                        if e is None:e=sh[pod]=Pod(pod,epoch)
                        e.seen=epoch
                        upd(e.m[idx],sub,value,ts)
            elif name==RPS_METRIC:supd(hub,(L[5],L[6],L[7],L[8],L[12]),value,ts,epoch)
            elif name==LAT_METRIC and L[9]:supd(hist,(L[5],L[6],L[7],L[8],L[9],L[12]),value,ts,epoch)
            elif name==ENVOY_METRIC:supd(env,(L[5],L[10],L[11]),value,ts,epoch)
        else:p=skip(raw,p,w)
    k=ring_write%RING_CAP
    ring[k]=recv
    proc_ring[k]=time.perf_counter_ns()-recv
    ring_write+=1
    return Response(status_code=200)

if __name__=='__main__':
    uvicorn.run(app,host=LISTEN_HOST,port=LISTEN_PORT,log_level='warning')