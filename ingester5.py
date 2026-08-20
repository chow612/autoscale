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
NS_FILTER='bank-of-anthos'
STALE_CYCLES=5
EPS=1e-9
EPS_L=1e-4
MEWMA_DIM=6
MEWMA_LAMBDA=0.2
MEWMA_WINDOW=120
MEWMA_ALPHA=0.01
MEWMA_MIN_SAMPLES=max(MEWMA_DIM+1,30)
MEWMA_RIDGE=1e-6
RIDGE_EYE=np.eye(MEWMA_DIM)*MEWMA_RIDGE
MIN_REQ=5
MIN_BYTES=1024
SIGMA_FLOOR2=array.array('d',[v*v for v in (0.01,1.0e5,0.5,0.05,0.05,0.05)])
FFT_N=672
FFT_DT=900.0
FFT_INTERVAL_SEC=900.0
K_MAX=8
Z_MAD=5.0
FFT_COV_MIN=FFT_N*0.5
GARCH_OMEGA=1e-6
GARCH_ALPHA=0.08
GARCH_BETA=0.90
H_FLOOR=0.1
DCC_A=0.02
DCC_B=0.96
DCC_TCAP=300
DCC_RIDGE=1e-6
DCC_CONST=1.0-DCC_A-DCC_B
CHI2_3=11.3449
GARCH_MIN_TICKS=60
LATCH_TICKS=5
HIT_MASK=3
POPCNT=bytes(bin(i).count('1') for i in range(256))
STATES=('Healthy','Flash Crowd','Silent Dependency Failure','L7 DDoS')
MIMIR_QUERY_URL=os.environ.get('MIMIR_QUERY_URL','http://mimir-nginx.monitoring.svc.cluster.local:80/prometheus/api/v1/query_range')
CPU,RAM,NIN,NOUT=0,1,2,3
DIMS=('cpu','ram','rps','err','asym','p99')
FFT_DIM_IDX=(0,2,4,5)
FFT_DIM_NAMES=('cpu','rps','asym','p99')
FFT_LOG=(False,False,True,True)
FFT_SCALE=(FFT_DT,FFT_DT,1.0,1.0)
FFT_EVAL_SCALE=(FLUSH_SEC/FFT_DT,FLUSH_SEC/FFT_DT,1.0,1.0)
CADVISOR={'container_cpu_usage_seconds_total':CPU,
          'container_memory_working_set_bytes':RAM,
          'container_network_receive_bytes_total':NIN,
          'container_network_transmit_bytes_total':NOUT}
RPS_METRIC='hubble_http_requests_total'
LAT_METRIC='hubble_http_request_duration_seconds_bucket'
ENVOY_METRIC='envoy_cluster_external_upstream_rq'
LABELS=('__name__','pod','container','namespace','interface','instance','method','status','reporter','le','envoy_response_code','envoy_cluster_name')
LABEL_IDX={n:i for i,n in enumerate(LABELS)}
FFT_PROMQL=(
    f'sum(rate(container_cpu_usage_seconds_total{{namespace="{NS_FILTER}"}}[15m]))',
    f'sum(rate({RPS_METRIC}[15m]))',
    f'sum(rate(container_network_receive_bytes_total{{namespace="{NS_FILTER}"}}[15m]))/(sum(rate(container_network_transmit_bytes_total{{namespace="{NS_FILTER}"}}[15m]))+{EPS})',
    f'histogram_quantile(0.99,sum(rate({LAT_METRIC}[15m])) by (le))')

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
    for i in range(12):out[i]=None
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
    __slots__=('m','prev','dep','seen')
    def __init__(self,pod,epoch):
        self.m=[{},{},{},{}]
        self.prev=[None,None,None,None]
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

def p99_from_buckets(bd):
    if not bd:return 0.0
    items=sorted(((float('inf') if k=='+Inf' else float(k)),v) for k,v in bd.items())
    total=items[-1][1]
    if total<=0:return 0.0
    rank=0.99*total
    low_le=0.0;low_cum=0.0
    for le,cum in items:
        if cum>=rank:
            if le==float('inf'):return low_le
            if cum==low_cum:return le
            return low_le+(rank-low_cum)/(cum-low_cum)*(le-low_le)
        low_le=le;low_cum=cum
    return low_le

class SlidingWelford:
    __slots__=('buf','n','mean','M2')
    def __init__(self):
        self.buf=deque();self.n=0;self.mean=0.0;self.M2=0.0
    def add(self,x):
        self.n+=1
        d=x-self.mean
        self.mean+=d/self.n
        self.M2+=d*(x-self.mean)
        self.buf.append(x)
        if len(self.buf)>MEWMA_WINDOW:self._rm(self.buf.popleft())
    def _rm(self,x):
        if self.n<=1:
            self.n=0;self.mean=0.0;self.M2=0.0;return
        om=self.mean
        self.n-=1
        self.mean=(om*(self.n+1)-x)/self.n
        self.M2-=(x-om)*(x-self.mean)
    def std(self):
        if self.n<2:return 0.0
        v=self.M2/self.n
        return v**0.5 if v>0 else 0.0

UCL_CACHE={}
def ucl_for(n):
    u=UCL_CACHE.get(n)
    if u is None:
        d=n-MEWMA_DIM
        u=(MEWMA_DIM*(n-1)/d)*f_dist.ppf(1-MEWMA_ALPHA,MEWMA_DIM,d)
        UCL_CACHE[n]=u
    return u

class StateMEWMA:
    __slots__=('w','ring','pos','count','n','mean','C','z','x','t','_S','_z','_nm')
    def __init__(self):
        self.w=[SlidingWelford() for _ in range(MEWMA_DIM)]
        self.ring=array.array('d',[0.0]*(MEWMA_DIM*MEWMA_WINDOW))
        self.pos=0;self.count=0;self.n=0
        self.mean=array.array('d',[0.0]*MEWMA_DIM)
        self.C=[array.array('d',[0.0]*MEWMA_DIM) for _ in range(MEWMA_DIM)]
        self.z=array.array('d',[0.0]*MEWMA_DIM)
        self.x=array.array('d',[0.0]*MEWMA_DIM)
        self.t=0
        self._S=np.empty((MEWMA_DIM,MEWMA_DIM))
        self._z=np.empty(MEWMA_DIM)
        self._nm=array.array('d',[0.0]*MEWMA_DIM)
    def _add_row(self,ring,base):
        n=self.n+1
        om=self.mean;nm=self._nm
        for i in range(MEWMA_DIM):nm[i]=om[i]+(ring[base+i]-om[i])/n
        for i in range(MEWMA_DIM):
            di=ring[base+i]-om[i];row=self.C[i]
            for j in range(MEWMA_DIM):row[j]+=di*(ring[base+j]-nm[j])
        for i in range(MEWMA_DIM):om[i]=nm[i]
        self.n=n
    def _sub_row(self,ring,base):
        if self.n<=1:
            self.n=0
            m=self.mean
            for i in range(MEWMA_DIM):m[i]=0.0
            for row in self.C:
                for i in range(MEWMA_DIM):row[i]=0.0
            return
        om=self.mean;n=self.n-1;nm=self._nm
        for i in range(MEWMA_DIM):nm[i]=(om[i]*(n+1)-ring[base+i])/n
        for i in range(MEWMA_DIM):
            di=ring[base+i]-om[i];row=self.C[i]
            for j in range(MEWMA_DIM):row[j]-=di*(ring[base+j]-nm[j])
        for i in range(MEWMA_DIM):om[i]=nm[i]
        self.n=n
    def update(self,e,valid):
        nv=0
        for v in valid:
            if v:nv+=1
        if nv<2:return 0.0,float('inf'),False,0.0
        x=self.x
        for i in range(MEWMA_DIM):
            if valid[i]:
                wi=self.w[i];sd=wi.std()
                x[i]=(e[i]-wi.mean)/math.sqrt(sd*sd+SIGMA_FLOOR2[i])
                wi.add(e[i])
            else:
                x[i]=0.0
        self.t+=1
        lam=MEWMA_LAMBDA;inv=1-lam
        z=self.z
        for i in range(MEWMA_DIM):z[i]=lam*x[i]+inv*z[i]
        ring=self.ring;base=self.pos*MEWMA_DIM
        if self.count==MEWMA_WINDOW:self._sub_row(ring,base)
        for i in range(MEWMA_DIM):ring[base+i]=x[i]
        self._add_row(ring,base)
        if self.count<MEWMA_WINDOW:self.count+=1
        self.pos=self.pos+1 if self.pos+1<MEWMA_WINDOW else 0
        if self.n<MEWMA_MIN_SAMPLES:return 0.0,float('inf'),False,0.0
        S=self._S
        S[:]=self.C
        S/=(self.n-1)
        S+=RIDGE_EYE
        S*=(lam/(2-lam))*(1-inv**(2*self.t))
        try:si=np.linalg.inv(S)
        except np.linalg.LinAlgError:return 0.0,float('inf'),False,0.0
        zv=self._z
        zv[:]=z
        t2=float(zv@si@zv)
        ev=np.linalg.eigvalsh(S)
        cond=float(ev[-1]/ev[0]) if ev[0]>0 else float('inf')
        return t2,ucl_for(self.n),t2>ucl_for(self.n),cond

class BlockGARCH:
    __slots__=('h','u','up','xp','Qb','Q','Qn','t')
    def __init__(self):
        self.h=array.array('d',[1.0]*3)
        self.u=array.array('d',[0.0]*3)
        self.up=array.array('d',[0.0]*3)
        self.xp=array.array('d',[0.0]*3)
        self.Qb=array.array('d',[1.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,1.0])
        self.Q=array.array('d',[1.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,1.0])
        self.Qn=array.array('d',[0.0]*9)
        self.t=0
    def update(self,x0,x1,x2,fz):
        h=self.h;u=self.u;up=self.up;xp=self.xp;Q=self.Q
        if not fz:
            h[0]=GARCH_OMEGA+GARCH_ALPHA*xp[0]*xp[0]+GARCH_BETA*h[0]
            h[1]=GARCH_OMEGA+GARCH_ALPHA*xp[1]*xp[1]+GARCH_BETA*h[1]
            h[2]=GARCH_OMEGA+GARCH_ALPHA*xp[2]*xp[2]+GARCH_BETA*h[2]
        u[0]=x0/math.sqrt(h[0] if h[0]>H_FLOOR else H_FLOOR)
        u[1]=x1/math.sqrt(h[1] if h[1]>H_FLOOR else H_FLOOR)
        u[2]=x2/math.sqrt(h[2] if h[2]>H_FLOOR else H_FLOOR)
        if not fz:
            xp[0]=x0;xp[1]=x1;xp[2]=x2
            self.t+=1
            w=1.0/(self.t if self.t<DCC_TCAP else DCC_TCAP)
            iw=1.0-w
            Qb=self.Qb;Qn=self.Qn
            for i in range(3):
                ui=u[i];b=i*3
                for j in range(3):Qb[b+j]=iw*Qb[b+j]+w*ui*u[j]
            for i in range(3):
                pi=up[i];b=i*3
                for j in range(3):Qn[b+j]=DCC_CONST*Qb[b+j]+DCC_A*pi*up[j]+DCC_B*Q[b+j]
            for i in range(9):Q[i]=Qn[i]
            up[0]=u[0];up[1]=u[1];up[2]=u[2]
        d0=math.sqrt(Q[0]) if Q[0]>1e-12 else 1e-6
        d1=math.sqrt(Q[4]) if Q[4]>1e-12 else 1e-6
        d2=math.sqrt(Q[8]) if Q[8]>1e-12 else 1e-6
        r01=Q[1]/(d0*d1);r02=Q[2]/(d0*d2);r12=Q[5]/(d1*d2)
        r=1.0+DCC_RIDGE
        c00=r*r-r12*r12
        c01=r02*r12-r01*r
        c02=r01*r12-r02*r
        det=r*c00+r01*c01+r02*c02
        if -1e-14<det<1e-14:det=1e-14
        c11=r*r-r02*r02
        c12=r02*r01-r*r12
        c22=r*r-r01*r01
        u0=u[0];u1=u[1];u2=u[2]
        return (u0*u0*c00+u1*u1*c11+u2*u2*c22+2.0*(u0*u1*c01+u0*u2*c02+u1*u2*c12))/det

class Router:
    __slots__=('ma','mb','la','lb','out')
    def __init__(self):
        self.ma=0;self.mb=0;self.la=0;self.lb=0;self.out=0
    def step(self,xa,xb,ready):
        if not ready:
            self.ma=0;self.mb=0;self.la=0;self.lb=0;self.out=0
            return 0
        ma=((self.ma<<1)|(1 if xa>=CHI2_3 else 0))&HIT_MASK
        mb=((self.mb<<1)|(1 if xb>=CHI2_3 else 0))&HIT_MASK
        self.ma=ma;self.mb=mb
        if POPCNT[ma]>=2:self.la=LATCH_TICKS
        elif self.la>0:self.la-=1
        if POPCNT[mb]>=2:self.lb=LATCH_TICKS
        elif self.lb>0:self.lb-=1
        self.out=(1 if self.la>0 else 0)|(2 if self.lb>0 else 0)
        return self.out

class Harmonics:
    __slots__=('mean','a','f','p','k')
    def __init__(self):
        self.a=array.array('d',[0.0]*K_MAX)
        self.f=array.array('d',[0.0]*K_MAX)
        self.p=array.array('d',[0.0]*K_MAX)
        self.mean=0.0
        self.k=0
    def at(self,tau):
        s=self.mean;a=self.a;f=self.f;p=self.p
        for i in range(self.k):s+=a[i]*math.cos(6.283185307179586*f[i]*tau+p[i])
        return s

class HarmonicSet:
    __slots__=('active','shadow','t0a','t0s')
    def __init__(self):
        n=len(FFT_DIM_IDX)
        self.active=[Harmonics() for _ in range(n)]
        self.shadow=[Harmonics() for _ in range(n)]
        self.t0a=array.array('d',[0.0]*n)
        self.t0s=array.array('d',[0.0]*n)
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

def fft_fit(y):
    ya=np.frombuffer(y,dtype=np.float64)
    n=ya.shape[0]
    m=float(ya.mean())
    yc=ya-m
    Y=np.fft.rfft(yc,n)
    P=np.abs(Y)
    tail=P[1:]
    med=float(np.median(tail))
    mad=float(np.median(np.abs(tail-med)))
    idx=np.nonzero(tail>med+Z_MAD*1.4826*mad)[0]+1
    if idx.size>K_MAX:idx=np.sort(idx[np.argsort(P[idx])[::-1][:K_MAX]])
    A=2.0*np.abs(Y[idx])/n
    F=idx/(n*FFT_DT)
    PH=np.arctan2(Y[idx].imag,Y[idx].real)
    var=float(yc.var())
    return m,A,F,PH,(100.0*float((A*A).sum()/2.0)/var if var>0 else 0.0)

def _http_get(url):
    try:
        with urllib.request.urlopen(url,timeout=30) as r:
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
        if 0<=i<n:out[i]=float(v);got[i]=1
    cov=sum(got)
    last=0.0
    for i in range(n):
        if got[i]:last=out[i]
        else:out[i]=last
    return out,cov

async def fft_slow():
    while True:
        end=time.time()
        start=end-FFT_N*FFT_DT
        start-=start%FFT_DT
        qend=start+(FFT_N-1)*FFT_DT
        for fi in range(len(FFT_DIM_IDX)):
            try:
                y,cov=to_series(await query_range(FFT_PROMQL[fi],start,qend),start,FFT_N)
                if cov<FFT_COV_MIN:
                    print(f'FFT_SLOW skip dim={FFT_DIM_NAMES[fi]} coverage={cov}/{FFT_N}')
                    continue
                if FFT_LOG[fi]:
                    for i in range(FFT_N):y[i]=math.log(y[i]+EPS_L)
                sc=FFT_SCALE[fi]
                if sc!=1.0:
                    for i in range(FFT_N):y[i]*=sc
                m,A,F,PH,ve=fft_fit(y)
                h=hset.shadow[fi];k=len(A)
                for i in range(k):h.a[i]=float(A[i]);h.f[i]=float(F[i]);h.p[i]=float(PH[i])
                h.mean=m;h.k=k
                hset.t0s[fi]=start
                hset.swap(fi)
                print(f'FFT_SLOW dim={FFT_DIM_NAMES[fi]} bins={k} var_explained={ve:.1f}%')
            except Exception as ex:
                print(f'FFT_SLOW error dim={FFT_DIM_NAMES[fi]}:',ex)
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
_lbl=[None]*12
ram_prev=0.0
ram_init=False

def cdelta(cur,prev):
    if prev is None:return 0.0
    d=cur-prev
    return d if d>=0.0 else 0.0

def series_delta(cur,prev,idx):
    tot=0.0;err=0.0
    for k,e in cur.items():
        p=prev.get(k)
        d=e[0]-p if p is not None else 0.0
        if d<0.0:d=0.0
        tot+=d
        c=k[idx]
        if c and c[0] in '45':err+=d
        prev[k]=e[0]
    return tot,err

async def flush_loop():
    global ring_read,epoch,ram_prev,ram_init
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
        dep_cpu={};dep_ram={};dep_nin={};dep_nout={}
        for sh in pod_shards:
            for pod in [p for p,e in sh.items() if epoch-e.seen>STALE_CYCLES]:del sh[pod]
            for e in sh.values():
                d=e.dep
                tot=msum(e.m[CPU])
                dep_cpu[d]=dep_cpu.get(d,0.0)+cdelta(tot,e.prev[CPU]);e.prev[CPU]=tot
                tot=msum(e.m[NIN])
                dep_nin[d]=dep_nin.get(d,0.0)+cdelta(tot,e.prev[NIN]);e.prev[NIN]=tot
                tot=msum(e.m[NOUT])
                dep_nout[d]=dep_nout.get(d,0.0)+cdelta(tot,e.prev[NOUT]);e.prev[NOUT]=tot
                dep_ram[d]=dep_ram.get(d,0.0)+msum(e.m[RAM])
        sweep(hub,hub_prev,epoch);sweep(hist,hist_prev,epoch);sweep(env,env_prev,epoch)
        rps,err=series_delta(hub,hub_prev,2)
        err_rate=err/(rps+EPS)
        ing_tot,ing_err=series_delta(env,env_prev,1)
        ing_rate=ing_err/(ing_tot+EPS)
        bd={}
        for k,e in hist.items():
            p=hist_prev.get(k)
            d=e[0]-p if p is not None else 0.0
            if d<0.0:d=0.0
            le=k[4]
            bd[le]=bd.get(le,0.0)+d
            hist_prev[k]=e[0]
        p99=p99_from_buckets(bd)
        p99_total=bd.get('+Inf',0.0)
        tcpu=0.0;tram=0.0;tnin=0.0;tnout=0.0
        for d in dep_cpu:
            tcpu+=dep_cpu[d];tram+=dep_ram.get(d,0.0)
            tnin+=dep_nin.get(d,0.0);tnout+=dep_nout.get(d,0.0)
        asym=tnin/(tnout+EPS)
        rd=(tram-ram_prev) if ram_init else 0.0
        ram_prev=tram;ram_init=True
        valid_buf[3]=1 if rps>=MIN_REQ else 0
        valid_buf[4]=1 if tnout>=MIN_BYTES else 0
        valid_buf[5]=1 if p99_total>=MIN_REQ else 0
        y=y_buf
        y[0]=tcpu;y[1]=rd;y[2]=rps
        y[3]=math.log(err_rate+EPS_L) if valid_buf[3] else last_valid[3]
        y[4]=math.log(asym+EPS_L) if valid_buf[4] else last_valid[4]
        y[5]=math.log(p99+EPS_L) if valid_buf[5] else last_valid[5]
        for i in range(MEWMA_DIM):
            if valid_buf[i]:last_valid[i]=y[i]
            else:y[i]=last_valid[i]
        now_wall=time.time()
        res=e_buf;yh=yhat_buf
        for fi in range(len(FFT_DIM_IDX)):
            di=FFT_DIM_IDX[fi]
            yh[di]=hset.active[fi].at(now_wall-hset.t0a[fi])*FFT_EVAL_SCALE[fi]
            res[di]=y[di]-yh[di]
        yh[1]=0.0;res[1]=y[1]
        yh[3]=0.0;res[3]=y[3]
        st=router.out
        fzA=(st&1)!=0;fzB=(st&2)!=0
        t2,u,trig,cond=mewma.update(res,valid_buf)
        nv=0
        for i in range(MEWMA_DIM):
            if valid_buf[i]:nv+=1
        if nv>=2:
            mx=mewma.x
            xiA=blkA.update(mx[0],mx[1],mx[2],fzA)
            xiB=blkB.update(mx[3],mx[4],mx[5],fzB)
            st=router.step(xiA,xiB,blkA.t>=GARCH_MIN_TICKS)
        else:
            xiA=xiB=0.0
        print(f'--- window={FLUSH_SEC:.0f}s packets={cnt} proc_us(avg={psum*f/1e3:.1f},max={pmax/1e3:.1f}) wait_ms(avg={lsum*f/1e6:.3f},max={lmax/1e6:.3f}) ---')
        print(f'STATE cpu={tcpu:.6f} ram={rd:+.0f} rps={rps:.2f} err={err_rate:.4f} asym={asym:.4f} p99={p99:.4f}')
        print(f'MEWMA T2={t2:.4f} ucl={u:.4f} trig={trig} n={mewma.n} cond={cond:.3e} ingress_err={ing_rate:.4f}')
        print(f'DCC xiA={xiA:.4f} xiB={xiB:.4f} thr={CHI2_3} fz={int(fzA)}{int(fzB)} t={blkA.t} STATE={STATES[st]}')
        print('  FFT '+' '.join(f'{DIMS[i]}={y[i]:.4f}->{yh[i]:.4f}->{res[i]:+.4f}' for i in range(MEWMA_DIM)))
        print('  Z '+' '.join(f'{DIMS[i]}={mewma.x[i]:+.3f}/{mewma.z[i]:+.3f}' for i in range(MEWMA_DIM)))
        print('  SD '+' '.join(f'{DIMS[i]}={mewma.w[i].std():.6g}(m={mewma.w[i].mean:.6g})' for i in range(MEWMA_DIM)))
        for d in dep_cpu:
            print(f'  {d} cpu={dep_cpu[d]:.6f} ram={dep_ram.get(d,0.0):.0f} nin={dep_nin.get(d,0.0):.0f} nout={dep_nout.get(d,0.0):.0f}')

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
            elif name==RPS_METRIC:supd(hub,(L[5],L[6],L[7],L[8]),value,ts,epoch)
            elif name==LAT_METRIC and L[9]:supd(hist,(L[5],L[6],L[7],L[8],L[9]),value,ts,epoch)
            elif name==ENVOY_METRIC:supd(env,(L[5],L[10],L[11]),value,ts,epoch)
        else:p=skip(raw,p,w)
    k=ring_write%RING_CAP
    ring[k]=recv
    proc_ring[k]=time.perf_counter_ns()-recv
    ring_write+=1
    return Response(status_code=200)

if __name__=='__main__':
    uvicorn.run(app,host=LISTEN_HOST,port=LISTEN_PORT,log_level='warning')