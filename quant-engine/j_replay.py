import sys,os,array,importlib.util,hashlib
import numpy as np

DIMS=('cpu','ram','rps','err','asym','slow')
DEPS=('balancereader','contacts','frontend','ledgerwriter','transactionhistory','userservice')
DIDX={n:i for i,n in enumerate(DEPS)}
D6=6
ND=6
DZ=D6*ND
ENVK=('JANUS_W','JANUS_UCL','JANUS_DTHR','JANUS_TR_MAX','JANUS_LATCH','JANUS_COOL',
      'JANUS_BAND','JANUS_FRZ','JANUS_FRZ_REL','JANUS_FRZ_MAX','JANUS_FRZ_DEP',
      'JANUS_FRZ_ACT','JANUS_CALIB_TICKS','JANUS_DCALIB_TICKS','JANUS_CALIB_SPAN',
      'JANUS_DCALIB_SPAN','JANUS_EVAL_FILE','JANUS_EVAL_SUM','JANUS_THR_A','JANUS_THR_B',
      'JANUS_DECIDE_CSV','JANUS_FFT_KMIN','JANUS_FFT_TREND_MAX','JANUS_UNI_K','JANUS_UNI_K2',
      'JANUS_DT_MAX','JANUS_DT_MAX_POD')
SRC=os.path.join(os.path.dirname(os.path.abspath(__file__)),'ingester6.py')
_lc=[0]


def load(**env):
    for k in ENVK:os.environ[k]=''
    for k,v in env.items():os.environ[k]='' if v is None else str(v)
    _lc[0]+=1
    sp=importlib.util.spec_from_file_location('ing_r%d'%_lc[0],SRC)
    m=importlib.util.module_from_spec(sp)
    sp.loader.exec_module(m)
    return m


def q(a,p):
    b=sorted(a)
    if not b:return float('nan')
    return b[int(round(p*0.01*(len(b)-1)))]


def mad(a):
    m=q(a,50)
    return q([abs(v-m) for v in a],50)


def _v(t):
    return t[t.index('=')+1:]


class Log:
    __slots__=('n','mask','seen','res','dd','sd','mu','xx','zz','dz','live','trig','frz','t2','ucl',
               'tr','dw','cnt','pp','nl','calm','tuoi')

    def __init__(s):
        for a in ('res','dd','sd','mu','xx','zz','dz','t2','ucl','tr','dw'):
            setattr(s,a,array.array('d'))
        for a in ('live','trig','frz'):
            setattr(s,a,bytearray())
        for a in ('cnt','pp','nl','calm','tuoi'):
            setattr(s,a,array.array('i'))
        s.n=0;s.mask=0;s.seen=0


MASK_FULL=127
MASK_LOOSE=7
BITNM=('window','MEWMA','FFT','Z','SD','D','FRZstate')

_LOG_ARR_D=('res','dd','sd','mu','xx','zz','dz','t2','ucl','tr','dw')
_LOG_ARR_B=('live','trig','frz')
_LOG_ARR_I=('cnt','pp','nl','calm','tuoi')


def _cache_path(path,limit,loose):
    st=os.stat(path)
    key='%d_%d_%d_%d'%(st.st_size,int(st.st_mtime),limit,loose)
    h=hashlib.md5(key.encode()).hexdigest()[:12]
    return path+'.jrc_%s.npz'%h


def _save_cache(cp,L):
    kw={}
    for a in _LOG_ARR_D+_LOG_ARR_I:
        kw[a]=np.frombuffer(getattr(L,a),dtype=(np.float64 if a in _LOG_ARR_D else np.int32))
    for a in _LOG_ARR_B:
        kw[a]=np.frombuffer(getattr(L,a),dtype=np.uint8)
    kw['n']=np.array([L.n]);kw['mask']=np.array([L.mask]);kw['seen']=np.array([L.seen])
    tmp=cp+'.tmp.npz'
    np.savez(tmp,**kw)
    os.replace(tmp,cp)


def _load_cache(cp):
    z=np.load(cp)
    L=Log()
    for a in _LOG_ARR_D:
        setattr(L,a,array.array('d',z[a].tobytes()))
    for a in _LOG_ARR_I:
        setattr(L,a,array.array('i',z[a].astype(np.int32).tobytes()))
    for a in _LOG_ARR_B:
        setattr(L,a,bytearray(z[a].tobytes()))
    L.n=int(z['n'][0]);L.mask=int(z['mask'][0]);L.seen=int(z['seen'][0])
    return L


def parse(path,limit=0,loose=0,use_cache=True):
    if use_cache:
        cp=_cache_path(path,limit,loose)
        if os.path.exists(cp):
            try:
                return _load_cache(cp)
            except Exception:
                pass
        L=_parse_raw(path,limit,loose)
        try:
            _save_cache(cp,L)
        except Exception:
            pass
        return L
    return _parse_raw(path,limit,loose)


def _parse_raw(path,limit=0,loose=0):
    L=Log()
    need=MASK_LOOSE if loose else MASK_FULL
    L.mask=need;L.seen=0
    r=array.array('d',[0.0]*D6);dv=array.array('d',[0.0]*D6)
    sv=array.array('d',[0.0]*D6);mv=array.array('d',[0.0]*D6)
    xv=array.array('d',[0.0]*D6);zv=array.array('d',[0.0]*D6)
    lv=bytearray(D6);dz=array.array('d',[0.0]*DZ)
    st={'have':0,'dw':0.0,'t2':0.0,'ucl':0.0,'tr':0.0,'trig':0,'cnt':0,'pp':0,
        'nl':0,'frz':0,'calm':0,'tuoi':0}

    def flush():
        L.res.extend(r);L.dd.extend(dv);L.sd.extend(sv);L.mu.extend(mv)
        L.xx.extend(xv);L.zz.extend(zv);L.dz.extend(dz);L.live.extend(lv)
        L.trig.append(st['trig']);L.frz.append(st['frz'])
        L.calm.append(st['calm']);L.tuoi.append(st['tuoi'])
        L.t2.append(st['t2']);L.ucl.append(st['ucl'])
        L.tr.append(st['tr']);L.dw.append(st['dw'])
        L.cnt.append(st['cnt']);L.pp.append(st['pp']);L.nl.append(st['nl'])
        L.n+=1

    f=open(path,'r',errors='replace')
    for ln in f:
        if ln[:3]=='---':
            if (st['have']&need)==need:
                flush()
                if limit and L.n>=limit:break
            L.seen|=st['have']
            st['have']=0
            if ln[:10]=='--- window':
                for t in ln.split():
                    if t[:3]=='dw=':
                        st['dw']=float(t[3:-1]);break
                st['have']=1
            continue
        if ln[:6]=='MEWMA ':
            k=ln.split()
            st['t2']=float(_v(k[1]));st['ucl']=float(_v(k[2]))
            st['trig']=1 if _v(k[3])=='True' else 0
            st['cnt']=int(_v(k[4]));st['pp']=int(_v(k[5]));st['tr']=float(_v(k[6]))
            st['have']|=2
            continue
        if ln[:2]!='  ':continue
        k=ln.split()
        h=k[0]
        if h=='FFT':
            for i in range(D6):r[i]=float(k[i+1].rsplit('>',1)[1])
            st['have']|=4
        elif h=='Z':
            for i in range(D6):
                t=k[i+1]
                if t[-1]=='*':
                    lv[i]=0;t=t[:-1]
                else:
                    lv[i]=1
                a,b=_v(t).split('/')
                xv[i]=float(a);zv[i]=float(b)
            st['have']|=8
        elif h=='SD':
            for i in range(D6):
                t=_v(k[i+1]);j=t.index('(')
                sv[i]=float(t[:j]);mv[i]=float(t[j+3:-1])
            st['have']|=16
        elif h=='D':
            for i in range(D6):dv[i]=float(_v(k[i+1]))
            st['nl']=int(_v(k[7]));st['have']|=32
        elif h=='FRZstate':
            st['frz']=int(_v(k[1]));st['calm']=int(_v(k[2]));st['tuoi']=int(_v(k[3]))
            st['have']|=64
        elif h in DIDX:
            t=k[-1]
            if t[:2]=='z=':
                j=DIDX[h];p=_v(t).split('/')
                for i in range(D6):dz[i*ND+j]=float(p[i])
    else:
        if (st['have']&need)==need:flush()
    L.seen|=st['have']
    f.close()
    if L.n==0:
        mi=[BITNM[i] for i in range(7) if (need>>i&1) and not (L.seen>>i&1)]
        raise RuntimeError('j_replay.parse: 0 tick tu %s ; can_mask=%d thay_mask=%d ; thieu loai dong: %s'%(path,need,L.seen,','.join(mi) if mi else '(du loai dong nhung khong tick nao du bo)'))
    return L


def frz_from_log(L):
    fl=bytearray(L.n)
    for t in range(1,L.n):fl[t]=L.frz[t-1]
    return fl


def replay(L,m,dthr=None,frzin=None,frz_on=1,frz_rel=8,frz_max=400,latch=6,cool=0,
           use_live=0,i0=0,i1=-1):
    if i1<0:i1=L.n
    n=i1-i0
    S=m.StateMEWMA()
    DT=m.DTHR
    if dthr is not None:
        for i in range(D6):DT[i]=dthr[i]
    W=m.MEWMA_WINDOW;MS=m.MEWMA_MIN_SAMPLES
    e=array.array('d',[0.0]*D6);vb=bytearray(D6)
    t2o=array.array('d',[0.0]*n);uco=array.array('d',[0.0]*n);tro=array.array('d',[0.0]*n)
    do=array.array('d',[0.0]*(n*D6))
    tgo=bytearray(n);sgo=bytearray(n);fzo=bytearray(n);dco=bytearray(n)
    wio=array.array('i',[-1]*n);wdo=array.array('i',[-1]*n)
    res=L.res;liv=L.live;dz=L.dz
    frz=0;calm=0;tuoi=0;reb=0;dkey=-1;drun=0;dcool=0;nfire=0;nev=0;prev=0
    for t in range(i0,i1):
        k=t-i0;b=t*D6
        for i in range(D6):
            e[i]=res[b+i]
            vb[i]=liv[b+i] if use_live else 1
        if frzin is not None:frz=frzin[t]
        fzo[k]=frz
        t2,uc,tg=S.update(e,vb,frz)
        tg=1 if tg else 0
        t2o[k]=t2;uco[k]=uc;tro[k]=S.tr;tgo[k]=tg
        if S.count>=MS:S.decomp(t2)
        else:
            for i in range(D6):S.d[i]=0.0
        d=S.d;ob=k*D6
        for i in range(D6):do[ob+i]=d[i]
        rat=1
        for i in range(D6):
            if DT[i]<=0.0:
                rat=0;break
        wi=-1;wd=0.0;ws=0.0
        if rat:
            for i in range(D6):
                if d[i]>ws*DT[i]:
                    ws=d[i]/DT[i];wi=i;wd=d[i]
        else:
            for i in range(D6):
                if d[i]>wd:
                    wd=d[i];wi=i
        sg=1 if (wi>=0 and (DT[wi]<=0.0 or wd>DT[wi])) else 0
        wdep=-1
        if wi>=0:
            zb=t*DZ+wi*ND;wz=0.0
            for j in range(ND):
                az=dz[zb+j]
                if az<0.0:az=-az
                if az>wz:
                    wz=az;wdep=j
        if not sg:wdep=-1
        wio[k]=wi;wdo[k]=wdep;sgo[k]=sg
        if dcool>0:dcool-=1
        dk=wi*ND+wdep if (tg and sg and wdep>=0) else -1
        if dk>=0 and dk==dkey:drun+=1
        else:
            dkey=dk;drun=1 if dk>=0 else 0
        if drun>=latch and dcool==0:
            dco[k]=1;nfire+=1
            if not prev:nev+=1
            if cool:dcool=cool
            prev=1
        else:
            prev=0
        if frzin is None and frz_on:
            if frz:
                tuoi+=1
                calm=calm+1 if not tg else 0
                if calm>=frz_rel:
                    frz=0;calm=0;tuoi=0
                elif tuoi>=frz_max:
                    reb+=1;frz=0;calm=0;tuoi=0
            elif tg and S.count>=W:
                frz=1;calm=0;tuoi=1
    return {'n':n,'i0':i0,'t2':t2o,'ucl':uco,'tr':tro,'d':do,'trig':tgo,'sg':sgo,
            'frz':fzo,'dec':dco,'wi':wio,'wdep':wdo,'reb':reb,'nfire':nfire,'nev':nev}


def frz_stats(fz,n):
    ep=[];a=0
    for t in range(n):
        if fz[t]:a+=1
        elif a:
            ep.append(a);a=0
    if a:ep.append(a)
    return ep


ASSUM='GIA DINH: res lam tron 4 chu so; valid doc tu dau * dong Z (live, khong phai valid=1); frz lay tu log lech mot tick'


def validate(path,ucl=50.0,w=120,cut=142,limit=0):
    L=parse(path,limit)
    print(ASSUM)
    print('log=%s n_tick=%d'%(path,L.n))
    m=load(JANUS_W=w,JANUS_UCL=ucl)
    fl=frz_from_log(L)
    for nm,ul in (('live',1),('valid=1',0)):
        R=replay(L,m,frzin=fl,use_live=ul)
        er=[];nd=0;nc=0
        for t in range(cut,L.n):
            if L.pp[t]!=6 or L.cnt[t]<w:continue
            nc+=1
            b=L.t2[t]
            if b>0.0:er.append(abs(R['t2'][t]-b)/b)
            if R['trig'][t]!=L.trig[t]:nd+=1
        print('T2 %-8s n=%d med=%.3e mad=%.3e p99=%.3e max=%.3e trig_lech=%d (%.4f pp)'%(
            nm,nc,q(er,50),mad(er),q(er,99),max(er) if er else 0.0,nd,100.0*nd/max(nc,1)))
    R=replay(L,m,frzin=None,use_live=0,frz_on=1,frz_rel=8,frz_max=400)
    bad=sum(1 for t in range(cut,L.n) if R['frz'][t]!=fl[t])
    ea=frz_stats(fl,L.n);eb=frz_stats(R['frz'],L.n)
    print('FRZ may trang thai: lech=%d/%d tick  dot_log=%d dot_replay=%d  tuoi_max log=%d replay=%d  REBASELINE=%d'%(
        bad,L.n-cut,len(ea),len(eb),max(ea) if ea else 0,max(eb) if eb else 0,R['reb']))
    er2=[]
    for t in range(cut,L.n):
        if L.pp[t]!=6 or L.cnt[t]<w:continue
        b=L.t2[t]
        if b>0.0:er2.append(abs(R['t2'][t]-b)/b)
    print('T2 frz_sim med=%.3e p99=%.3e max=%.3e'%(q(er2,50),q(er2,99),max(er2) if er2 else 0.0))


def ucltab(path,cut=500,w=120,thr=(30,40,45,50,55,60,70)):
    L=parse(path)
    print(ASSUM)
    print('log=%s n_tick=%d cut=%d'%(path,L.n,cut))
    for nm,fil in (('loc p=6,n>=W',1),('khong loc',0)):
        s=[L.t2[t] for t in range(cut,L.n) if (not fil) or (L.pp[t]==6 and L.cnt[t]>=w)]
        if not s:continue
        print('%s n=%d med=%.2f p90=%.2f p99=%.2f p999=%.2f max=%.4f'%(
            nm,len(s),q(s,50),q(s,90),q(s,99),q(s,99.9),max(s)))
        for u in thr:
            c=sum(1 for v in s if v>u)
            print('   UCL=%-4g trig=%.3f%% (%d)'%(u,100.0*c/len(s),c))


def dtab(path,cut=142,w=120):
    L=parse(path)
    print(ASSUM)
    print('log=%s n_tick=%d'%(path,L.n))
    sel=[t for t in range(cut,L.n) if L.trig[t]==0 and L.nl[t]==6 and L.cnt[t]>=w]
    print('tick in-control (not trig, nl=6, n>=W) = %d'%len(sel))
    for i in range(D6):
        v=[L.dd[t*D6+i] for t in sel]
        print('  %-5s med=%.4f p90=%.4f p99=%.4f max=%.4f'%(DIMS[i],q(v,50),q(v,90),q(v,99),max(v)))
    v=[L.tr[t] for t in range(cut,L.n) if L.cnt[t]>=w]
    print('  tr    med=%.4f p90=%.4f p95=%.4f p99=%.4f max=%.4f'%(q(v,50),q(v,90),q(v,95),q(v,99),max(v)))


if __name__=='__main__':
    a=sys.argv[1:]
    if not a:
        print('dung:')
        print('  j_replay.py check LOG [ucl] [W] [cut] [limit]')
        print('  j_replay.py ucl   LOG [cut] [W]')
        print('  j_replay.py d     LOG [cut] [W]')
        sys.exit(1)
    if a[0]=='check':
        validate(a[1],float(a[2]) if len(a)>2 else 50.0,int(a[3]) if len(a)>3 else 120,
                 int(a[4]) if len(a)>4 else 142,int(a[5]) if len(a)>5 else 0)
    elif a[0]=='ucl':
        ucltab(a[1],int(a[2]) if len(a)>2 else 500,int(a[3]) if len(a)>3 else 120)
    elif a[0]=='d':
        dtab(a[1],int(a[2]) if len(a)>2 else 142,int(a[3]) if len(a)>3 else 120)
    else:
        print('khong hieu:',a[0]);sys.exit(1)
