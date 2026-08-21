import sys,array,j_replay as J

D6=J.D6
DIMS=J.DIMS
SDW=(0.3317,388431.0,20.7576,0.3291,0.0276,0.6234)
SIG_SDF=(0.166,0.0158,0.0,0.0395,-0.0514,1.0)
SIG_PURE=(0.0,0.0,0.0,0.0,0.0,1.0)
FAR=1.0


def clean_pass(L,m,i0,i1):
    S=m.StateMEWMA()
    e=array.array('d',[0.0]*D6);vb=bytearray(b'\x01'*D6)
    res=L.res
    t2=array.array('d');za=[array.array('d') for _ in range(D6)]
    for t in range(i0,i1):
        b=t*D6
        for i in range(D6):e[i]=res[b+i]
        v,u,g=S.update(e,vb,0)
        if S.count<m.MEWMA_WINDOW:continue
        t2.append(v)
        z=S.z
        for i in range(D6):za[i].append(abs(z[i]))
    return t2,za


def fam_rate(za,h):
    n=len(za[0]);c=0
    for k in range(n):
        for i in range(D6):
            if za[i][k]>h[i]:
                c+=1;break
    return 100.0*c/n


def calib(t2,za):
    ucl=J.q(t2,100.0-FAR)
    hs=J.q(za[5],100.0-FAR)
    base=[J.q(za[i],100.0-FAR) for i in range(D6)]
    lo=1.0;hi=4.0
    for _ in range(40):
        k=0.5*(lo+hi)
        if fam_rate(za,[k*b for b in base])>FAR:lo=k
        else:hi=k
    k=0.5*(lo+hi)
    hv=[k*b for b in base]
    ra=100.0*sum(1 for v in t2 if v>ucl)/len(t2)
    rb=100.0*sum(1 for v in za[5] if v>hs)/len(za[5])
    rc=fam_rate(za,hv)
    return ucl,hs,hv,k,ra,rb,rc


def trial(L,m,t0,warm,hor,sh,ucl,hs,hv):
    S=m.StateMEWMA()
    e=array.array('d',[0.0]*D6);vb=bytearray(b'\x01'*D6)
    res=L.res
    a=b=c=-1
    s=t0-warm
    if s<0:s=0
    end=t0+hor
    if end>L.n:end=L.n
    for t in range(s,end):
        q=t*D6
        if t>=t0:
            for i in range(D6):e[i]=res[q+i]+sh[i]
        else:
            for i in range(D6):e[i]=res[q+i]
        v,u,g=S.update(e,vb,0)
        if t<t0:continue
        d=t-t0
        z=S.z
        if a<0 and v>ucl:a=d
        if b<0 and (z[5] if z[5]>=0 else -z[5])>hs:b=d
        if c<0:
            for i in range(D6):
                az=z[i] if z[i]>=0 else -z[i]
                if az>hv[i]:
                    c=d;break
        if a>=0 and b>=0 and c>=0:break
    return a,b,c


def rep(tag,v,nt):
    ok=[x for x in v if x>=0]
    if not ok:
        print('  %-10s KHONG PHAT HIEN LAN NAO (n=%d)'%(tag,nt));return
    print('  %-10s med=%5.1f mad=%4.1f p90=%5.1f max=%5.1f  kiem_duyet=%d/%d (%.0f%%)'%(
        tag,J.q(ok,50),J.mad(ok),J.q(ok,90),max(ok),nt-len(ok),nt,100.0*(nt-len(ok))/nt))


def main():
    a=sys.argv[1:]
    path=a[0] if a else 'ing_20260819T122931Z.log'
    sig=SIG_PURE if len(a)>1 and a[1]=='pure' else SIG_SDF
    non=int(a[2]) if len(a)>2 else 100
    warm=300;hor=250;w=120
    L=J.parse(path)
    m=J.load(JANUS_W=w)
    print(J.ASSUM)
    print('log=%s n=%d W=%d chu_ky=%s onset=%d warm=%d horizon=%d'%(
        path,L.n,w,'pure' if sig is SIG_PURE else 'sdf',non,warm,hor))
    t2,za=clean_pass(L,m,0,L.n)
    ucl,hs,hv,k,ra,rb,rc=calib(t2,za)
    print('hieu chinh tren %d tick sach: ucl=%.4f  h_slow=%.4f  k=%.4f'%(len(t2),ucl,hs,k))
    print('  h_i = '+' '.join('%s=%.4f'%(DIMS[i],hv[i]) for i in range(D6)))
    print('  FAR do duoc: A=%.4f%%  B=%.4f%%  C=%.4f%%   (muc tieu %.2f%%)'%(ra,rb,rc,FAR))
    if abs(ra-FAR)>0.05 or abs(rb-FAR)>0.05 or abs(rc-FAR)>0.05:
        print('  CANH BAO: FAR lech qua 0.05 pp, so sanh khong hop le')
    lo=warm+50;hi=L.n-hor
    step=(hi-lo)//non
    ons=[lo+i*step for i in range(non)]
    for dl in (0.0,2.0,2.5,3.0,4.0):
        sh=[dl*sig[i]*SDW[i] for i in range(D6)]
        A=[];B=[];C=[]
        for t0 in ons:
            x,y,z=trial(L,m,t0,warm,hor,sh,ucl,hs,hv)
            A.append(x);B.append(y);C.append(z)
        print('delta=%.1f  (shift slow=%+.4f logit, cpu=%+.4f)'%(dl,sh[5],sh[0]))
        rep('A T2',A,len(ons))
        rep('B slow',B,len(ons))
        rep('C any6',C,len(ons))
        ka=[x for x in A if x>=0];kc=[x for x in C if x>=0]
        if ka and kc:
            print('  A - C = %+.1f tick (trung vi)'%(J.q(ka,50)-J.q(kc,50)))
        sys.stdout.flush()


if __name__=='__main__':
    main()
