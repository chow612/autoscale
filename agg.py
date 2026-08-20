tot=0;fl=0;ig=0;leaf={}
for ln in open('/tmp/prof.txt'):
    ln=ln.strip()
    if not ln:continue
    i=ln.rfind(' ')
    try:n=int(ln[i+1:])
    except ValueError:continue
    st=ln[:i];tot+=n
    if 'flush_loop' in st:fl+=n
    if 'ingest' in st:ig+=n
    k=st.split(';')[-1];leaf[k]=leaf.get(k,0)+n
if not tot:raise SystemExit('prof.txt rong')
print('mau=%d  flush_loop=%.1f%%  ingest=%.1f%%  khac=%.1f%%'%(
    tot,100.0*fl/tot,100.0*ig/tot,100.0*(tot-fl-ig)/tot))
for k,v in sorted(leaf.items(),key=lambda kv:-kv[1])[:20]:
    print('  %6.2f%%  %s'%(100.0*v/tot,k))
