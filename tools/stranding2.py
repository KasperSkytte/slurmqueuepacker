#!/usr/bin/env python3
"""Break down the cross-class result: who are these fat pending jobs, how long
did they actually wait, and where was the idle capacity sitting?"""
import sqlite3, re, collections, sys

DB='biocloud.sqlite'; T0=1760000000
db=sqlite3.connect(DB); db.row_factory=sqlite3.Row
PART={}
for p,ns in {'zen3':[1,2,3,4,5,6,7],'zen3x':[8,9],'zen5':[12,13,16,17],
             'zen5x':[14,15],'interactive':[11],'gpu-a10':[10]}.items():
    for n in ns: PART[f'bio-node{n:02d}']=p
SLIM={'zen3','zen5'}
CAP={}
for r in db.execute("select node_name,tres from event_table"):
    if not r['node_name'].startswith('bio-node'): continue
    t=dict(kv.split('=') for kv in r['tres'].split(',') if '=' in kv)
    if r['node_name'] in PART: CAP[r['node_name']]=(int(t.get('1',0)),int(t.get('2',0)))

def expand(nl):
    out=[]
    for tok in re.findall(r'[^,\[]+(?:\[[^\]]*\])?', nl or ''):
        tok=tok.strip()
        if not tok: continue
        m=re.match(r'^(.*?)\[([0-9,\-]+)\]$',tok)
        if not m: out.append(tok); continue
        pre,body=m.groups()
        for seg in body.split(','):
            if '-' in seg:
                a,b=seg.split('-'); w=len(a)
                out+= [f'{pre}{i:0{w}d}' for i in range(int(a),int(b)+1)]
            else: out.append(f'{pre}{seg}')
    return out

ev=collections.defaultdict(list); pend=[]
for r in db.execute("""select nodelist,alloc_cpus,alloc_mem_mb,start,end,eligible,wait,
                       cpus_req,mem_mb,partition from jobs
                       where start>=? and end>start and alloc_cpus>0""",(T0,)):
    nodes=[n for n in expand(r['nodelist']) if n in CAP]
    if not nodes: continue
    k=len(nodes); dc,dm=r['alloc_cpus']/k, r['alloc_mem_mb']/k
    for n in nodes:
        ev[r['start']].append((n,dc,dm)); ev[r['end']].append((n,-dc,-dm))
    if r['wait']>300 and r['cpus_req']>0 and r['mem_mb']/r['cpus_req']>6000:
        pend.append((r['eligible']+60,r['cpus_req'],r['mem_mb'],r['wait'],r['partition']))

print("=== who are the fat jobs that waited >5 min? ===")
print(f"  n = {len(pend):,}")
w=sorted(p[3] for p in pend)
for q in (50,75,90,99):
    print(f"  wait p{q}: {w[int(q/100*(len(w)-1))]/3600:.2f} h")
print(f"  wait max: {w[-1]/3600:.1f} h   total waiting time: {sum(w)/3600:,.0f} job-hours")
mem=sorted(p[2] for p in pend)
print("  total memory requested:")
for q in (50,90,99):
    print(f"    p{q}: {mem[int(q/100*(len(mem)-1))]/1024:,.0f} GB")
print(f"    > 1.0 TB (cannot fit any zen3 node): {sum(1 for m in mem if m>1021567)/len(mem)*100:.1f}%")
print(f"    > 1.5 TB (cannot fit any zen5 node): {sum(1 for m in mem if m>1537338)/len(mem)*100:.1f}%")
cpus=sorted(p[1] for p in pend)
print(f"  CPUs requested  p50={cpus[len(cpus)//2]}  p90={cpus[int(.9*len(cpus))]}  max={cpus[-1]}")

times=sorted(ev); pend.sort(key=lambda x:x[0])
used={n:[0.0,0.0] for n in CAP}
idle_by_part=collections.Counter(); stranded_by_part=collections.Counter()
hits=collections.Counter(); probes=0; pi=0; prev=times[0]
for t in times:
    dtl=(t-prev)/3600.0
    if dtl>0:
        for n,(uc,um) in used.items():
            cc,cm=CAP[n]; fc,fm=cc-uc,cm-um
            idle_by_part[PART[n]]+=fc*dtl
            stranded_by_part[PART[n]]+=max(0.0,fc-fm/4267)*dtl
        while pi<len(pend) and pend[pi][0]<=t:
            _,jc,jm,_,_=pend[pi]; pi+=1; probes+=1
            fits=set()
            for n,(uc,um) in used.items():
                if PART[n] in SLIM and CAP[n][0]-uc>=jc and CAP[n][1]-um>=jm:
                    fits.add(PART[n])
            if fits:
                hits['any']+=1
                for f in fits: hits[f]+=1
    for n,dc,dm in ev[t]:
        u=used[n]; u[0]+=dc; u[1]+=dm
        if u[0]<1e-6: u[0]=0.0
        if u[1]<1e-6: u[1]=0.0
    prev=t

print("\n=== idle / stranded CPU-hours by partition (p50 demand = 4267 MB/CPU) ===")
for p in ('zen3','zen5','zen3x','zen5x'):
    cap=sum(CAP[n][0] for n in CAP if PART[n]==p)
    span=(times[-1]-times[0])/3600.0
    print(f"  {p:<7} cap={cap:>5} CPU  idle={idle_by_part[p]:>12,.0f} CPU-h "
          f"({idle_by_part[p]/(cap*span)*100:5.1f}% of its capacity)  "
          f"stranded={stranded_by_part[p]:>11,.0f} CPU-h")

print(f"\n=== cross-class availability (each probe independent) ===")
print(f"  probes: {probes:,}   at least one slim node had room: {hits['any']:,} ({hits['any']/probes*100:.1f}%)")
print(f"    zen3 had room: {hits['zen3']:,} ({hits['zen3']/probes*100:.1f}%)")
print(f"    zen5 had room: {hits['zen5']:,} ({hits['zen5']/probes*100:.1f}%)")
