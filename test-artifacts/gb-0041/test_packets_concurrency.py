import os,sys,random,json,threading,time
sys.path.insert(0,'/workspace/backend')
os.environ['DATABASE_URL']='sqlite:////tmp/conc.db'
if os.path.exists('/tmp/conc.db'):os.remove('/tmp/conc.db')
from src.config.database import Base,engine,Session
from src.models import packet as pm
from src.services import packets,packet_worker
from src.services.algorithm import calculate
from sqlalchemy import insert
Base.metadata.create_all(engine)
packets._enqueue=lambda pid: True
class U:
    def __init__(s,e,r):s.email=e;s.role=r
A=U('a','analyst')
params={'folds':3,'smoothing':1e-9}
n=4000;rng=random.Random(1);names=['正常','轴承磨损','转子偏心']
recs=[{'reference':f'P-{i:05d}','values':{'rms':round(max(.1,rng.gauss([1.2,3.,2.][i%3],.35)),3),'centroid':round(max(100,rng.gauss([900,1600,2300][i%3],170)),2),'kurtosis':round(max(.1,rng.gauss([2.8,5.2,3.9][i%3],.45)),3),'label':names[i%3]}} for i in range(n)]
res=calculate(recs,params);fold_of={}
for f in res['folds']:
    for i in f['validation_indices']:fold_of[i]=f['fold']
items=[{'position':i+1,'reference':row['reference'],'values':recs[i]['values'],'claim':{'fold':fold_of[i],'predicted':row['predicted'],'posterior':row['posterior']}} for i,row in enumerate(res['rows'])]
with Session() as db:
    pid=packets.create_packet(db,A,type('D',(),{'book_key':'CONC','name':'并发测试册','description':'','expected_count':n,'parameters':params})())['id']
    for k in range(0,n,100):
        db.execute(insert(pm.PacketShard).values(packet_id=pid,shard_key=f's{k//100:04d}',item_count=100,digest=f'x{k}',created_at='t'));db.flush()
        sh=db.query(pm.PacketShard).order_by(pm.PacketShard.id.desc()).first().id
        db.execute(insert(pm.PacketItem),[{'packet_id':pid,'position':it['position'],'reference':it['reference'],'values':it['values'],'claimed_fold':it['claim']['fold'],'claimed_predicted':it['claim']['predicted'],'claimed_posterior':it['claim']['posterior'],'shard_id':sh,'created_at':'t'} for it in items[k:k+100]])
    db.commit()
    p=db.get(pm.Packet,pid);p.received_count=n;db.commit()
    packets.seal_packet(db,A,pid,type('D',(),{'expected_version':None})())
outcomes=[]
def worker(tag):
    with Session() as db:
        try:outcomes.append((tag,packet_worker.verify_once(db,pid)))
        except Exception as e:outcomes.append((tag,{'status':'crash','error':str(e)}))
threads=[threading.Thread(target=worker,args=(i,)) for i in range(3)]
for t in threads:t.start()
for t in threads:t.join()
print('outcomes:',[ (t,o.get('status')) for t,o in outcomes])
with Session() as db:
    p=db.get(pm.Packet,pid)
    print('final state',p.state,'leases',p.verify_lease_until)
    assert p.state=='verified',p.state
    r=p.result
    assert r['checked']==n and r['all_matched'] and r['accuracy'] is not None
    print('checked',r['checked'],'all_matched',r['all_matched'])
    statuses=[o.get('status') for _,o in outcomes]
    assert statuses.count('verified')==1,statuses
    assert 'busy' in statuses or 'superseded' in statuses,statuses
print('CONCURRENCY OK')
