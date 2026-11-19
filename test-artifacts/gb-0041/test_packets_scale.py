import os,sys,random,time,tracemalloc,gc
sys.path.insert(0,'/workspace/backend')
os.environ['DATABASE_URL']='sqlite:////tmp/mem3.db'
if os.path.exists('/tmp/mem3.db'):os.remove('/tmp/mem3.db')
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
for n in (20000,50000,100000):
    recs=[];rng=random.Random(9);names=['正常','轴承磨损','转子偏心']
    for i in range(n):
        c=i%3
        recs.append({'reference':f'P-{i:06d}','values':{'rms':round(max(.1,rng.gauss([1.2,3.,2.][c],.35)),3),'centroid':round(max(100,rng.gauss([900,1600,2300][c],170)),2),'kurtosis':round(max(.1,rng.gauss([2.8,5.2,3.9][c],.45)),3),'label':names[c]}})
    res=calculate(recs,params);fold_of={}
    for f in res['folds']:
        for i in f['validation_indices']:fold_of[i]=f['fold']
    items=[{'position':i+1,'reference':row['reference'],'values':recs[i]['values'],'claim':{'fold':fold_of[i],'predicted':row['predicted'],'posterior':row['posterior']}} for i,row in enumerate(res['rows'])]
    del res,recs
    with Session() as db:
        pid=packets.create_packet(db,A,type('D',(),{'book_key':f'BK-{n}','name':'规模测试册','description':'','expected_count':n,'parameters':params})())['id']
        for k in range(0,n,2000):
            db.execute(insert(pm.PacketShard).values(packet_id=pid,shard_key=f's{k//2000:04d}',item_count=min(2000,n-k),digest=f'x{k}',created_at='t'));db.flush()
            sh=db.query(pm.PacketShard).order_by(pm.PacketShard.id.desc()).first().id
            db.execute(insert(pm.PacketItem),[{'packet_id':pid,'position':it['position'],'reference':it['reference'],'values':it['values'],'claimed_fold':it['claim']['fold'],'claimed_predicted':it['claim']['predicted'],'claimed_posterior':it['claim']['posterior'],'shard_id':sh,'created_at':'t'} for it in items[k:k+2000]])
        db.commit()
        p=db.get(pm.Packet,pid);p.received_count=n;db.commit()
        packets.seal_packet(db,A,pid,type('D',(),{'expected_version':None})())
    del items;gc.collect()
    tracemalloc.start();t=time.time()
    with Session() as db: out=packet_worker.verify_once(db,pid)
    cur,peak=tracemalloc.get_traced_memory();tracemalloc.stop()
    print(f'n={n:6d} status={out["status"]:9s} time={time.time()-t:5.1f}s peak={peak/1e6:6.1f}MB current={cur/1e6:5.1f}MB')
