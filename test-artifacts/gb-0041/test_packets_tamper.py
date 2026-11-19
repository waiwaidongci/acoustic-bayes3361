import os,sys,random,json
sys.path.insert(0,'/workspace/backend')
os.environ['DATABASE_URL']='sqlite:////tmp/tamper.db'
if os.path.exists('/tmp/tamper.db'):os.remove('/tmp/tamper.db')
from src.config.database import Base,engine,Session
from src.models import packet as pm
from src.services import packets,packet_worker
from src.services.algorithm import calculate
from sqlalchemy import update
Base.metadata.create_all(engine)
packets._enqueue=lambda pid: True
class U:
    def __init__(s,e,r):s.email=e;s.role=r
A=U('a','analyst')
params={'folds':3,'smoothing':1e-9}
rng=random.Random(9);names=['正常','轴承磨损','转子偏心'];n=60
recs=[{'reference':f'P-{i:03d}','values':{'rms':round(max(.1,rng.gauss([1.2,3.,2.][i%3],.35)),3),'centroid':round(max(100,rng.gauss([900,1600,2300][i%3],170)),2),'kurtosis':round(max(.1,rng.gauss([2.8,5.2,3.9][i%3],.45)),3),'label':names[i%3]}} for i in range(n)]
res=calculate(recs,params);fold_of={}
for f in res['folds']:
    for i in f['validation_indices']:fold_of[i]=f['fold']
items=[{'position':i+1,'reference':row['reference'],'values':recs[i]['values'],'claim':{'fold':fold_of[i],'predicted':row['predicted'],'posterior':row['posterior']}} for i,row in enumerate(res['rows'])]
def shard_obj(spec):
    its=[type('I',(),{'position':it['position'],'reference':it['reference'],'values':it['values'],'claim':type('C',(),{'fold':it['claim']['fold'],'predicted':it['claim']['predicted'],'posterior':it['claim']['posterior']})()}) for it in spec['items']]
    return type('D',(),{'shard_key':spec['shard_key'],'expected_version':None,'items':its})()
with Session() as db:
    pid=packets.create_packet(db,A,type('D',(),{'book_key':'TAMP','name':'篡改测试册','description':'','expected_count':n,'parameters':params})())['id']
    for k in range(0,n,10):packets.receive_shard(db,A,pid,shard_obj({'shard_key':f's{k//10:03d}','items':items[k:k+10]}))
    packets.seal_packet(db,A,pid,type('D',(),{'expected_version':None})())
# tamper one item's rms directly in the store after sealing
with Session() as db:
    db.execute(update(pm.PacketItem).where(pm.PacketItem.packet_id==pid,pm.PacketItem.position==17).values(values={**items[16]['values'],'rms':items[16]['values']['rms']+3.0}))
    db.commit()
with Session() as db:
    out=packet_worker.verify_once(db,pid)
print('tamper outcome',out['status'])
with Session() as db:
    p=db.get(pm.Packet,pid);print('state',p.state,'error',p.error)
    assert p.state=='rejected' and '哈希' in p.error
print('TAMPER DETECTED OK')
