import os,sys,random,json
sys.path.insert(0,'/workspace/backend')
os.environ['DATABASE_URL']='sqlite:////tmp/hashdet.db'
if os.path.exists('/tmp/hashdet.db'):os.remove('/tmp/hashdet.db')
from src.config.database import Base,engine,Session
from src.models import packet as pm
from src.services import packets
from sqlalchemy import insert
Base.metadata.create_all(engine);packets._enqueue=lambda pid:True
class U:
    def __init__(s,e,r):s.email=e;s.role=r
A=U('a','analyst')
params={'folds':3,'smoothing':1e-9}
n=40;rng=random.Random(1);names=['正常','轴承磨损','转子偏心']
items=[{'position':i+1,'reference':f'P-{i:03d}','values':{'rms':round(max(.1,rng.gauss([1.2,3.,2.][i%3],.35)),3),'centroid':round(max(100,rng.gauss([900,1600,2300][i%3],170)),2),'kurtosis':round(max(.1,rng.gauss([2.8,5.2,3.9][i%3],.45)),3),'label':names[i%3]},'claim':{'fold':i%3+1,'predicted':names[i%3],'posterior':0.9}} for i in range(n)]
def shard_obj(spec):
    its=[type('I',(),{'position':it['position'],'reference':it['reference'],'values':it['values'],'claim':type('C',(),{'fold':it['claim']['fold'],'predicted':it['claim']['predicted'],'posterior':it['claim']['posterior']})()}) for it in spec['items']]
    return type('D',(),{'shard_key':spec['shard_key'],'expected_version':None,'items':its})()
# packet A: sequential shards
with Session() as db:
    a=packets.create_packet(db,A,type('D',(),{'book_key':'A','name':'顺序册','description':'','expected_count':n,'parameters':params})())['id']
    for k in range(0,n,10):packets.receive_shard(db,A,a,shard_obj({'shard_key':f's{k//10}','items':items[k:k+10]}))
    ha=packets.seal_packet(db,A,a,type('D',(),{'expected_version':None})())['content_hash']
# packet B: shuffled shards, plus a spurious re-send of an existing identical item then a conflicting resolve back to existing
with Session() as db:
    b=packets.create_packet(db,A,type('D',(),{'book_key':'B','name':'乱序册','description':'','expected_count':n,'parameters':params})())['id']
    order=list(range(0,n,10));random.Random(5).shuffle(order)
    for k in order:packets.receive_shard(db,A,b,shard_obj({'shard_key':f's{k//10}','items':items[k:k+10]}))
    # resend identical shard
    packets.receive_shard(db,A,b,shard_obj({'shard_key':f's{order[0]//10}','items':items[order[0]:order[0]+10]}))
    hb=packets.seal_packet(db,A,b,type('D',(),{'expected_version':None})())['content_hash']
print('hashA',ha)
print('hashB',hb)
assert ha==hb,'content hash must be independent of arrival order'
print('HASH DETERMINISTIC ACROSS ARRIVAL ORDER OK')
