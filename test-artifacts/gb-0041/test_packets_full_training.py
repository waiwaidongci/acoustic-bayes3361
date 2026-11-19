import sys,random
sys.path.insert(0,'/workspace/backend')
import numpy as np
from src.services import verification as V
from src.services.algorithm import calculate
# Build 20k dataset; assert each fold train_count == n - fold_size (all non-test items)
rng=random.Random(4);n=20000;names=['A','B','C']
recs=[{'reference':f'R{i}','values':{'rms':rng.gauss([1,3,2][i%3],1),'centroid':rng.gauss([900,1600,2300][i%3],200),'kurtosis':rng.gauss([2.8,5.2,3.9][i%3],.5),'label':names[i%3]}} for i in range(n)]
src=lambda:[(i,r['reference'],r['values'],0,'',0.0) for i,r in enumerate(recs)]
built=V.build_models(src,{'folds':3,'smoothing':1e-9})
totals=[m['train_count'] for m in built['models']]
fold_sizes=[sum(1 for lab in built['sequence'] for f in built['sequence'][lab] if f==k) for k in range(3)]
print('train counts',totals,'fold sizes',fold_sizes)
assert all(t==n-s for t,s in zip(totals,fold_sizes)),(totals,fold_sizes)
assert sum(fold_sizes)==n
print('FULL TRAINING SET PER FOLD OK')
