"""Oracle: the independent streaming verifier must agree exactly with the
production algorithm (sklearn StratifiedKFold + StandardScaler + GaussianNB)
that produced the claims.

The verifier in ``services/verification.py`` imports no scikit-learn; this test
confirms fold assignment, predicted class and posterior agree to 1e-9 over many
class distributions, fold counts and label sets.
"""
import sys,random
sys.path.insert(0,'/workspace/backend')
from src.services.algorithm import calculate
from src.services import verification as V


def build(recs,folds,smoothing=1e-9):
    source=lambda:[(index,r['reference'],r['values'],0,'',0.0) for index,r in enumerate(recs)]
    return V.build_models(source,{'folds':folds,'smoothing':smoothing})


def verify(recs,folds,smoothing=1e-9):
    built=build(recs,folds,smoothing)
    assigner=V.FoldAssigner(built['sequence'])
    claims=[]
    for index,r in enumerate(recs):
        fold=assigner.fold(str(r['values']['label']))
        predicted,posterior=V.predict_one(built['models'][fold],V.features(r['values']))
        claims.append((fold+1,predicted,posterior))
    return claims,built


def main():
    rng=random.Random(2026)
    checked=0
    for trial in range(80):
        n=rng.randint(24,240)
        folds=rng.choice([2,3,4,5])
        names=['正常','轴承磨损','转子偏心','松动','气隙异常'][:rng.randint(2,5)]
        recs=[]
        for i in range(n):
            label=rng.choice(names)
            recs.append({'reference':f'R{i:04d}','values':{
                'rms':round(max(.1,rng.gauss(2,1.4)),4),
                'centroid':round(max(100,rng.gauss(1500,650)),4),
                'kurtosis':round(max(.1,rng.gauss(4,1.6)),4),'label':label}})
        smoothing=rng.choice([1e-9,1e-8,1e-6])
        try:expected=calculate([{'reference':r['reference'],'values':r['values']} for r in recs],
                               {'folds':folds,'smoothing':smoothing})
        except ValueError:
            continue  # structurally invalid sample set; production rejects it too
        claims,_=verify(recs,folds,smoothing)
        fold_of={}
        for fold in expected['folds']:
            for index in fold['validation_indices']:fold_of[index]=fold['fold']
        for index,row in enumerate(expected['rows']):
            fold,predicted,posterior=claims[index]
            assert fold==fold_of[index],(trial,index,fold,fold_of[index])
            assert predicted==row['predicted'],(trial,index,predicted,row['predicted'])
            assert abs(posterior-row['posterior'])<1e-9,(trial,index,posterior,row['posterior'])
        checked+=1
    print('oracle datasets checked:',checked)
    assert checked>=40,checked


if __name__=='__main__':main()
