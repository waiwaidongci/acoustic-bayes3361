"""Independent, fixed-memory verification for a sealed judgement book.

This module intentionally does not import scikit-learn. It re-derives:

* the fold plan of ``StratifiedKFold(shuffle=True, random_state=42)``;
* the training-only ``StandardScaler`` mean/scale of every fold;
* the Gaussian naive-Bayes class means, variances, priors and per-item
  posterior of every fold.

Everything is computed with compensated streaming accumulators, so peak memory
is O(folds x classes x features) and does not grow with the number of items.
Each fold model is always fitted on *all* training items in the book (every
item whose fold is not the fold under test), never on one batch or one shard.
"""
import math
from collections import defaultdict

import numpy as np

FEATURES=('rms','centroid','kurtosis')
SEED=42
POSTERIOR_TOLERANCE=1e-9


class StructuralError(ValueError):
    pass


def _neumaier(total,compensation,value):
    """Kahan-Babuska-Neumaier compensated addition."""
    updated=total+value
    if abs(total)>=abs(value):
        compensation+=(total-updated)+value
    else:
        compensation+=(value-updated)+total
    return updated,compensation


class Welford:
    """Compensated streaming mean and population variance.

    The running mean is kept as a compensated high/low pair and ``m2`` is
    accumulated with Neumaier summation. This keeps per-fold statistics stable
    to ~1e-16 relative error even at 1e5 observations, so the independent result
    can be compared against a producer that used a two-pass NumPy computation.
    """

    __slots__=('count','_mean_hi','_mean_c','_m2_hi','_m2_c','dimension')

    def __init__(self,dimension):
        self.dimension=dimension
        self.count=0
        self._mean_hi=[0.0]*dimension
        self._mean_c=[0.0]*dimension
        self._m2_hi=[0.0]*dimension
        self._m2_c=[0.0]*dimension

    @property
    def mean(self):
        return [hi+c for hi,c in zip(self._mean_hi,self._mean_c)]

    def add(self,values):
        self.count+=1
        count=self.count
        for j,value in enumerate(values):
            delta=value-(self._mean_hi[j]+self._mean_c[j])
            step=delta/count
            hi,c=_neumaier(self._mean_hi[j],self._mean_c[j],step)
            self._mean_hi[j]=hi
            self._mean_c[j]=c
            mean=hi+c
            term=delta*(value-mean)
            m2hi,m2c=_neumaier(self._m2_hi[j],self._m2_c[j],term)
            self._m2_hi[j]=m2hi
            self._m2_c[j]=m2c

    def variance(self):
        if not self.count:return [0.0]*self.dimension
        return [max(0.0,(hi+c)/self.count)
                for hi,c in zip(self._m2_hi,self._m2_c)]


def features(values):
    return [float(values[key]) for key in FEATURES]


# --------------------------------------------------------------------------
# structure + fold plan
# --------------------------------------------------------------------------
def count_items(source):
    """Streaming pass: item count, class order/counts and per-feature range."""
    n=0
    counts={}
    order=[]
    low=[math.inf]*len(FEATURES)
    high=[-math.inf]*len(FEATURES)
    for item in source():
        values=item[2]
        label=str(values['label'])
        if label not in counts:
            counts[label]=0
            order.append(label)
        counts[label]+=1
        for j,value in enumerate(features(values)):
            if value<low[j]:low[j]=value
            if value>high[j]:high[j]=value
        n+=1
    return {'n':n,'counts':counts,'order':order,'low':low,'high':high}


def check_structure(profile,folds):
    if len(profile['counts'])<2 or min(profile['counts'].values())<folds:
        raise StructuralError('至少两种故障类型，且每类样本数不少于交叉验证折数')
    if profile['n']<12:
        raise StructuralError('分类研究至少需要12条样本')
    if all(profile['low'][j]==profile['high'][j] for j in range(len(FEATURES))):
        raise StructuralError('全部声学特征恒定，无法学习故障差异，请补充独立观测')


def fold_sequence(counts,order,folds):
    """Reproduce sklearn's per-class shuffled fold blocks.

    ``allocation[i][c]`` is ``np.bincount(y_order[i::n_splits])`` for encoded
    class ``c``. The generator is consumed once per class in first-appearance
    order, exactly as ``StratifiedKFold(shuffle=True, random_state=42)`` does.
    """
    n_classes=len(order)
    allocation=np.zeros((folds,n_classes),dtype=int)
    offset=0
    for ci,label in enumerate(order):
        count=counts[label]
        for i in range(folds):
            start=offset+((i-offset)%folds)
            if start<offset+count:
                allocation[i,ci]=(offset+count-1-start)//folds+1
        offset+=count
    rng=np.random.RandomState(SEED)
    sequence={}
    for ci,label in enumerate(order):
        blocks=np.arange(folds).repeat(allocation[:,ci])
        rng.shuffle(blocks)
        sequence[label]=[int(value) for value in blocks]
    return sequence


class FoldAssigner:
    """Deterministic per-item fold assignment with a resumable cursor."""

    __slots__=('sequence','cursor')

    def __init__(self,sequence,cursor=None):
        self.sequence=sequence
        self.cursor=dict(cursor or {})

    def fold(self,label):
        index=self.cursor.get(label,0)
        if index>=len(self.sequence[label]):raise StructuralError('样本类别数量与封包不一致')
        self.cursor[label]=index+1
        return self.sequence[label][index]


# --------------------------------------------------------------------------
# fitting (three streaming passes over the whole book)
# --------------------------------------------------------------------------
def build_models(source,parameters):
    folds=int(parameters['folds'])
    smoothing=float(parameters['smoothing'])
    profile=count_items(source)
    check_structure(profile,folds)
    sequence=fold_sequence(profile['counts'],profile['order'],folds)

    assigner=FoldAssigner(sequence)
    scalers=[Welford(len(FEATURES)) for _ in range(folds)]
    for item in source():
        own=assigner.fold(str(item[2]['label']))
        sample=features(item[2])
        for index in range(folds):
            if index!=own:scalers[index].add(sample)
    means=[]
    scales=[]
    for accumulator in scalers:
        variance=accumulator.variance()
        means.append(accumulator.mean)
        scales.append([math.sqrt(value) if value>0 else 1.0 for value in variance])

    assigner=FoldAssigner(sequence)
    global_stats=[Welford(len(FEATURES)) for _ in range(folds)]
    class_stats=[defaultdict(lambda:Welford(len(FEATURES))) for _ in range(folds)]
    for item in source():
        label=str(item[2]['label'])
        own=assigner.fold(label)
        sample=features(item[2])
        for index in range(folds):
            if index==own:continue
            scaled=[(sample[j]-means[index][j])/scales[index][j]
                    for j in range(len(FEATURES))]
            global_stats[index].add(scaled)
            class_stats[index][label].add(scaled)

    models=[]
    for index in range(folds):
        epsilon=smoothing*max(global_stats[index].variance())
        classes=sorted(class_stats[index])
        total=sum(stat.count for stat in class_stats[index].values())
        models.append({'fold':index+1,'classes':classes,
                       'theta':[class_stats[index][label].mean for label in classes],
                       'var':[[value+epsilon for value in class_stats[index][label].variance()]
                              for label in classes],
                       'prior':[class_stats[index][label].count/total for label in classes],
                       'train_count':total,'mean':means[index],'scale':scales[index],
                       'epsilon':epsilon})
    return {'profile':profile,'sequence':sequence,'models':models}


# --------------------------------------------------------------------------
# per-item verification
# --------------------------------------------------------------------------
def predict_one(model,sample):
    mean=model['mean']
    scale=model['scale']
    scaled=[(sample[j]-mean[j])/scale[j] for j in range(len(FEATURES))]
    joint=[]
    for index in range(len(model['classes'])):
        theta=model['theta'][index]
        variance=model['var'][index]
        value=math.log(model['prior'][index])
        for j in range(len(FEATURES)):
            value-=0.5*math.log(2.0*math.pi*variance[j])
            value-=0.5*((scaled[j]-theta[j])**2)/variance[j]
        joint.append(value)
    best=max(range(len(joint)),key=joint.__getitem__)
    peak=joint[best]
    weights=[math.exp(value-peak) for value in joint]
    return model['classes'][best],weights[best]/sum(weights)


def init_state(models):
    size=len(models[0]['classes'])
    return {'phase':'items','cursor':{},'through':0,'checked':0,'correct':0,
            'confusion':[[0]*size for _ in range(size)],
            'fold_total':[0]*len(models),'fold_correct':[0]*len(models),
            'first_divergence':None}


def verify_items(source,assigner,models,state,tolerance=POSTERIOR_TOLERANCE):
    """Stream items from the resume cursor, reporting the first divergence.

    ``state['through']`` is the position of the last item accepted (0 before the
    first item), so a resume re-reads with ``position > through``. ``state`` is
    mutated in place and is JSON-serialisable. Returning ``complete=False`` means
    a divergence was found; the caller must not admit. Batches may be fed in
    successive calls with the same ``assigner``/``state`` to checkpoint between
    them.
    """
    classes=models[0]['classes']
    class_index={label:index for index,label in enumerate(classes)}
    confusion=state['confusion']
    fold_total=state['fold_total']
    fold_correct=state['fold_correct']
    through=state['through']
    for item in source():
        position,reference,values,claimed_fold,claimed_predicted,claimed_posterior=item
        actual=str(values['label'])
        fold=assigner.fold(actual)
        predicted,posterior=predict_one(models[fold],features(values))
        divergence=None
        if int(claimed_fold)!=fold+1:
            divergence={'kind':'fold','claimed':int(claimed_fold),'computed':fold+1}
        elif str(claimed_predicted)!=predicted:
            divergence={'kind':'predicted','claimed':str(claimed_predicted),'computed':predicted}
        elif abs(float(claimed_posterior)-posterior)>tolerance:
            divergence={'kind':'posterior','claimed':float(claimed_posterior),
                        'computed':posterior,
                        'delta':abs(float(claimed_posterior)-posterior)}
        if divergence:
            divergence.update({'position':int(position),'reference':reference,'actual':actual})
            state['through']=through
            state['cursor']=dict(assigner.cursor)
            state['first_divergence']=divergence
            return state,False
        state['checked']+=1
        fold_total[fold]+=1
        if predicted==actual:
            state['correct']+=1
            fold_correct[fold]+=1
        confusion[class_index[actual]][class_index[predicted]]+=1
        through=int(position)
    state['through']=through
    state['cursor']=dict(assigner.cursor)
    return state,True


def macro_f1(confusion):
    scores=[]
    for index,row in enumerate(confusion):
        true_positive=row[index]
        predicted=sum(confusion[r][index] for r in range(len(confusion)))
        actual=sum(row)
        if not actual:continue
        precision=true_positive/predicted if predicted else 0.0
        recall=true_positive/actual
        scores.append(2*precision*recall/(precision+recall) if precision+recall else 0.0)
    return sum(scores)/len(scores) if scores else 0.0


def summary(state,models,expected_count,content_hash):
    checked=state['checked']
    return {'items':expected_count,'checked':checked,'all_matched':checked==expected_count,
            'accuracy':(state['correct']/checked) if checked else 0.0,
            'macro_f1':macro_f1(state['confusion']),
            'classes':models[0]['classes'],
            'fold_training_counts':[model['train_count'] for model in models],
            'confusion_matrix':state['confusion'],
            'fold_total':state['fold_total'],
            'fold_correct':state['fold_correct'],
            'fold_accuracy':[(correct/total if total else 0.0)
                             for correct,total in zip(state['fold_correct'],state['fold_total'])],
            'content_hash':content_hash,'posterior_tolerance':POSTERIOR_TOLERANCE}
