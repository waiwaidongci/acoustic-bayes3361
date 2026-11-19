import hashlib,json


def dumps(obj):
    return json.dumps(obj,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)


def item_payload(position,reference,values,fold,predicted,posterior):
    return {'position':int(position),'reference':reference,'values':values,
            'claim':{'fold':int(fold),'predicted':predicted,'posterior':float(posterior)}}


def item_digest(payload):
    return hashlib.sha256(dumps(payload).encode('utf-8')).hexdigest()


def shard_digest(payloads):
    h=hashlib.sha256(b'shard-v1|')
    for payload in payloads:h.update(item_digest(payload).encode())
    return h.hexdigest()


class ContentHasher:
    """Streaming equivalent of hashing the ordered list of item digests."""

    def __init__(self,parameters):
        self._hash=hashlib.sha256(b'packet-v1|'+dumps(parameters).encode('utf-8')+b'|')

    def update(self,digest):
        self._hash.update(str(digest).encode())

    def hexdigest(self):
        return self._hash.hexdigest()


def content_hash(parameters,item_digests):
    hasher=ContentHasher(parameters)
    for digest in item_digests:hasher.update(digest)
    return hasher.hexdigest()
