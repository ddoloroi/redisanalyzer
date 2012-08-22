import operator
import collections
import redis

max_type_length=7
default_namespace_divider=':'

def average(values):
    """Computes the arithmetic mean of a list of numbers.

    >>> print average([20, 30, 70])
    40.0
    """
    return sum(values, 0.0) / len(values)

# Gratis: http://code.activestate.com/recipes/577081-humanized-representation-of-a-number-of-bytes/
def humanize_bytes(bytes, precision=1):
    """Return a humanized string representation of a number of bytes.

    Assumes `from __future__ import division`.

    >>> humanize_bytes(1)
    '1 byte'
    >>> humanize_bytes(1024)
    '1.0 kB'
    >>> humanize_bytes(1024*123)
    '123.0 kB'
    >>> humanize_bytes(1024*12342)
    '12.1 MB'
    >>> humanize_bytes(1024*12342,2)
    '12.05 MB'
    >>> humanize_bytes(1024*1234,2)
    '1.21 MB'
    >>> humanize_bytes(1024*1234*1111,2)
    '1.31 GB'
    >>> humanize_bytes(1024*1234*1111,1)
    '1.3 GB'
    """
    abbrevs = (
        (1<<50L, 'PB'),
        (1<<40L, 'TB'),
        (1<<30L, 'GB'),
        (1<<20L, 'MB'),
        (1<<10L, 'KB'),
        (1, 'bytes')
    )
    if bytes == 1:
        return '1 byte'
    for factor, suffix in abbrevs:
        if bytes >= factor:
            break
    return '%.*f %s' % (precision, bytes / factor, suffix)

class BucketStats:

    def __init__(self):
        self.count = 0
        self.bytes = 0
        self.type = None

        self.item_lengths = []

    def record(self, redis, key):
        debug_info = redis.debug_object(key)
        object_type = redis.type(key)

        if self.type is None:
            self.type = object_type
        elif self.type != object_type:
            self.type = 'multiple'
        else: pass

        if self.type == 'zset':
            l = redis.zcard(key)
            self.item_lengths.append(l)


        self.count += 1
        self.bytes += debug_info['serializedlength']


    def __cmp__(self, other):
        return self.bytes.__cmp__(other.bytes)

    def formatted_bytes(self):
        return humanize_bytes(self.bytes)

    def type_specific_label(self):
        if self.type == 'zset':
            avg_num_items = average(self.item_lengths)
            avg_item_size = self.bytes / avg_num_items / self.count
            return 'Avg set count: %0.2f, Avg bytes / set item: %s' % (avg_num_items, humanize_bytes(avg_item_size))
        else:
            return ''

def bucket_for_key(key, namespace_divider):
    comps = key.split(namespace_divider)
    return namespace_divider.join(comps[:-1])

def sorted_dict(d):
    return reversed(sorted(d.iteritems(), key=operator.itemgetter(1)))

def print_column(l):
    print '\t'.join(l)

def analyze(host, port, db, namespace_divider=default_namespace_divider):
    r = redis.Redis(host='localhost', port=6379, db=0)

    keys = r.keys()
    buckets = collections.defaultdict(BucketStats)

    for k in keys:
        bucket_key = bucket_for_key(k, namespace_divider)
        buckets[bucket_key].record(r, k)

    max_key_length = max(len(k) for k in keys)
    max_count_length = max(len(str(b.count)) for b in buckets.itervalues())
    max_bytes_length = max(len(b.formatted_bytes()) for b in buckets.itervalues())

    print 'Categorized %s keys into %s buckets' % (len(keys), len(buckets))
    print
    for (k, b) in sorted_dict(buckets):
        print_column([
            k.ljust(max_key_length),
            str(b.count).rjust(max_count_length),
            str(b.formatted_bytes()).rjust(max_bytes_length),
            b.type.ljust(max_type_length),
            b.type_specific_label()
            ])

if __name__ == '__main__':
    analyze(host='localhost', port=6379, db=0)
