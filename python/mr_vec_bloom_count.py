from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol, PickleProtocol, RawValueProtocol
import sys
import hashlib
import binascii
import random
import array
import os
import itertools
import math
from string import hexdigits
try :
    import json
except ImportError :
    import simplejson as json

class BloomFilter( object ) :
    HASH_ALGO = 'sha256'
    DIGEST_SIZE = hashlib.new(HASH_ALGO).digest_size
    M = 16384
    K = 8

    def __init__( self, m=None, k=None, ivs=None, value=None ) :
        if m is not None :
            self.M = m
        if k is not None :
            self.K = k
        if value is not None :
            self._filter = array.array('B',value)
            l = len(self._filter)
            if l < self.M :
                self._filter.extend(itertools.repeat(0,self.M-l))
        else :
            self._filter = array.array('B',itertools.repeat(0,self.M))
        self.calc_nbytes()
        if ivs is None :
            self.ivs = [os.urandom(16) for x in
                        range((self.n_bytes*self.K/self.DIGEST_SIZE)-1)]
            self.ivs.insert(0,'')
        else :
            self.ivs = ivs

    def calc_nbytes( self ) :
        n_bits = int(math.floor(math.log(self.M,2)))
        self.n_bytes = int(math.ceil(n_bits/8.0))

    def __eq__( self, other ) :
        if self.K == other.K :
            if self.M == other.M :
                if self._filter == other._filter :
                    return True
        return False

    def __ne__( self, other ) :
        return not (self == other)

    def __setstate__( self, d ) :
        self.M = d['m']
        self.K = d['k']
        self._filter = array.array('B',d['filter'])
        self.calc_nbytes()
        self.ivs = d['ivs']

    def __getstate__( self ) :
        return {
            'm'      : self.M,
            'k'      : self.K,
            'filter' : self._filter.tolist(),
            'ivs'    : self.ivs,
        }

    def hash( self, d ) :
        h_l,l = [], []
        for iv in self.ivs :
            h = hashlib.new(self.HASH_ALGO,iv)
            h.update(d)
            h_l.append(h.digest())
        hashes = ''.join(h_l)
        pos = 0
        for x in range(self.K) :
            v = 0
            for n,x in enumerate(hashes[pos:pos+self.n_bytes]) :
                v += ord(x) << (n*8)
            l.append(v%self.M)
            pos += self.n_bytes
        return l

    def full( self ) :
        return sum(bool(x) for x in self._filter) == self.M

    def empty( self ) :
        return sum(bool(x) for x in self._filter) == 0

    def __nonzero__( self ) :
        return not self.empty()

    def add( self, value ) :
        hash = self.hash(value)
        if all(self._filter[x] for x in hash) :
            return
        for x in hash :
            self._filter[x] += 1
            if self._filter[x] == 0 :   # Prevent overflow
                self._filter[x] -= 1

    def _remove( self, value, hash, cnt=0 ) :
        for x in hash :
            if self._filter[x] :
                self._filter[x] -= 1
        # Check if the value is still present
        if cnt > 5 :
            raise RuntimeError, "Can't remove the element from this filter"
        if all(self._filter[x] for x in hash) :  
            return self._remove( value, hash, cnt+1 )
        return True

    def remove( self, value ) :
        hash = self.hash(value) 
        return self._remove( value, hash )
        
    def __contains__( self, value ) :
        return all(self._filter[x] for x in self.hash(value))

    def serialize( self ) :
        d = {
            'm'      : self.M,
            'k'      : self.K,
        }
        min_val = min(self._filter)
        cfilter = [x-min_val for x in self._filter.tolist()]
        if any(cfilter) :
            d['filter'] = cfilter
        if min_val :
            d['min_val'] = min_val
        if any(self.ivs) :
            d['ivs'] = self.ivs
        return d

    @classmethod
    def unserialize( cls, d ) :
        min_val = d.get('min_val',0)
        filter = [x+min_val for x in d.get('filter',[])]
        if not filter :
            filter = None
        ivs = d.get('ivs',[''])
        return cls( d['m'],d['k'],ivs,filter )

    def toJSON( self ) :
        return json.dumps(self.serialize())

    @classmethod
    def fromJSON( cls, s ) :
        d = json.loads(s)
        return cls.unserialize( d )

HEX_MAP         = list('0123456789ABCDEF')
REVERSE_HEX_MAP = dict(zip(HEX_MAP,range(16)))

def _rle( s ) :
    '''
    Simple RLE to encode on wire data
    If the run length is less than 11 chars,
    its rather more efficient to use the string.
    '''
    for k, g in itertools.groupby(s) :
        l = len(list(g))
        if l < 11 :
            yield k*l
        else :
            yield [k,l]

def rle( s ) :
    it = itertools.groupby(_rle(s), lambda x : isinstance(x, (str,unicode)))
    for k,g in it :
        if k is False :
            for x in g :
                yield x
        else :
            yield ''.join(g)

def unrle( l ) :
    '''
    Undo the RLE
    '''
    o_l = []
    for x in l :
        if isinstance(x,list) :
            o_l.append(x[0]*x[1])
        else :
            o_l.append(x)
    return ''.join(o_l)


class SimpleBloomFilter(BloomFilter) :
    def __init__( self, capacity=100, err=0.1, ivs=None, value=None ) :
        '''
        @capacity: The capacity of the filter
        @err: The tolerable false positives rate ( 0 < err < 0.5 )
        '''
        m, k = self.calc_mk( capacity, err )
        BloomFilter.__init__( self, m, k, ivs, value )

    @staticmethod
    def calc_mk( capacity, err ) :
        m = int(math.ceil(capacity * math.log(err) / math.log(1.0 / 2**math.log(2))))
        k = int(math.floor(math.log(2) * m / capacity))
        return m, k

    def serialize( self ) :
        d = BloomFilter.serialize( self )
        cfilter = d.get('filter',[])
        if cfilter :
            max_value = max(cfilter)
            if max_value < 16 :
                filter = ''.join([HEX_MAP[x] for x in cfilter])
            else :  # Its a byte value
                l = []
                for x in cfilter :
                    l.append(HEX_MAP[x>>4])
                    l.append(HEX_MAP[x&0x0F])
                filter = ''.join(l)
            d['filter'] = list(rle(filter))
        return d

    @classmethod
    def unserialize( cls, d ) :
        filter = unrle(d.get('filter',[]))
        f_l = len(filter)
        m = d['m']
        if f_l == 0 :
            cfilter = []
        elif f_l == m :
            cfilter = [REVERSE_HEX_MAP[x] for x in filter]
        elif f_l == (m << 1) :
            state = 0
            v = 0
            cfilter = []
            for x in filter :
                if state == 0 :
                    v = REVERSE_HEX_MAP[x] << 4
                else :
                    v += REVERSE_HEX_MAP[x]
                    cfilter.append(v)
                state = (state + 1) & 1
        else :
            RuntimeError, "Inconsistent filter length %d for m: %d"%(f_l,m)
        d['filter'] = cfilter
        return BloomFilter.unserialize( d )

class MRVecCount(MRJob):

    def mapper(self, _, value): # key is always null?
        #hashes = dict({"fa3adbff3f27875ff947ae82442abbe026ac5aae": 1})
        f = open("w.bloomfilter.0100.correct", "r")
        x = f.readline().rstrip()
        hashes = BloomFilter.fromJSON(x)
        f.close()

        if(len(value) != 0):
            #sys.stderr.write(str(value) + "\n")
            try:
              value2 = value.replace("\"", "") 
              k, v = value2.split("\t", 1)
              #sys.stderr.write(str(value2))
              #sys.stderr.write("filename: " + str(k) + "\n")

              bad = 0
              total = 0
              for x in v.split(":"):
                if(x != ""): # make sure it isn't empty
                  total = total + 1
                  if(x in hashes):
                    bad = bad + 1

              yield(k, str(bad) + "^" + str(total))

            except (ValueError):
              sys.stderr.write("error: " + str(value) + "\n")

    # don't think a reducer is necessary
   #def reducer(self, key, values):

if __name__ == '__main__':
    MRVecCount.run()  
