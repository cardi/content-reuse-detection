from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol, PickleProtocol, RawValueProtocol
import sys
import hashlib
import binascii

class MRSha1(MRJob):

    #HADOOP_INPUT_FORMAT="org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat"
    HADOOP_INPUT_FORMAT="org.apache.hadoop.mapred.SequenceFileAsTextInputFormat"

   #def mapper(self, key, value):
    def mapper(self, _, value): # key is always null?
        #yield(value, 1) # works
        k, v = value.split("\t")
        v = v.replace(' ','');
        h = hashlib.sha1()
        h.update(binascii.unhexlify(v))
        hz = h.hexdigest()
        #yield(hashlib.sha1(binascii.unhexlify(v)), 1)
        #yield(hz, k) 
        sys.stderr.write("filename: " + str(k) + "\n")
        #print "processing %s\n" % k
        yield(hz, 1)
        #yield("hello", 1)

   # don't think a reducer is necessary
    def reducer(self, key, values):
        #yield(key, sum(values)); # works
        #v = ''.join([x+":" for x in values])
        #yield(key, v)
        yield(key, sum(values))

if __name__ == '__main__':
    MRSha1.run()  
