from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol, PickleProtocol, RawValueProtocol
import os
import sys
import hashlib
import binascii

class MRSha1(MRJob):

    #HADOOP_INPUT_FORMAT="org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat"
    HADOOP_INPUT_FORMAT="org.apache.hadoop.mapred.SequenceFileAsTextInputFormat"

    def mapper(self, _, value): # key is always null
        k, v = value.split("\t")
        v = v.replace(' ','');
        v = binascii.unhexlify(v)

        sys.stderr.write("filename: " + str(k) + "\n")

        # http://stackoverflow.com/questions/541390/extracting-extension-from-filename-in-python
        extension = os.path.splitext(k)[1][1:].strip()

        if(extension == "htm" or extension == "html"):
            chunks = v.split("<p>") # split on paragraph breaks
            r_hashes = []

            for chunk in chunks:
              h = hashlib.sha1()
              h.update(chunk)
              hz = h.hexdigest()
              r_hashes.append(hz)

            for r_hash in r_hashes:
                yield(r_hash, 1)

    def combiner(self, key, values):
        yield(key, sum(values))

    def reducer(self, key, values):
        yield(key, sum(values))

if __name__ == '__main__':
    MRSha1.run()  
