from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol, PickleProtocol, RawValueProtocol
import sys
import hashlib
import binascii

class MRSha1(MRJob):

    def mapper(self, _, value): # key is always null?
        if(len(value) != 0):
            #sys.stderr.write(str(value) + "\n")
            try:
              k, v = value.split("\t", 1)
              #sys.stderr.write("filename: " + str(k) + "\n")
              yield(k, str(v.count("seq")) + "^" + v)
            except (ValueError):
              sys.stderr.write("error: " + str(value) + "\n")

   # don't think a reducer is necessary
    def reducer(self, key, values):
        #yield(key, sum(values)); # works
        #v = ''.join([x+":" for x in values])
        #yield(key, v)
        sums = []
        paths = []
        for x in values:
          one, v = x.split("^", 1)
          sums.append(int(one))
          paths.append(v.split(":")[1]) 
        yield(key, str(sum(sums)) + "^" + min(paths, key=len))
        #answer = str(sum(sums));
        #yield(key, answer);

if __name__ == '__main__':
    MRSha1.run()  
