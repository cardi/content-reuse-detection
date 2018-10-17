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
              if(k == "e28937da14b7c5d085c224f3fc3542ea1d6dcd0a" or k == "af9f42048790777ace53c9276da7471fbb3b0db3"):
                yield(k, v)
            except (ValueError):
              sys.stderr.write("error: " + str(value) + "\n")

   # don't think a reducer is necessary
    def reducer(self, key, values):
      yield(key, values);

if __name__ == '__main__':
    MRSha1.run()  
