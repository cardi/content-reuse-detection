from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol, PickleProtocol, RawValueProtocol
import sys
import hashlib
import binascii

class MRSha1(MRJob):

    def mapper(self, _, value): # key is always null?
        if(len(value) != 0):
            try:
              value2 = value.replace("\"", "")  # remove quotes
              k, v = value2.split("\t", 1)      # split k,v
              number, rest = v.split("^", 1)    # split on ^
              yield(None, number)
            except (ValueError):
              sys.stderr.write("error: " + str(value) + "\n")

   # don't think a reducer is necessary
    def reducer(self, key, values):
        yield(None, None)

if __name__ == '__main__':
    MRSha1.run()  
