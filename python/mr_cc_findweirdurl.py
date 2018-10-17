from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol, PickleProtocol, RawValueProtocol
import sys
import hashlib
import binascii

class MRVecCount(MRJob):

    def mapper(self, _, value): # key is always null?
        if(len(value) != 0):
            #sys.stderr.write(str(value) + "\n")
            try:
              value2 = value.replace("\"", "") 
              k, v = value2.split("\t", 1)
               
              urls = ["net-accounting"]

              for url in urls:
                if url in k:
                  yield(k, 1)

            except (ValueError):
              sys.stderr.write("error: " + str(value) + "\n")

if __name__ == '__main__':
    MRVecCount.run()  
