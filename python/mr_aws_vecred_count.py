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
              value2 = value.replace("\"", "")  # strip quotes
              k, v = value2.split("\t", 1)      # split k,v on \t
              bad, total = v.split("^", 1)      # split 0^0 -> 0 and 0
              #sys.stderr.write(str(value2))
              #sys.stderr.write("filename: " + str(k) + "\n")

              # >>> for i in range(1,a.count("/")+1):
              # ...     print a.rsplit("/", i)[0]
              # ...
              # geocities/YAHOOIDS/S/u/SunsetStrip/Garage/5644/bandas
              # geocities/YAHOOIDS/S/u/SunsetStrip/Garage/5644
              # geocities/YAHOOIDS/S/u/SunsetStrip/Garage
              # geocities/YAHOOIDS/S/u/SunsetStrip
              # geocities/YAHOOIDS/S/u
              # geocities/YAHOOIDS/S
              # geocities/YAHOOIDS
              # geocities
              for i in range(1, k.count("/")+1):
                temp_k = k.rsplit("/", i)[0]
                #yield(temp_k, str(bad) + "^" + str(total))
                yield(temp_k, str(1) + "^" + str(bad) + "^" + str(total))

            except (ValueError):
              sys.stderr.write("error: " + str(value) + "\n")

   # don't think a reducer is necessary
    def reducer(self, key, values):
        pages = []
        sums = []
        totals = []
        for x in values:
          one, bad, total = x.split("^", 2)
          pages.append(int(one))
          sums.append(int(bad))
          totals.append(int(total))
        yield(key, str(sum(pages)) + "^" + str(sum(sums)) + "^" + str(sum(totals)))

if __name__ == '__main__':
    MRVecCount.run()  
