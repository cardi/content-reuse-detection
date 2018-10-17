from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol, PickleProtocol, RawValueProtocol
import sys
import hashlib
import binascii
from urlparse import urlparse

class MRCcCount(MRJob):

    def mapper(self, _, value):
      if(len(value) != 0):
        #sys.stderr.write(str(value) + "\n")
        try:
          k, v = value.split("\t", 1)
          #sys.stderr.write("filename: " + str(k) + "\n")
          yield(k, "1" + "^" + v)
        except (ValueError):
          sys.stderr.write("error: " + str(value) + "\n")

    def reducer(self, key, values):
      sums = []
      #path = ""
      paths = []
      for x in values:
        one, v = x.split("^", 1)
        sums.append(int(one))

        ## OLD
        #if path == "":
        #  path = v
        #else:
        #  if len(v) < len(path):
        #    path = v

        ## NEW
        paths.append(v)

      paths.sort()        # sort paths lexically
      paths.sort(key=len) # sort paths by length

      ## old
      #result=""
      #for x in paths[:10]: # get at most 10 paths starting from the shortest 
      #  result=result+x+"^"

      result = ""
      i = 0
      seen = []
      #while(i < 10): # we want 10 paths
      for p in paths:
        url = p[p.find("http://"):] # get the URL
        url_parsed = urlparse(url)
        url_netloc = url_parsed.netloc

        if(url_netloc not in seen and i < 10):
          result=result+p+"^"       # append result
          seen.append(url_netloc)   # put the netloc inside seen
          i = i + 1                 # increment counter
        else:
          if(i >= 10):
            break                   # we have enough unique paths

      #yield(key, str(sum(sums)) + "^" + path)
      yield(key, str(sum(sums)) + "^" + result)

if __name__ == '__main__':
    MRCcCount.run()  
