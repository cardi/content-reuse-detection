from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol, PickleProtocol, RawValueProtocol
import sys
import hashlib
import binascii
from urlparse import urlparse

class MRAwsHashFile(MRJob):
  def mapper(self, _, value): 
    if(len(value) != 0):
      try:
        k, v = value.split("\t", 1)
        prefix = "HashFile-"
        if k.startswith(prefix):
          #yield(k[len(prefix):], 1) # output sample file, too?
          yield(k[len(prefix):], "1^" + v)
      except (ValueError):
        sys.stderr.write("error: " + str(value) + "\n")

  def reducer(self, key, values):
    #sums = []
    running_sum = 0

    paths = []
    j = 0
    num_samples = 10

    seen = []

    interval = 10

    for x in values:
      one, v = x.split("^", 1)

      #sums.append(int(one))
      running_sum = running_sum + int(one)

      if(running_sum % interval == 0):
        print "running_sum: %d" % (running_sum)

      ####paths.append(v)

      #if(j < num_samples):

      #  url = v[v.find("http://"):] # get the URL
      #  url_parsed = urlparse(url)
      #  url_netloc = url_parsed.netloc

      #  if(url_netloc not in seen):
      #    paths.append(v) # fill up our list
      #    seen.append(url_netloc)   # put the netloc inside seen
      #    j = j + 1                 # increment counter

      #else:
      #  if(j == num_samples): # do an initial sort for our comparison
      #    paths.sort()
      #    paths.sort(key=len)

      #  y = paths[len(paths)-1]
      #  if v > y: # if our new path is lexically greater than the last element, ignore it
      #    pass
      #  else:
      #    url = v[v.find("http://"):] # get the URL
      #    url_parsed = urlparse(url)
      #    url_netloc = url_parsed.netloc

      #    if(url_netloc not in seen):
      #      seen.append(url_netloc)   # put the netloc inside seen

      #      paths.append(v)
      #      paths.sort()
      #      paths.sort(key=len)
      #      paths.pop() # pop the last path

    # in case len(paths) < num_samples, still need to sort
    paths.sort()
    paths.sort(key=len)

    result = ""
    for p in paths:
      result = result + p + "^"

    yield(key, str(running_sum) + "^" + result)


   # lexically greater
   # then check length
   # keep stuff @ size 10

   #paths.sort()        # sort paths lexically
   #paths.sort(key=len) # sort paths by length

   #result = ""
   #i = 0
   #seen = []

    #while(i < 10): # we want 10 paths
    #for p in paths:
    #  if(i < 10):
    #    i = i + 1
    #    result = result + p + "^"
    #  else:
    #    if(i >= 10):
    #      break
      #url = p[p.find("http://"):] # get the URL
      #url_parsed = urlparse(url)
      #url_netloc = url_parsed.netloc
      #if(url_netloc not in seen and i < 10):
      #  result=result+p+"^"       # append result
      #  seen.append(url_netloc)   # put the netloc inside seen
      #  i = i + 1                 # increment counter
      #else:
      #  if(i >= 10):
      #    break                   # we have enough unique paths

    #yield(key, str(sum(sums)) + "^" + path)
    #yield(key, sum(values))
    #yield(key, str(sum(sums)) + "^" + result)

if __name__ == '__main__':
  MRAwsHashFile.run()  
