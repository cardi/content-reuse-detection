import sys, binascii

f = open('README.rst', 'rb')
c = f.read()
f.close()

print binascii.hexlify(c)
