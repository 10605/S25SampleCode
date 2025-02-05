import sys
import math
import random
import hashlib

BITS_PER_INT = 31

class BloomFilter(object):

    def __init__(self,seed=0,maxInserts=10000,falsePosProb=0.01):
        self.numBits = int(2 * maxInserts*math.log(1.0/falsePosProb) + 0.5)
        self.numHashes = int(1.5 * math.log(1.0/falsePosProb) + 0.5)
        self.bits = []
        for i in range(round(self.numBits/BITS_PER_INT + 1)):
            self.bits.append(int(0))

        rnd = random.Random()
        rnd.seed(seed)
        self.ithHashSeed = []
        for i in range(self.numHashes):
            self.ithHashSeed.append(rnd.getrandbits(BITS_PER_INT))

    def ithHash(self,i,value):
        return (self.ithHashSeed[i] ^ hash(value)) % self.numBits

    def insert(self,value):
        for i in range(self.numHashes):
            hi = self.ithHash(i,value)
            self.setbit(hi)

    def contains(self,value):
        for i in range(self.numHashes):
            hi = self.ithHash(i,value)
            if not self.testbit(hi): return False
        return True

    def setbit(self,index):
        self.bits[index//BITS_PER_INT] = int(self.bits[index//BITS_PER_INT] | (1 << (index % BITS_PER_INT)))

    def testbit(self,index):
        return self.bits[index//BITS_PER_INT] & (1 << (index % BITS_PER_INT))

    def density(self):
        numBits = 0.0
        numBitsSet = 0.0
        for bi in self.bits:
            for j in range(BITS_PER_INT):
                numBits += 1
                if bi & (1 << j):
                    numBitsSet += 1
        return numBitsSet/numBits

if __name__=="__main__":
    #python bloomfilter.py 7000 0.01 data/train.txt data/test.txt
    n = p = file1 = file2 = None
    try:
        n = int(sys.argv[1])
        p = float(sys.argv[2])
        file1 = sys.argv[3]
        file2 = sys.argv[4]
    except IndexError:
        print('usage: n p file1 file2')
        sys.exit(-1)
    bf = BloomFilter(maxInserts=n,falsePosProb=p)
    lines1 = lines2 = overlap = 0
    fp1 = open(file1)
    for line in fp1:
        if line.strip(): bf.insert(line.strip())
        lines1 += 1
    print('bit size',bf.numBits,'kb size',bf.numBits/(8.0*1024))
    print('numhashes',bf.numHashes)
    print('density',bf.density())
    fp2 = open(file2)
    for line in fp2:
        if line.strip() and bf.contains(line.strip()): 
            overlap += 1
        lines2 += 1
    print('lines in', file1, lines1)
    print('lines in', file2, lines2)
    print('overlap estimated by Bloom filter',overlap)

