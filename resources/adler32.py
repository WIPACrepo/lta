"""A script to calculate adler32 checksum of given files."""
import sys
from zlib import adler32

BLOCKSIZE = 256 * 1024 * 1024

# adapted from: https://gist.github.com/kofemann/2303046
for fname in sys.argv[1:]:
    asum = 1
    with open(fname, "rb") as f:
        while True:
            data = f.read(BLOCKSIZE)
            if not data:
                break
            asum = adler32(data, asum)
            if asum < 0:
                asum += 2**32
    print(hex(asum)[2:10].zfill(8).lower(), fname)
