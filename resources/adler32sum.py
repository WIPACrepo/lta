"""A script to calculate the adler32 checksum of a supplied filename."""
import sys

from lta.crypto import adler32sum


if __name__ == '__main__':
    print(adler32sum(sys.argv[1]))
