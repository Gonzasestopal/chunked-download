""" Chunked Download - Download partial file data using Python.

    What it does
    -==========-

    1. Access to the file data by chunk as each chunk download completes.

       Usage
    -==========-

    python chunk_d.py <fileurl> <numchunks>

       Example
    -==========-

    python chunk_d.py http://samplecsvs.s3.amazonaws.com/Sacramentorealestatetransactions.csv 5
"""

import argparse
import urllib2

# positional arguments
parser = argparse.ArgumentParser()
parser.add_argument("fileurl", help="valid file url")
parser.add_argument("numchunks", help="number of splitted subfiles", type=int)
args = parser.parse_args()

url = args.fileurl
numchunks = args.numchunks

# bypass most of HTTP Forbidden request errors
hdr = {'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'}
req = urllib2.Request(url, headers=hdr)

# file information
response = urllib2.urlopen(req)
meta = response.info()
bname = url.split('/')[-1]
fsize = int(meta.getheaders("Content-Length")[0])

# get size of each chunk
chunksz = int(float(fsize) / float(numchunks))

total_bytes = 0
postfix = ''

for x in range(numchunks):
    chunkfilename = bname + '-' + str(x + 1) + postfix

    # append to the last chunk leftover data
    if x == numchunks - 1:
        chunksz = fsize - total_bytes

    try:
        print 'Writing file', chunkfilename
        data = response.read(chunksz)
        total_bytes += len(data)
        chunkf = file(chunkfilename, 'wb')
        chunkf.write(data)
        chunkf.close()
    except (OSError, IOError), e:
        print e
        break

print 'Done.'
