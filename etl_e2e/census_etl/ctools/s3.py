import urllib.parse 
import subprocess
import copy
import json
import os
import tempfile
import sys
from subprocess import run,PIPE,Popen,check_call

# 
# This creates an S3 file that supports seeking and caching.
#

global debug

_LastModified='LastModified'
_ETag='ETag'
_StorageClass='StorageClass'
_Key='Key'
_Size='Size'

READ_CACHE_SIZE=4096                 # big enough for front and back caches
MAX_READ=65536*16
debug=False

READTHROUGH_CACHE_DIR='/mnt/tmp/s3cache'

def get_bucket_key(loc):
    """Given a location, return the bucket and the key"""
    p = urllib.parse.urlparse(loc)
    if p.scheme=='s3':
        return (p.netloc, p.path[1:])
    if p.scheme=='':
        if p.path.startswith("/"):
            (ignore,bucket,key) = p.path.split('/',2)
        else:
            (bucket,key) = p.path.split('/',1)
        return (bucket,key)
    assert ValueError("{} is not an s3 location".format(loc))


def aws_s3api(cmd):
    fcmd = ['aws','s3api','--output=json'] + cmd
    if debug:
        print(" ".join(fcmd),file=sys.stderr)
    data = subprocess.check_output(fcmd, encoding='utf-8')
    if not data:
        return None
    try:
        return json.loads(data)
    except (TypeError,json.decoder.JSONDecodeError) as e:
        raise RuntimeError("s3 api {} failed data: {}".format(cmd,data))

def put_object(bucket,key,fname):
    """Given a bucket and a key, upload a file"""
    return aws_s3api(['put-object','--bucket',bucket,'--key',key,'--body',fname])

def get_object(bucket,key,fname):
    """Given a bucket and a key, upload a file"""
    return aws_s3api(['get-object','--bucket',bucket,'--key',key,fname])

def head_object(bucket,key):
    """Wrap the head-object api"""
    return aws_s3api(['head-object','--bucket',bucket,'--key',key])

PAGE_SIZE=1000
MAX_ITEMS=1000
def list_objects(bucket,prefix,limit=None,delimiter=None):
    """Returns a generator that lists objects in a bucket. Returns a list of dictionaries, including Size and ETag"""
    next_token = None
    total = 0
    while True:
        cmd = ['list-objects-v2','--bucket',bucket,'--prefix',prefix,
               '--page-size',str(PAGE_SIZE),'--max-items',str(MAX_ITEMS)]
        if delimiter:
            cmd += ['--delimiter',delimiter]
        if next_token:
            cmd += ['--starting-token',next_token]
        
        res = aws_s3api(cmd)
        if not res:
            return
        if 'Contents' in res:
            for data in res['Contents']:
                yield data
                total += 1
                if limit and total>=limit:
                    return

            if 'NextToken' not in res:
                return               # no more!
            next_token = res['NextToken']
        elif 'CommonPrefixes' in res:
            for data in res['CommonPrefixes']:
                yield data
            return
        else:
            return
            


def etag(obj):
    """Return the ETag of an object. It is a known bug that the S3 API returns ETags wrapped in quotes
    see https://github.com/aws/aws-sdk-net/issue/815"""
    etag = obj['ETag']
    if etag[0]=='"':
        return etag[1:-1]
    return etag

def object_sizes(sobjs):
    """Return an array of the object sizes"""
    return [obj['Size'] for obj in sobjs]

def sum_object_sizes(sobjs):
    """Return the sum of all the object sizes"""
    return sum( object_sizes(sobjs) )

def any_object_too_small(sobjs):
    """Return if any of the objects in sobjs is too small"""
    return any([size < MIN_MULTIPART_COMBINE_OBJECT_SIZE for size in object_sizes(sobjs)])

def download_object(tempdir,bucket, obj):
    """Given a dictionary that defines an object, download it, and set the fname property to be where it was downloaded"""
    if 'fname' not in obj:
        obj['fname'] = tempdir+"/"+os.path.basename(obj['Key'])
        get_object(bucket, obj['Key'], obj['fname'])
    
def concat_downloaded_objects(obj1, obj2):
    """Concatenate two downloaded files, delete the second"""
    # Make sure both objects exist
    assert os.path.exists(obj1['fname'])
    assert os.path.exists(obj2['fname'])

    # Concatenate with cat  (it's faster than doing it in Python)
    subprocess.run(['cat',obj2['fname']],stdout=open(obj1['fname'],'ab'))

    # Update obj1
    obj1['Size'] += obj2['Size']
    if 'ETag' in obj1:          # if it had an eTag
        del obj1['ETag']        # it is no longer valid
    os.unlink(obj2['fname']) # remove the second file
    return


class S3File:
    """Open an S3 file that can be seeked. This is done by caching to the local file system."""
    def __init__(self,name,mode='rb'):
        self.name   = name
        self.url    = urlparse(name)
        if self.url.scheme != 's3':
            raise RuntimeError("url scheme is {}; expecting s3".format(url.scheme))
        self.bucket = self.url.netloc
        self.key    = self.url.path[1:]
        self.fpos   = 0
        self.tf     = tempfile.NamedTemporaryFile()
        cmd = ['aws','s3api','list-objects','--bucket',self.bucket,'--prefix',self.key,'--output','json']
        data = json.loads(Popen(cmd,encoding='utf8',stdout=PIPE).communicate()[0])
        file_info = data['Contents'][0]
        self.length = file_info['Size']
        self.ETag   = file_info['ETag']

        # Load the caches

        self.frontcache = self._readrange(0,READ_CACHE_SIZE) # read the first 1024 bytes and get length of the file
        if self.length > READ_CACHE_SIZE:
            self.backcache_start = self.length-READ_CACHE_SIZE
            if debug: print("backcache starts at {}".format(self.backcache_start))
            self.backcache  = self._readrange(self.backcache_start,READ_CACHE_SIZE)
        else:
            self.backcache  = None

    def _readrange(self,start,length):
        # This is gross; we copy everything to the named temporary file, rather than a pipe
        # because the pipes weren't showing up in /dev/fd/?
        # We probably want to cache also... That's coming
        cmd = ['aws','s3api','get-object','--bucket',self.bucket,'--key',self.key,'--output','json',
               '--range','bytes={}-{}'.format(start,start+length-1),self.tf.name]
        if debug:print(cmd)
        data = json.loads(Popen(cmd,encoding='utf8',stdout=PIPE).communicate()[0])
        if debug:print(data)
        self.tf.seek(0)         # go to the beginning of the data just read
        return self.tf.read(length) # and read that much
        
    def __repr__(self):
        return "FakeFile<name:{} url:{}>".format(self.name,self.url)

    def read(self,length=-1):
        # If length==-1, figure out the max we can read to the end of the file
        if length==-1:
            length = min(MAX_READ, self.length - self.fpos + 1)
        
        if debug:
            print("read: fpos={}  length={}".format(self.fpos,length))
        # Can we satisfy from the front cache?
        if self.fpos < READ_CACHE_SIZE and self.fpos+length < READ_CACHE_SIZE:
            if debug:print("front cache")
            buf = self.frontcache[self.fpos:self.fpos+length]
            self.fpos += len(buf)
            if debug:print("return 1: buf=",buf)
            return buf

        # Can we satisfy from the back cache?
        if self.backcache and (self.length - READ_CACHE_SIZE < self.fpos):
            if debug:print("back cache")
            buf = self.backcache[self.fpos - self.backcache_start:self.fpos - self.backcache_start + length]
            self.fpos += len(buf)
            if debug:print("return 2: buf=",buf)
            return buf

        buf = self._readrange(self.fpos, length)
        self.fpos += len(buf)
        if debug:print("return 3: buf=",buf)
        return buf

    def seek(self,offset,whence=0):
        if debug:print("seek({},{})".format(offset,whence))
        if whence==0:
            self.fpos = offset
        elif whence==1:
            self.fpos += offset
        elif whence==2:
            self.fpos = self.length + offset
        else:
            raise RuntimeError("whence={}".format(whence))
        if debug:print("   ={}  (self.length={})".format(self.fpos,self.length))

    def tell(self):
        return self.fpos

    def write(self):
        raise RuntimeError("Write not supported")

    def flush(self):
        raise RuntimeError("Flush not supported")

    def close(self):
        return

#
# S3 Cache
#



# Tools for reading and write files from Amazon S3 without boto or boto3
# http://boto.cloudhackers.com/en/latest/s3_tut.html
# but it is easier to use the AWS cli, since it's configured to work.
# 
# This could be redesigned to simply use the S3File() below

def s3open(path, mode="r", encoding=sys.getdefaultencoding(), cache=False):
    """
    Open an s3 file for reading or writing. Can handle any size, but cannot seek.
    We could use boto.
    http://boto.cloudhackers.com/en/latest/s3_tut.html
    but it is easier to use the aws cli, since it is present and more likely to work.
    """
    if "b" in mode:
        encoding = None

    cache_name = os.path.join(READTHROUGH_CACHE_DIR, path.replace("/","_"))

    # If not caching and a cache file is present, delete it. 
    if not cache and os.path.exists(cache_name):
        os.unlink(cache_name)

    if cache and ('w' not in mode):
        if not os.path.exists(READTHROUGH_CACHE_DIR):
            os.mkdir(READTHROUGH_CACHE_DIR)
        if os.path.exists(cache_name):
            return open(cache_name, mode=mode, encoding=encoding)
        
    assert 'a' not in mode
    assert '+' not in mode
    
    if "r" in mode:
        if cache:
            check_call(['aws','s3','cp','--quiet',path,cache_name])
            open(cache_name, mode=mode, encoding=encoding)
        p = Popen(['aws','s3','cp','--quiet',path,'-'],stdout=PIPE,encoding=encoding)
        return p.stdout

    elif "w" in mode:
        p = Popen(['aws','s3','cp','--quiet','-',path],stdin=PIPE,encoding=encoding)
        return p.stdin
    else:
        raise RuntimeError("invalid mode:{}".format(mode))


def s3exists(path):
    """Return True if the S3 file exists"""
    from subprocess import run,PIPE,Popen
    out = Popen(['aws','s3','ls','--page-size','10',path],stdout=PIPE,encoding='utf-8').communicate()[0]
    return len(out) > 0


if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Combine multiple files on Amazon S3 to the same file." )
    parser.add_argument("--ls", help="list a s3 prefix")
    parser.add_argument("--debug", action='store_true')
    args = parser.parse_args()
    if args.debug:
        debug=args.debug
    if args.ls:
        (bucket,prefix) = get_bucket_key(args.ls)
        for data in list_objects(bucket,prefix):
            print("{:18,} {}".format(data[_Size],data[_Key]))
    



