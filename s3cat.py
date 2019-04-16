#!/usr/bin/env Python3

"""
s3_cat.py:

This program uses the AWS S3 API or local disk IO, as appropriate, to concatenate all of the files within a prefix into a single file.
The components are kept.

References:
https://docs.aws.amazon.com/cli/latest/userguide/using-s3-commands.html
https://aws.amazon.com/blogs/developer/efficient-amazon-s3-object-concatenation-using-the-aws-sdk-for-ruby/
https://docs.aws.amazon.com/cli/latest/reference/s3api/create-multipart-upload.html
https://docs.aws.amazon.com/cli/latest/reference/s3api/upload-part-copy.html
https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPartCopy.html
https://docs.aws.amazon.com/cli/latest/reference/s3api/head-object.html
Note- minimum part size is 5MB

"""

import subprocess
import json
import urllib
import urllib.parse
import tempfile
import os
import sys
from pathlib import Path


BUCKET='uscb-decennial-ite-das'
DEST_FILE='slg2/all'
SOURCE1='/uscb-decennial-ite-das/slg/part01'
SOURCE2='/uscb-decennial-ite-das/slg/part02'
MIN_MULTIPART_COMBINE_OBJECT_SIZE = 1024*1024*5 # Amazon says it is 5MB

def aws_s3api(cmd):
    fcmd = ['aws','s3api','--output=json'] + cmd
    # print(" ".join(fcmd),file=sys.stderr)
    data = subprocess.check_output(fcmd, encoding='utf-8')
    try:
        return json.loads(data)
    except (TypeError,json.decoder.JSONDecodeError) as e:
        raise RuntimeError("s3 api {} failed data: {}".format(cmd,data))

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
    
def concat_downloaded_objects(obj1, obj2, delete_obj2=True):
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
    if delete_obj2:
        os.unlink(obj2['fname']) # remove the second file
    return


def concat_on_local_disk(loc, demand_success: bool = False, metadata: bool = True):
    """Concatenate from a local disk source
    :param: loc - directory containing parts to concatenate
    :param: demand_success - _SUCCESS file must be present at loc
    :param: metadata - roll metadata file into concatenated result"""
    p = Path(loc)

    if not p.is_dir():
        # Looking to concat files within a directory, so loc must be a directory
        return

    # Create .dat file outside of directory with same stem name
    output_path = Path(str(p.with_suffix('')) + '.dat')

    if demand_success:
        # Search for _SUCCESS file to indicate the run was successful before combining files
        if len(list(p.glob("_SUCCESS"))) <= 0:
            # if _SUCCESS not found, and we demand it, then return without doing anything
            return

    # Remove output file if it currently exists
    if output_path.is_file():
        output_path.unlink()

    # Initialize concat object
    output_path.touch()
    output_file = {'fname': str(output_path.absolute()), 'Size': 0}
    print(output_file['fname'])

    if metadata:
        # Concatenate metadata if it exists
        metadata_path = p / 'metadata'

        if metadata_path.is_file():
            metadata_file = {'fname': str(metadata_path.absolute()), 'Size': metadata_path.stat().st_size}

            concat_downloaded_objects(output_file, metadata_file, False)

    # For every
    for file_name in list(p.glob('part*')):
        if file_name.is_dir():
            # Ignore directories
            continue

        next_file = {'fname': str(file_name.absolute()), 'Size': file_name.stat().st_size}

        # Concatenate files, but do not delete next_file
        concat_downloaded_objects(output_file, next_file, False)


def s3_cat(loc,demand_success=False,metadata=True):
    # Check to see if the location given is a local drive -- if so, skip the downoad and just directly concatenate the files
    if Path(loc).is_dir():
        concat_on_local_disk(loc, demand_success, metadata)
        return

    with tempfile.TemporaryDirectory() as TMP_DIR:
        (bucket, key) = get_bucket_key(loc)

        # print("Temporary directory: {}".format(TMP_DIR))

        # Download all of the objects that are too small. 
        # When we download a run, concatenate them.
        # This could be more efficient if we concatentated on download.
        run = None
        nobjs = []                  # the new list, after the small ones are deleted
        total_bytes = 0
        found_success = False
        total_objects = 0

        # List the objects. This cannot be made multi-threaded

        print("Listing objects...")
        s3objs = list( list_objects(bucket, key) )

        # Get the size of each object and download the small ones.
        for (counter,obj) in enumerate(s3objs,1):

            total_bytes   += obj['Size']
            if obj['Size']==0:
                if obj['Key'].endswith("/_SUCCESS"):
                    found_success = True
                continue        # ignore zero length

            if obj['Size'] < MIN_MULTIPART_COMBINE_OBJECT_SIZE:
                print("Object {} size {} < MIN_MULTIPART_COMBINE_OBJECT_SIZE {}; downloading...".
                      format(counter,obj['Size'],MIN_MULTIPART_COMBINE_OBJECT_SIZE),flush=True)
                download_object(TMP_DIR, bucket, obj)
                if run:
                    concat_downloaded_objects(run, obj)
                    continue        # dont process this object anymore
                else:
                    run = obj   # start of a new run
            else:
                run = None          # no longer a run
            nobjs.append(obj)
        assert sum_object_sizes(nobjs)==total_bytes # make sure nothing was lost

        if demand_success and not found_success:
            print("No _SUCCESS in {}".format(loc),file=sys.stderr)
            exit(1)

        # Now all of the runs have been collapsed. If any of the objects
        # are still too small, we will need to download the previous "big enough" object and combine them.
        # If there is no previous "big enough" object, then we download the next object and combine them
        prev_big_enough = None
        prepend_object  = None
        parts = []
        for obj in nobjs:
            if obj['Size'] < MIN_MULTIPART_COMBINE_OBJECT_SIZE:
                assert obj['fname'] >'' # make sure that this was downloaded
                # If there is a previous object, download it and combine the current object with the previous
                if prev_big_enough:
                    download_object(TMP_DIR, bucket, prev_big_enough) # make sure it is downloaded
                    download_object(TMP_DIR, bucket, obj)
                    concat_downloaded_objects(prev_big_enough,obj)
                    continue        # don't process this object anymore; prev is already in nobjs
                # There was no previous object. Remember this object as the prepend object
                assert prepend_object==None # there should be no prepend object at the moment
                prepend_object=obj
                continue
            if prepend_object:
                # Even though object is big enough, we need to download it and append it to the prepend_object
                download_object(TMP_DIR, bucket, obj)
                concat_downloaded_objects(prepend_object, obj)
                obj = prepend_object
                prepend_object = None
            prev_big_enough = obj # the current object is now big enough to be prepended to
            parts.append(obj)

        # If all of the objects were too small together, then there is nothing in parts and they are
        # all in prepend_object. Process it.
        if prepend_object:
            assert len(parts)==0
            assert prepend_object['Size'] == total_bytes
            # Just upload the single object, and we're done.
            put_object(bucket,key,prepend_object['fname'])
            return

        # IF we got here, there should not have been a prepend_object
        assert total_bytes == sum_object_sizes(parts)    # Make sure we didn't lose anybody
        assert prepend_object == None # make sure that nothing is waiting to be prepended

        # Now we can multipart upload!
        # Some objects will need to be uploaded, others are already uploaded

        upload    = aws_s3api(['create-multipart-upload','--bucket',bucket,'--key',key])
        upload_id = upload['UploadId']

        # Now use upload-part or upload-part-copy for each part
        mpustruct = {"Parts":[]}
        mpargs = ['--bucket',bucket,'--key',key,'--upload-id',upload_id]
        for (part_number,obj) in enumerate(parts,1):
            args = mpargs + ['--part-number',str(part_number)]

            if 'fname' in obj:
                # This is on disk, so we need to use upload-part
                cpr = aws_s3api(['upload-part']+args+['--body',obj['fname']])
                mpustruct['Parts'].append({"PartNumber":part_number,"ETag":etag(cpr)})
            else:
                # Not on disk, so just combine the part
                cpr = aws_s3api(['upload-part-copy'] + args + ['--copy-source',bucket+"/"+obj['Key']])
                mpustruct['Parts'].append({"PartNumber":part_number,"ETag":etag(cpr['CopyPartResult'])})

        # Complete the transaction
        aws_s3api(['complete-multipart-upload'] + mpargs + ['--multipart-upload', json.dumps(mpustruct)])

if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Combine multiple files on Amazon S3 to the same file." )
    parser.add_argument("--demand_success", action='store_true', help="require that a _SUCCESS part exists")
    parser.add_argument("prefix", help="Amazon S3 prefix, include s3://")
    parser.add_argument("--metadata", action="store_true", help="Include metadata file in concatenation")
    args = parser.parse_args()
    s3_cat(args.prefix)
    


