import zipfile

class VerboseFile:
    """Like a real file, but prints what's happening."""
    def __init__(self,name):
        self.name = name
        self.fp  = open(name,"rb")

    def __repr__(self):
        return "VerboseFile<name:{} fp:{}>".format(self.name,self.fp)

    def __enter__(self):
        return self

    def __exit__(self,a,b,c):
        return

    def read(self,len=-1):
        print("read({})".format(len))
        return self.fp.read(len)

    def seek(self,offset,whence):
        print("will seek {},{}".format(offset,whence))
        try:
            r = self.fp.seek(offset,whence)
        except Exception as e:
            print("Exception: ",e)
        print("seek({},{})={}".format(offset,whence,r))
        return r

    def tell(self):
        r = self.fp.tell()
        print("tell()={}".format(r))
        return r

    def write(self):
        raise RuntimeError("Write not supported")

    def flush(self):
        raise RuntimeError("Flush not supported")

    def close(self):
        print("closed")
        return self.fp.close()


# Try with
# aws s3api get-object --bucket my_s3_bucket --key s3_folder/file.txt --range bytes=0-1000000 tmp_file.txt && head tmp_file.txt

if __name__=="__main__":
    import argparse
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("zipfile",help="A file that you want to test")
    args = parser.parse_args()

    with VerboseFile(args.zipfile) as vf:
        with zipfile.ZipFile(vf, mode='r', allowZip64=True) as zf:
            print("name list:",zf.namelist())
    
