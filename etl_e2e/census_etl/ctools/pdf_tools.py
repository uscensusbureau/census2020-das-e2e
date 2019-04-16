#
# pdf_tools
# Simson Garfinkel
# US Census Bureau

import os
import os.path
import sys
import zipfile
import subprocess
import time

DOC_MAGIC=b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"
DOCX_MAGIC=b"PK"
SOFFICE_DARWIN='/Applications/LibreOffice.app/Contents/MacOS/soffice'


def check_magic(filepath,magic):
    head = open(filepath,"rb").read(len(magic))
    if head!=magic:
        print("{} != {}".format(head,magic))
        return False
    return True

def valid_docx(fname):
    if check_magic(fname,DOCX_MAGIC)==False:
        return False
    # Make sure it's a valid ZIP file
    try:
        with zipfile.ZipFile(fname,"r") as zf:
            return True
    except zipfile.BadZipFile as e:
        return False

def valid_convertable_document_file(fname):
    if not os.path.exists(fname):
        return False            # doesn't exist

    (name,ext) = os.path.splitext(fname.lower())
    if ext=='.doc':
        if os.path.getsize(fname) % 512 !=0:
            return False
        return  check_magic(fname,DOC_MAGIC)
    if ext=='.docx':
        return valid_docx(fname)
    if ext=='.rtf':
        return True
    return False



# https://stackoverflow.com/questions/30481136/pywin32-save-docx-as-pdf
# https://stackoverflow.com/questions/6011115/doc-to-pdf-using-python
def quit_word():
    """Quit Microsoft Word"""
    if sys.platform=='win32':
        word = win32com.client.Dispatch("Word.Application")
        word.Quit()

def convert_document_to_pdf(infile):
    """Convert a .doc, .docx, or .rtf file to PDF. Converted file has the
    same pathname, except .docx and been changed to .pdf. Throws an
    exception if it can't convert.
    """
    import os.path
    if not os.path.exists(infile):
        raise FileNotFoundError(infile)

    outfile = os.path.splitext(infile)[0] + ".pdf"
    if os.path.exists(outfile):
        raise FileExistsError
    
    print("CONVERT {}".format(infile))
    print("    --> {}".format(outfile))
    import sys                  # this shouldn't be needed, but it is...?
    if sys.platform=='win32':
        try:
            import os,win32com.client,pywintypes
        except ModuleNotFoundError as e:
            print(e)
            print("Cannot convert {} --- please convert it manually".format(infile))
            raise e

        wdFormatPDF = 17
        in_file = os.path.abspath(infile)
        out_file = os.path.abspath(outfile)
        word = win32com.client.Dispatch("Word.Application")
        try:
            doc = word.Documents.Open(in_file)
            doc.SaveAs(out_file, FileFormat=wdFormatPDF)
            doc.Close()
            word.Quit()
        except pywintypes.com_error as e:
            print("")
            print("**** CANNOT CONVERT: {} *****".format(infile))
            print(str(e))
            print("")
            word.Quit()
            return None
        word.Quit()
        return outfile
    if sys.platform=='darwin':
        #
        # soffice --headless --convert-to pdf filename.doc
        # libreoffice --headless --convert-to pdf filename.doc

        if not os.path.exists(SOFFICE_DARWIN):
            print("{} not found.".format(SOFFICE_DARWIN),file=sys.stderr)
            raise RuntimeError("DOCX conversion on MacOS requires LibreOffice to be installed")
        outdir = os.path.dirname(infile)
        cmd = [SOFFICE_DARWIN,'--headless','--convert-to','pdf','--outdir',outdir,infile]
        r = subprocess.call(cmd)
        outfile = os.path.splitext(infile)[0] + ".pdf"
        #
        # Strangely, the file does not immediately appear. So wait until it does, and timeout after 5 seconds
        for i in range(500):
            if os.path.exists(outfile):
                return outfile
            time.sleep(.01)
        raise RuntimeError("{}: {} not created".format(" ".join(cmd),outfile))
    raise RuntimeError("unknown how to do PDF conversion on {}".format(sys.platform))

#
# soffice --headless --convert-to pdf filename.doc
# libreoffice --headless --convert-to pdf filename.doc
#    raise RuntimeError("Please manually convert {}".format(infile))

if __name__=="__main__":
    #
    # Command to test the conversion
    #
    import sys
    ofn = convert_doc_to_pdf(sys.argv[1])
    print("Converted {} to {}".format(sys.argv[1],ofn))
    exit(0)
    
