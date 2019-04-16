import certificate
import py.test
import datetime
import time

def disabled_test_make_certificate():
    """Generate a certificate"""
    c = certificate.CertificatePrinter(title="Test Certificate")
    c.add_params({"@@NAME@@":"A very precise data set",
                  "@@DATE@@":datetime.datetime.now().isoformat()[0:19],
                  "@@PERSON1@@": "Ben Bitdiddle",
                  "@@TITLE1@@" : "Novice Programmer",
                  "@@PERSON2@@": "Alyssa P. Hacker",
                  "@@TITLE2@@" : "Supervisor"})
    c.typeset("certificate_demo.pdf")

if __name__=="__main__":
    disabled_test_make_certificate()

    

                          

    
