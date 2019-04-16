ALL = cert_demo.pdf bashful_demo.pdf

all: $(ALL)

cert_demo.pdf:
	latexmk -pdf cert_demo.tex

bashful_demo.pdf: bashful_demo.tex
	latexmk -pdf -shell-escape bashful_demo.tex

clean:
	/bin/rm -f *.{bak,aux,log,fls,sh,stderr,stdout,fdb_latexmk}
	/bin/rm -f *~
	/bin/rm -f $(ALL)

