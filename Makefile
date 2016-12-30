prefix=okdataset

testFiles=\
  $(prefix)/clist.py

all:
	pip install -r requirements.txt

test:
	for i in $(testFiles); do python $$i; done

