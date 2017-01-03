prefix=okdataset

testFiles=\
  $(prefix)/clist.py \
  $(prefix)/master.py

all:
	pip install -r requirements.txt

test:
	x=0; for i in $(testFiles); do env PYTHONPATH=$$PYTHONPATH:. python $$i; x=$$?; [ $$x -ne 0 ] && break; done; exit $$x

