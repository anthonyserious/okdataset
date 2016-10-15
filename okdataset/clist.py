from itertools import groupby, chain, imap

"""
List with chainable function methods.
"""
class ChainableList(list):
    def __init__(self, l):
        list.__init__(self,l)

    def map(self, f):
        return ChainableList(map(f, self[:]))

    def flatMap(self, f):
        return ChainableList(chain.from_iterable(imap(f, self)))

    def filter(self, f):
        return ChainableList(filter(f, self[:]))

    def reduce(self, f):
        return reduce(f, self[:])

    def head(n=5):
        return ChainableList(self[:n])

    def tail(n=5):
        return ChainableList(self[:n])

    def dict(f):
        return dict(f(x) for x in self[:])

    # list items must be tuples of the form (key, ChainableList(values))
    def reduceByKey(self, f):
        groups = ChainableList([ (key, ChainableList(group)) for key, group in groupby(sorted(self), lambda x: x[0])])\
            .map(lambda (key, items): (key, items.map(lambda x: x[1])))

        res = ChainableList([])

        for key, values in groups:
            print(values)
            res.append((key, reduce(f, values)))

        return res

