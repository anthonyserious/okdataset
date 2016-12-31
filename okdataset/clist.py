from itertools import groupby, chain, imap

"""
List with chainable function methods.
"""
class ChainableList(list):
    """
    ChainableList is a list subclass providing chainable functional methods, effectively
    a subset of the PySpark API.
    """
    def __init__(self, l=[]):
        """
        >>> ChainableList()
        []
        >>> ChainableList([1, 2, 3])
        [1, 2, 3]
        """
        list.__init__(self,l)

    def map(self, f):
        """
        >>> ChainableList([1, 2, 3]).map(lambda x: x + 1)
        [2, 3, 4]
        """
        return ChainableList(map(f, self[:]))

    def flatMap(self, f):
        """
        >>> def fm(x):
        ...     for i in x[1]:
        ...             yield (x[0], i)
        ...
        >>> ChainableList([ (1, (1,2,3)), (2, (1,2,3)), (3, (1,2,3)) ]).flatMap(fm)
        [(1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (2, 3), (3, 1), (3, 2), (3, 3)]
        """
        return ChainableList(chain.from_iterable(imap(f, self)))

    def filter(self, f):
        """
        >>> ChainableList([1, 2, 3]).filter(lambda x: x % 2 == 1)
        [1, 3]
        """
        return ChainableList(filter(f, self[:]))

    def reduce(self, f):
        """
        >>> ChainableList([1, 2, 3]).reduce(lambda x, y: x + y)
        6
        """
        return reduce(f, self[:])

    def head(self, n=5):
        """
        >>> ChainableList(range(0,10)).head()
        [0, 1, 2, 3, 4]
        >>> ChainableList(range(0,10)).head(n=2)
        [0, 1]
        """
        return ChainableList(self[:n])

    def tail(self, n=5):
        """
        >>> ChainableList(range(0,10)).tail()
        [5, 6, 7, 8, 9]
        >>> ChainableList(range(0,10)).tail(n=2)
        [8, 9]
        """
        return ChainableList(self[-n:])

    def dict(self, f):
        """
        >>> res = ChainableList(range(0,10)).dict(lambda x: ("key%d" % x, "val%d" % x))
        >>> res["key5"] == "val5"
        True
        """
        return dict(f(x) for x in self[:])

    def join(self, right, leftKey=None, rightKey=None):
        """
        >>> l1 = ChainableList([ (1, [ 1 ]), (1, [ 2 ]), (2, [ 3 ]), (3, [ 4 ]) ])
        >>> l2 = ChainableList([ (1, [ 3 ]), (1, [ 2 ]), (3, [ 3 ]) ])
        >>> l1.join(l2)
        [(1, [1, 3]), (1, [2, 3]), (1, [1, 2]), (1, [2, 2]), (3, [4, 3])]
        """
        hash = {}
        res = ChainableList([])

        # assume each item is a tuple/list
        if leftKey is None:
            for item in self:
                if item[0] not in hash:
                    hash[item[0]] = []

                hash[item[0]].append(item[1])
        else:
            for item in self:
                key = leftKey(item)

                if key not in hash:
                    hash[key] = []

                hash[key].append(item)

        if rightKey is None:
            for item in right:
                for x in hash.get(item[0], []):
                    if type(x) is dict:
                        z = x.copy()
                        z.update(item)
                        res.append(item[0], z)
                    else:
                        res.append((item[0], x + item[1]))
        else:
            for item in right:
                key = rightKey(item)

                for x in hash.get(key, []):
                    if type(x) is dict:
                        z = x.copy()
                        z.update(item)
                        res.append(z)
                    else:
                        res.append((key, x + item))

        return res

    def zip(self, other):
        """
        Returns a ChainableList [ [ self1, other1 ], [ self2, other2 ], ... ]
        """
        if len(self) != len(other):
            raise ValueError("self and other lists must be the same length")

        res = ChainableList([])

        for i in xrange(0, len(self)):
            res.append([ self[i], other[i] ])

        return res

    # list items must be tuples of the form (key, ChainableList(values))
    def reduceByKey(self, f):
        groups = ChainableList([ (key, ChainableList(group)) for key, group in groupby(sorted(self), lambda x: x[0])])\
            .map(lambda (key, items): (key, items.map(lambda x: x[1])))

        res = ChainableList([])

        for key, values in groups:
            res.append((key, reduce(f, values)))

        return res

if __name__ == "__main__":
    import doctest, sys

    sys.exit(doctest.testmod()[0])

