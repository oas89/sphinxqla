from unittest import TestCase
from sqlalchemy import util
from ..util import *


class MockFake(int):
    id = property(lambda self: self)


class MockReal(int):
    pass


class QueryMock(object):
    def __init__(self, total):
        self.total = total
        self.objs = [MockFake(i) for i in range(total)]

    def count(self):
        return self.total

    def __getitem__(self, item):
        if isinstance(item, slice):
            start, stop, step = util.decode_slice(item)
            if isinstance(stop, int) and \
               isinstance(start, int) and \
               stop - start <= 0:
                return []
            elif (isinstance(start, int) and start < 0) or\
                 (isinstance(stop, int) and stop < 0):
                return self.objs[item]
            res = self.objs[start:stop]
            if step is not None:
                return list(res)[None:None:item.step]
            else:
                return list(res)
        else:
            if item == -1:
                return self.objs[-1]
            else:
                return list(self[item:item + 1])[0]


def mock_loader(ids):
    if not isinstance(ids, (list, tuple)):
        return MockReal(ids)
    else:
        return [MockReal(i) for i in ids]


class TestProxyLoader(TestCase):

    def setUp(self):
        self.query = QueryMock(100)
        self.proxy = Proxy(self.query, mock_loader)

    def test_count(self):
        self.assertEqual(self.proxy.count(), self.query.count())

    def test_get(self):
        self.assertTrue(isinstance(self.proxy[1], MockReal))
        self.assertEqual(self.proxy[1], 1)
        self.assertEqual(self.proxy[-1], 99)

    def test_slice(self):
        self.assertEqual(self.proxy[:1], range(100)[:1])
        self.assertEqual(self.proxy[:-1], range(100)[:-1])
        self.assertEqual(self.proxy[1:10], range(100)[1:10])
        self.assertEqual(self.proxy[10:1], [])


class TestHelpers(TestCase):

    def test_flatten(self):
        test = ['a', ['b', ['c', 'd', ['e'], 'f'], ['g']]]
        self.assertEqual(list(flatten(test)), ['a', 'b', 'c', 'd', 'e', 'f', 'g'])

    def test_concat(self):
        test = ['a', ['b', ['c']], 'd']
        self.assertEqual(concat(test), 'a b c d')

    def test_compose(self):
        f = lambda x: 0
        g = lambda x: 1
        fg = compose(f, g)
        gf = compose(g, f)
        identity = lambda x: x
        self.assertEqual(compose(identity)(1), 1)
        self.assertEqual(fg(1), 0)
        self.assertEqual(gf(1), 1)
