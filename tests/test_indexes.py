from unittest import TestCase
from sqlalchemy import Column
from sqlalchemy.orm import ColumnProperty
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.ext.declarative import DeclarativeMeta
from ..indexes import *
from ..sphinxtypes import multi as multi_expr


class TestOrm(TestCase):

    def test_type_factory(self):
        obj = unsigned('test')
        self.assertTrue(obj.name, 'test')
        self.assertTrue(isinstance(obj.column, Column))

    def test_type_factory_wrap(self):
        obj = fulltext('test')
        self.assertTrue(obj.name, 'test')
        self.assertTrue(isinstance(obj.column, ColumnProperty))

    def test_create_index(self):
        TestIndex = create_index('tests', unsigned('attr'), fulltext('fts'))
        self.assertTrue(isinstance(TestIndex, DeclarativeMeta))
        self.assertTrue(isinstance(TestIndex.id, InstrumentedAttribute))
        self.assertTrue(isinstance(TestIndex.fts, InstrumentedAttribute))
        self.assertTrue(TestIndex.__tablename__, 'tests')


class TestConverter(TestCase):

    class MockModel(object):
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    def setUp(self):
        self.document = self.MockModel(id=1337, title=str('1337'), photos=None,
                                       contains_true=True, contains_false=False)
        self.document.photos = [self.MockModel(id=i, title=str(i)) for i in range(5)]

    def test_literal(self):
        convert = create_converter(public=literal(1))
        self.assertEqual(convert(self.document), {'public': 1})

    def test_plain(self):
        convert = create_converter(id=plain('id'), title=plain('title'))
        self.assertEqual(convert(self.document), {'id': 1337, 'title': '1337'})

    def test_collection(self):
        convert = create_converter(photo_ids=collection('photos', plain('id')))
        self.assertEqual(convert(self.document), {'photo_ids': [0,1,2,3,4]})

    def test_collection_wrap_multi(self):
        convert = create_converter(photo_ids=collection('photos', plain('id'),
                                                        wrap_multi=True))
        self.assertTrue(isinstance(convert(self.document)['photo_ids'],
                                   multi_expr))

    def test_combine(self):
        convert = create_converter(photo_titles=combine(
            collection('photos', plain('title'))))
        self.assertEqual(convert(self.document), {'photo_titles': '0 1 2 3 4'})

    def test_bool2int(self):
        convert = create_converter(contains_true=bool2int('contains_true'),
                                   contains_false=bool2int('contains_false'))
        self.assertEqual(convert(self.document), {'contains_true': 1,
                                                  'contains_false': 0})
