# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy.orm import deferred
from sqlalchemy.ext.declarative import declarative_base
from . import sphinxtypes as types
from .util import compose, concat


__all__ = ['unsigned', 'string', 'timestamp', 'float_', 'fulltext', 'multi',
           'boolean', 'create_index', 'create_converter',
           'literal', 'plain', 'collection', 'combine']


def declare_type__(type_, wrap=None):
    class declare_type_(object):
        @property
        def column(self):
            col = Column(type_, **self.kwargs)
            if wrap:
                return compose(*wrap)(col)
            return col

        def __call__(self, name, **kwargs):
            self.name, self.kwargs = name, kwargs
            return self
    return declare_type_()


def declare_type(type_, wrap=None):
    def declare_type_(name, **kwargs):
        class declare_type__(object):
            def __init__(self):
                self.name, self.kwargs = name, kwargs

            @property
            def column(self):
                col = Column(type_, **self.kwargs)
                if wrap:
                    return compose(*wrap)(col)
                return col
        return declare_type__()
    return declare_type_


boolean = declare_type(types.Boolean)
unsigned = declare_type(types.Unsigned)
string = declare_type(types.String)
timestamp = declare_type(types.Timestamp)
float_ = declare_type(types.Float)
fulltext = declare_type(types.Fulltext, wrap=[deferred])
multi = declare_type(types.Multi)


def create_index(name, *attrs):
    Base = declarative_base()
    class_name = '%sIndex' % name.title()
    values = dict(__tablename__=name, id=unsigned('id', primary_key=True).column)
    for attr in attrs:
        values[attr.name] = attr.column
    return type(class_name, (Base,), values)


def literal(value):
    return lambda model: value


def plain(field, default=None):
    def conv(model):
        value = getattr(model, field)
        if value is None:
            return default
        return value
    return conv


def collection(field, do,
               wrap_multi=False,
               from_self=False,
               with_parents=False,
               parent_field='parent'):
    # XXX: Avoid wrapping collections to be passed to mva fields
    def conv(model):
        if from_self:
            item = getattr(model, field)
            if item is not None:
                elems = [do(item)]
            else:
                elems = []
        else:
            elems = [do(obj) for obj in getattr(model, field) if obj is not None]

        if with_parents:
            #if from_self:
            #    item = getattr(model, field)
            #    if item is not None:
            #        for child in getattr(getattr(model, field), children_field):
            #            elems.append(do(child))
            #else:
            #    items = getattr(model, field)
            #    for item in items:
            #        for child in item.children:
            #            elems.append(do(child))

            if from_self:
                item = getattr(model, field)
                if item is not None:
                    parent = getattr(item, parent_field)
                    while parent:
                        elems.append(do(parent))
                        parent = getattr(parent, parent_field)
            else:
                items = getattr(model, field)
                for item in items:
                    parent = getattr(item, parent_field)
                    while parent:
                        elems.append(do(parent))
                        parent = getattr(parent, parent_field)

        if wrap_multi:
            return types.multi(elems)
        return elems
    return conv


def combine(*does):
    return lambda model: concat([do(model) for do in does])


def create_converter(**kwargs):
    return lambda model: {k: v(model) for k, v in kwargs.iteritems()}
