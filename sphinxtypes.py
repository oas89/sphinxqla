# -*- coding: utf-8 -*-
import time
from sqlalchemy import types, exc
from sqlalchemy.sql import expression


__all__ = ['String', 'Float', 'Fulltext', 'Unsigned', 'Timestamp', 'Multi',
           'Boolean', 'multi']


class Float(types.TypeDecorator):
    impl = types.Float

    def process_bind_param(self, value, dialect):
        if value is not None:
            return value
        return 0.0


class String(types.TypeDecorator):
    impl = types.String

    def process_bind_param(self, value, dialect):
        if value is not None:
            return value
        return u''


Fulltext = String


class Unsigned(types.TypeDecorator):
    impl = types.Integer

    def process_bind_param(self, value, dialect):
        if value is not None:
            if value < 0:
                raise exc.CompileError('Sphinx accepts only unsigned integers')
            return value
        return 0


class Timestamp(types.TypeDecorator):
    impl = types.Integer

    def process_bind_param(self, value, dialect):
        if value is not None:
            return int(time.mktime(value.timetuple()))
        return 0

    def process_result_value(self, value, dialect):
        if value:
            return value.fromtimestamp(value) if value else None
        return None


class Boolean(types.TypeDecorator):
    impl = types.Integer

    def process_bind_param(self, value, dialect):
        return int(value)

    def process_result_value(self, value, dialect):
        return bool(value)


class Multi(types.UserDefinedType):
    pass


class multi(expression.Tuple):
    __visit_name__ = 'multi'

    type = Multi

    def __init__(self, clauses, **kw):
        super(multi, self).__init__(*clauses, **kw)

    def self_group(self, against=None):
        return self
