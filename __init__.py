import re
from sqlalchemy import func
from sqlalchemy.sql.expression import ClauseElement, _literal_as_text
from sqlalchemy.sql.visitors import replacement_traverse
from sqlalchemy.orm import Query, class_mapper
from sqlalchemy.orm.base import manager_of_class, _generative
from sqlalchemy.dialects import registry

registry.register('sphinxql', 'sphinxqla.dialect', 'Dialect')


def escape_match(value):
    return re.sub(r'([=\(\)|\-!@~"&/\\\^\$=])', r'\\\1', value)


class MatchClause(ClauseElement):
    __visit_name__ = 'match'

    def __init__(self, value):
        self.value = value


class SphinxQuery(Query):

    def actual_count(self):
        from itertools import chain
        from sqlalchemy import sql
        from sqlalchemy.orm.query import _MapperEntity

        should_nest = [self._should_nest_selectable]

        def ent_cols(ent):
            if isinstance(ent, _MapperEntity):
                return ent.mapper.primary_key
            else:
                should_nest[0] = True
                return [ent.column]

        return self._col_aggregate(
            sql.literal_column('*'),
            sql.func.count,
            nested_cols=chain(*[ent_cols(ent) for ent in self._entities]),
            should_nest=should_nest[0]
        ) or 0

    def count(self):
        self._clone().limit(0).all()
        meta = self.session.execute('SHOW META').fetchone()
        if meta and len(meta) == 2 and meta[0] == 'total':
            return int(meta[1])
        return 0

    @_generative(Query._no_statement_condition, Query._no_limit_offset)
    def match(self, **kwargs):
        expression = ' '.join([
            u'@{} {}'.format(field, escape_match(text))
            for field, text in kwargs.iteritems()
        ])
        self.add_match(expression)

    @_generative(Query._no_statement_condition, Query._no_limit_offset)
    def match_expression(self, expression, *args, **kwargs):
        escaped_args = tuple(v for v in args)
        escaped_kwargs = {k: escape_match(v) for k, v in kwargs.iteritems()}
        expression = expression.format(*escaped_args, **escaped_kwargs)
        self.add_match(expression)

    def add_match(self, value):
        def replace(node):
            if isinstance(node, MatchClause):
                return MatchClause(' '.join([node.value, value]))
            return node

        if self._criterion is not None:
            self._criterion = replacement_traverse(self._criterion, {}, replace)
        else:
            criterion = _literal_as_text(MatchClause(value))
            self._criterion = self._adapt_clause(criterion, True, True)


class BulkIdProxy(object):

    def __init__(self, session, identities, cls, key=None):
        self._session = session
        self._cls = cls
        self._identities = identities

        if isinstance(key, basestring):
            manager = manager_of_class(cls)
            self._key = manager[key]
        elif key is None:
            mapper = class_mapper(cls)
            primary_keys = mapper.primary_key
            assert len(primary_keys) == 1
            self._key = primary_keys[0]
        else:
            self._key = key

    def count(self):
        return len(self._identities)

    def __getitem__(self, item):
        keys = self._identities[item]
        return self._session.query(self._cls).filter(self._key.in_(keys)).all()
