from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Query
from sqlalchemy.dialects import registry
from .util import ModelLoader, Proxy

registry.register('sphinxql', 'common.fulltext.sphinxql', 'Dialect')


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

    def proxy(self, session, model_cls):
        loader = ModelLoader(session, model_cls)
        return Proxy(self, loader)


def sphinx_sessionmaker(urn, **kwargs):
    engine = create_engine(urn, pool_recycle=60)
    if not kwargs.get('query_cls', None):
        kwargs.update({'query_cls': SphinxQuery})
    return sessionmaker(bind=engine, **kwargs)
