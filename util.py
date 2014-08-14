# -*- coding: utf-8 -*-
from itertools import chain


def flatten(items):
    """ Flatten nested lists/tuples """
    for item in items:
        if isinstance(item, (list, tuple)):
            items_ = flatten(item)
            for item_ in items_:
                yield item_
        else:
            yield item


def concat(*items):
    """ Concatenate text from nested lists """
    return ' '.join(flatten(items))


def compose(*functions):
    return reduce(lambda f, g: lambda x: f(g(x)), functions)


class ModelLoader(object):
    """ Load models in bulk from session by ids """

    def __init__(self, session, model, id='id'):
        self.session = session
        self.model = model
        self.id = getattr(model, id)

    def __call__(self, ids):
        query = self.session.query(self.model)
        if not isinstance(ids, (list, tuple)):
            if ids:
                return query.filter(self.id == ids).first()
            return None
        else:
            if ids:
                return query.filter(self.id.in_(ids)).all()
            return []


class Proxy(object):
    """ Populate objects from query using loader """

    def __init__(self, query, loader):
        self.query = query
        self.loader = loader

    def count(self):
        return self.query.count()

    def __getitem__(self, item):
        items = self.query.__getitem__(item)
        if not isinstance(items, (list, tuple)):
            return self.loader(items.id)
        else:
            ids = [item.id for item in items]
            return self.loader(ids)
