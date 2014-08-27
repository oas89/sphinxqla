
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