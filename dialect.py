from sqlalchemy import exc
from sqlalchemy.connectors.mysqldb import MySQLDBConnector
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql import compiler, expression, elements, functions


class match(functions.ReturnTypeFromArgs):
    name = 'MATCH'


class SQLCompiler(compiler.SQLCompiler):

    def visit_column(self, column, result_map=None, **kwargs):
        name = column.name

        if name is None:
            raise exc.CompileError(
                'Cannot compile Column object until it\'s "name" is assigned.'
            )

        is_literal = column.is_literal
        if not is_literal and isinstance(name, elements._truncated_label):
            name = self._truncated_identifier("colident", name)

        #if result_map is not None:
        #    result_map[name.lower()] = (name, (column, ), column.type)

        if is_literal:
            name = self.escape_literal_column(name)
        else:
            name = self.preparer.quote(name, column.name)

        return name

    def visit_select(self, select,
                     asfrom=False,
                     parens=True,
                     iswrapper=False,
                     fromhints=None,
                     compound_index=1,
                     force_result_map=False,
                     positional_name=None, **kwargs):

        entry = self.stack and self.stack[-1] or {}

        existingfroms = entry.get('from', None)

        froms = select._get_display_froms(existingfroms)

        correlate_froms = set(expression._from_objects(*froms))

        # TODO: might want to propagate existing froms for
        # select(select(select)) where innermost select should correlate
        # to outermost if existingfroms: correlate_froms =
        # correlate_froms.union(existingfroms)

        self.stack.append({'from': correlate_froms,
                           'iswrapper': iswrapper})

        if compound_index == 1 and not entry or entry.get('iswrapper', False):
            column_clause_args = {'result_map': self.result_map}
        else:
            column_clause_args = {}

        populate_result_map = force_result_map or (
            compound_index == 0 and (not entry or entry.get('iswrapper', False)))

        # the actual list of columns to print in the SELECT column list.
        inner_columns = [
            c for c in [
                self._label_select_column(select,
                                          column,
                                          populate_result_map,
                                          asfrom,
                                          column_clause_args,
                                          name=name)
                for name, column in select._columns_plus_names]
            if c is not None
        ]

        text = 'SELECT '

        if select._hints:
            byfrom = dict([
                            (from_, hinttext % {
                                'name':from_._compiler_dispatch(
                                    self, ashint=True)
                            })
                            for (from_, dialect), hinttext in
                            select._hints.iteritems()
                            if dialect in ('*', self.dialect.name)
                        ])
            hint_text = self.get_select_hint_text(byfrom)
            if hint_text:
                text += hint_text + ' '

        if select._prefixes:
            text += ' '.join(
                            x._compiler_dispatch(self, **kwargs)
                            for x in select._prefixes) + " "
        text += self.get_select_precolumns(select)
        text += ', '.join(inner_columns)

        if froms:
            text += ' \nFROM '

            if select._hints:
                text += ', '.join([f._compiler_dispatch(self,
                                    asfrom=True, fromhints=byfrom,
                                    **kwargs)
                                for f in froms])
            else:
                text += ', '.join([f._compiler_dispatch(self,
                                    asfrom=True, **kwargs)
                                for f in froms])
        else:
            text += self.default_from()

        if select._whereclause is not None:
            t = select._whereclause._compiler_dispatch(self, **kwargs)
            if t:
                text += ' \nWHERE ' + t

        if select._group_by_clause.clauses:
            group_by = select._group_by_clause._compiler_dispatch(
                                        self, **kwargs)
            if group_by:
                text += ' GROUP BY ' + group_by

        if select._having is not None:
            t = select._having._compiler_dispatch(self, **kwargs)
            if t:
                text += ' \nHAVING ' + t

        if select._order_by_clause.clauses:
            text += self.order_by_clause(select, **kwargs)
        if getattr(select, '_within_group_order_by_clause', None) is not None:
            if select._within_group_order_by_clause.clauses:
                text += self.within_group_order_by_clause(select, **kwargs)
        if select._limit is not None:
            text += self.limit_clause(select)
        if getattr(select, '_options', None) is not None:
            if select._options.options:
                text += self.options_clause(select, **kwargs)
        if select.for_update:
            text += self.for_update_clause(select)

        self.stack.pop(-1)

        if asfrom and parens:
            return "(" + text + ")"
        else:
            return text

    def limit_clause(self, select):
        limit, offset = select._limit, select._offset
        if limit:
            if offset:
                return ' \nLIMIT %s, %s' % (
                    self.process(expression.literal(offset)),
                    self.process(expression.literal(limit)))
            return ' \nLIMIT %s' % self.process(expression.literal(limit))
        return ''

    def visit_multi(self, element, **kwargs):
        return '(%s)' % self.visit_clauselist(element, **kwargs)

    def visit_match(self, element, **kwargs):
        return 'match(%s)' % self.process(expression.literal(element.value))


class Dialect(DefaultDialect, MySQLDBConnector):
    name = 'sphinxql'
    default_paramstyle = 'format'
    positional = True
    ddl_compiler = None
    statement_compiler = SQLCompiler
    supports_unicode_statements = True
    supports_multivalues_insert = True
    supports_right_nested_joins = False
    supports_alter = False
    supports_views = False
    description_encoding = None

    def _check_unicode_returns(self, connection, additional_tests=None):
        return True
