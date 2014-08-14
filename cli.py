import logging
import subprocess
from iktomi.cli.base import Cli
from . import sphinx_sessionmaker
from models.indexes import *
from models.front import *


logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)


class Fulltext(Cli):
    def __init__(self, db=None, cfg=None):
        self.db = db
        self.sphinx = sphinx_sessionmaker(cfg['uri'])()
        self.sphinx_config = cfg['config']
        self.sphinx_cmd_prefix = ['searchd', '-c', self.sphinx_config]

    def command_stop(self):
        if self.sphinx_config:
            return subprocess.call(self.sphinx_cmd_prefix + ['--stop'])
        print 'SPHINX[\'config\'] is not set'

    def command_start(self):
        if self.sphinx_config:
            return subprocess.call(self.sphinx_cmd_prefix)
        print 'SPHINX[\'config\'] is not set'

    def command_restart(self, purge=None):
        self.command_stop()
        self.command_start()

    def command_rebuild(self):
        print 'Rebuilding russian indices:'
        self._rebuild(DocRu, DocIndexRu, doc_converter)
        self._rebuild(EventRu, EventIndexRu, event_converter)

        print 'Rebuilding english indices:'
        self._rebuild(DocEn, DocIndexEn, doc_converter)
        self._rebuild(EventEn, EventIndexEn, event_converter)

    def _rebuild(self, model_cls, index_cls, convert):
        print 'Rebuilding index for %s' % model_cls.__name__
        print '\tDeleting existing entries...'
        query = self.sphinx.query(index_cls)
        total = query.actual_count()

        for i in range(0, total, 100):
            objs = self.sphinx.query(index_cls)[:100]
            for obj in objs:
                self.sphinx.delete(obj)

        self.sphinx.commit()
        print '\tIndexing entries...'

        total = self.db.query(model_cls).count()

        processed = 0
        for i in range(0, total, 100):
            objs = self.db.query(model_cls)[i:i+100]
            processed += len(objs)
            print '\tprocessing %i object(s) from %i (%i%% complete)' % (
                processed, total, int(float(processed) / total * 100))
            index_objs = [index_cls(**convert(obj)) for obj in objs]
            self.sphinx.add_all(index_objs)
            self.sphinx.commit()

        print '\tIndexed: %i object(s)' % total

