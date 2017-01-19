# -*- coding: utf-8 -*-

"""Manages scan results in a database."""

from itertools import islice

from sqlalchemy import MetaData, Table, Column, Integer, String, create_engine
from sqlalchemy.orm import mapper, sessionmaker
from sqlalchemy.engine import reflection

from .aggregating_scanner import scan, Directory


DEFAULT_BATCH_SIZE = 5


class SqlABackend(object):
    """A backend for storing directory metrics using SQLAlchemy."""
    def __init__(self, sql_url, **kwds):
        super().__init__(**kwds)
        self._sql_url = sql_url
        self._metadata = MetaData()
        self._define_schema()
        self._engine = create_engine(sql_url, echo=False)
        self._insp = reflection.Inspector.from_engine(self._engine)
        self.ensure_schema()
        self.Session = sessionmaker(bind=self._engine)
        self._session = self.Session()

    def _define_schema(self):
        self._directories = Table(
            'directories', self._metadata,
            Column('path', String, primary_key=True),
            Column('parent', String, index=True),
            Column('max_atime', Integer, default=0),
            Column('max_ctime', Integer, default=0),
            Column('max_mtime', Integer, default=0),
            Column('num_blocks', Integer, default=0),
            Column('num_bytes', Integer, default=0),
            Column('num_files', Integer, default=0),
            Column('num_dirs', Integer, default=0),
            Column('num_symlinks', Integer, default=0),
            Column('num_specials', Integer, default=0),
            Column('num_multi_links', Integer, default=0)
        )
        self._directory_mapper = mapper(Directory, self._directories)

    def ensure_schema(self):
        self._metadata.create_all(self._engine)
        if 'dirs' not in self._insp.get_view_names():
            self._engine.execute("""
                create view dirs as
                select
                    path,
                    parent,
                    datetime(max_atime, 'unixepoch') as max_atime,
                    datetime(max_ctime, 'unixepoch') as max_ctime,
                    datetime(max_mtime, 'unixepoch') as max_mtime,
                    num_blocks,
                    num_bytes,
                    num_files,
                    num_dirs,
                    num_symlinks,
                    num_specials,
                    num_multi_links
                from directories
            """)

    def clear_schema(self):
        """Database go POOF!"""
        if 'dirs' in self._insp.get_view_names():
            self._engine.execute('drop view dirs')
        if 'directories' in self._insp.get_table_names():
            self._engine.execute('drop table directories')

    def add_tree(self, top, batch_size=DEFAULT_BATCH_SIZE):
        gen = scan(top)
        try:
            while True:
                batch = list(islice(gen, batch_size))
                if batch:
                    self._session.add_all(batch)
                    self._session.commit()
                    # The top will be the last object in the last
                    # non-empty batch.
                    save = batch[-1]
                else:
                    break
            assert save.path == top, (save.path, top)
        except Exception as e:
            self._session.rollback()
            raise e
        return save

    def delete_tree(self, top):
        try:
            delete_children = self._directories.delete(
                self._directories.c.path.like(top + '/%')
            )
            delete_parent = self._directories.delete(
                self._directories.c.path == top
            )
            self._session.execute(delete_children)
            self._session.execute(delete_parent)
            self._session.commit()
        except Exception as e:
            self._session.rollback()
            raise e

    def query(self):
        """Return an SQLAlchemy Query object."""
        return self._session.query(Directory)

    @property
    def session(self):
        return self._session

    def close(self):
        self._session.close()
        self._engine.dispose()
