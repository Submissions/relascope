# -*- coding: utf-8 -*-

"""Manages scan results in a database."""

from itertools import islice

from sqlalchemy import MetaData, Table, Column, Integer, String, create_engine
from sqlalchemy.orm import mapper, sessionmaker
from sqlalchemy.engine import reflection

from .aggregating_scanner import Directory, ATTRIBUTES


DEFAULT_BATCH_SIZE = 1000


class SqlABackend(object):
    """A backend for storing directory metrics using SQLAlchemy. Is a
    context manager."""
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

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        """Just invokes `close` method."""
        self.close()
        return False

    def _define_schema(self):
        attributes = [
            Column(name, Integer, default=default)
            for name, default in ATTRIBUTES
        ]
        self._directories = Table(
            'directories', self._metadata,
            Column('path', String, primary_key=True),
            Column('parent', String, index=True),
            *attributes
        )
        self._directory_mapper = mapper(Directory, self._directories)

    def hard_reset(self):
        """Delete and re-create schema."""
        self.clear_schema()
        self.ensure_schema()

    def ensure_schema(self):
        self._metadata.create_all(self._engine)
        if 'dirs' not in self._insp.get_view_names():
            self._engine.execute("""
                create view dirs as
                select
                    num_blocks / 2147483648 as tb,
                    num_blocks / 2097152 as gb,
                    path,
                    parent,
                    depth,
                    max_depth,
                    datetime(scan_started, 'unixepoch') as scan_started,
                    datetime(scan_finished, 'unixepoch') as scan_finished,
                    datetime(last_updated, 'unixepoch') as last_updated,
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
        gen = Directory(top).scan()
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
