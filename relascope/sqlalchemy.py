# -*- coding: utf-8 -*-

"""Manages scan results in a database."""

from itertools import islice
import logging
import os

from sqlalchemy import MetaData, Table, Column, Integer, String, create_engine
from sqlalchemy.engine import reflection
from sqlalchemy.orm import mapper, sessionmaker
from sqlalchemy.sql.expression import or_


from .aggregating_scanner import Directory, ATTRIBUTES


DEFAULT_BATCH_SIZE = 1000

logger = logging.getLogger(__name__)


class SqlABackend(object):
    """A backend for storing directory metrics using SQLAlchemy. Is a
    context manager."""
    def __init__(self, sql_url, **kwds):
        super().__init__(**kwds)
        self._sql_url = sql_url
        self._metadata = MetaData()
        self._define_schema()
        self._engine = create_engine(sql_url, echo=False)
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
        insp = reflection.Inspector.from_engine(self._engine)
        if 'dirs' not in insp.get_view_names():
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
                    num_multi_links,
                    num_exceptions
                from directories
            """)

    def clear_schema(self):
        """Database go POOF!"""
        insp = reflection.Inspector.from_engine(self._engine)
        if 'dirs' in insp.get_view_names():
            self._engine.execute('drop view dirs')
        if 'directories' in insp.get_table_names():
            self._engine.execute('drop table directories')

    def hybrid_refresh(self, top_path):
        """Refresh the specified directory in the database using
        `local_hybrid_refresh` and then refresh each ancestor directory
        that is in the database. Returns the highest ancestor Directory object
        found in the database."""
        assert isinstance(top_path, str), top_path
        result = None
        # Convert str to Directory
        current_directory = self.query().get(top_path) or Directory(top_path)
        while current_directory:
            result = current_directory
            self.local_hybrid_refresh(current_directory)
            current_directory = self.fetch_parent(current_directory)
        return result

    def local_hybrid_refresh(self, directory):
        """Refresh the Directory object by rescanning local directory contents
        and combining the results with database results for immediate child
        directories. Automatically: (1) scans and adds any new local
        subdirectory trees and (2) deletes from the database any local
        subdirectory trees that no longer exist."""
        logger.info('refreshing %r', directory.path)
        immediate_children = self.query().filter(
            Directory.parent == directory.path
        )
        children_index = {d.path: d for d in immediate_children}
        for child_dir_path in directory.generate_local_contents():
            if child_dir_path in children_index:
                child_directory = children_index.pop(child_dir_path)
            else:
                # (1) Scan and add any new local subdirectories trees:
                child_directory = Directory(child_dir_path,
                                            directory.path,
                                            directory.depth + 1)
                self.add_directory(child_directory)
            directory.add_child_directory(child_directory)
        # (2) Delete from the database any local subdirectory trees that
        # no longer exist:
        for missing_path in sorted(children_index):
            missing_tree_root = children_index.pop(missing_path)
            assert missing_path == missing_tree_root.path, missing_path
            self._session.delete(missing_tree_root)
            self.delete_tree(missing_path)
        # Finish up.
        directory.set_last_updated()
        self._session.merge(directory)
        self._session.commit()

    def fetch_parent(self, directory):
        """If the parent Directory object is not in the database, return
        None, otherwise fetch the object from the database."""
        parent = directory.parent
        result = self.query().get(parent) if parent else None
        return result

    def add_tree(self, top_path, batch_size=DEFAULT_BATCH_SIZE):
        return self.add_directory(Directory(top_path), batch_size)

    def add_directory(self, top_directory, batch_size=DEFAULT_BATCH_SIZE):
        self.delete_tree(top_directory.path)
        logger.info('add_directory: %s', top_directory)
        gen = top_directory.scan()
        try:
            while True:
                batch = list(islice(gen, batch_size))
                if batch:
                    self._session.add_all(batch)
                    self._session.commit()
                    # The top_directory will be the last object in the last
                    # non-empty batch.
                    save = batch[-1]
                else:
                    break
            assert save == top_directory, (save.path, top_directory.path)
        except Exception as e:
            self._session.rollback()
            raise e
        return save
        # TODO: exception handling semantics may not make sense here

    def delete_tree(self, top_directory_path):
        """Deletes top_directory_path and all descendants from database."""
        logger.info('delete_tree: %s', top_directory_path)
        try:
            tree = self.query().filter(or_(
                Directory.path == top_directory_path,
                Directory.path.like(top_directory_path + '/%')
            ))
            num_deleted = tree.delete(False)
            self._session.commit()
        except Exception as e:
            self._session.rollback()
            raise e
        return num_deleted

    def query(self):
        """Return an SQLAlchemy Query object."""
        return self._session.query(Directory)

    @property
    def session(self):
        return self._session

    def close(self):
        self._session.close()
        self._engine.dispose()
