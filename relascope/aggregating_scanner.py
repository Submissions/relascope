"""A generator that recursively scans a directory depth-first accumulating
cumulative statistics for each directory."""


import logging
import os
from pathlib import PurePath
import time


logger = logging.getLogger(__name__)

# The properties of a directory excluding its location (path & parent):

DEFAULT_MAP = dict(
    synthetic=None,
    path=None,
    depth=None,
    timestamp=-1,
    count=0
)

# name            code  semantics
MODEL_STR = '''
tb                T      synthetic
gb                G      synthetic
mb                M      synthetic
path              p      path
parent            P      path
depth             h      depth
max_depth         H      depth
scan_started      s      timestamp
scan_finished     S      timestamp
last_updated      u      timestamp
max_atime         a      timestamp
max_ctime         c      timestamp
max_mtime         m      timestamp
num_blocks        B      count
num_bytes         b      count
num_files         f      count
num_dirs          d      count
num_symlinks      l      count
num_multi_links   L      count
num_specials      e      count
num_exceptions    E      count
'''

MODEL = tuple(  # (name, code, semantics, default) for each attribute
    (name, code, semantics, DEFAULT_MAP[semantics])
    for name, code, semantics in
    (l.split() for l in MODEL_STR.splitlines() if l)
)

CLEAR = tuple(
    (name, default) for name, _, _, default in MODEL
    if default is not None
)


class Directory:
    def __init__(self, path, parent=None, depth=None):
        self.path = path
        self.parent = parent
        if depth is not None:
            self.depth = depth
        else:
            self.depth = len(PurePath(path).parents)
        self.max_depth = self.depth
        self.clear()

    def clear(self, num_start=4):
        for name, default in CLEAR:
            setattr(self, name, default)

    def __repr__(self):
        return 'Directory(path=%r)' % (self.path)

    def scan(self):
        """Recursively scan this directory depth-first, updating totals and
        generating Directory instances yielding self last."""
        self.clear()
        self.scan_started = self.set_last_updated()
        for child_directory_path in self.generate_local_contents():
            child_directory = Directory(child_directory_path,
                                        self.path,
                                        self.depth + 1)
            yield from child_directory.scan()
            self.add_child_directory(child_directory)
        self.scan_finished = self.set_last_updated()
        yield self

    def generate_local_contents(self):
        """Refresh to only the local contents of this directory. Does not scan
        child directories; instead just generates the paths of the immediate
        child directories. To quickly refresh a directory and obtain a list of
        all child directories, run:

            d = Directory('some_path')
            child_directories = list(d.generate_local_contents())"""
        self.clear()
        try:
            generator = os.scandir(self.path)
        except Exception as e:
            logger.exception('unable to scan %r', self.path)
            self.num_exceptions += 1
        else:
            for entry in generator:
                self.add_dir_entry(entry)
                if entry.is_dir(follow_symlinks=False):
                    yield entry.path

    def add_dir_entry(self, dir_entry):
        try:
            stat = dir_entry.stat(follow_symlinks=False)
        except Exception as e:
            logger.exception('unable to stat %r', dir_entry.path)
            self.num_exceptions += 1
        else:
            pass
        self.max_atime = max(self.max_atime, int(stat.st_atime))
        self.max_ctime = max(self.max_ctime, int(stat.st_ctime))
        self.max_mtime = max(self.max_mtime, int(stat.st_mtime))
        if dir_entry.is_file(follow_symlinks=False):
            self.num_blocks += round(stat.st_blocks / stat.st_nlink)
            self.num_bytes += round(stat.st_size / stat.st_nlink)
            self.num_files += 1
            if stat.st_nlink != 1:
                self.num_multi_links += 1
        elif dir_entry.is_dir(follow_symlinks=False):
            self.num_blocks += stat.st_blocks
            self.num_dirs += 1
        elif dir_entry.is_symlink():
            self.num_symlinks += 1
        else:
            self.num_specials += 1

    def add_child_directory(self, child_directory):
        self.max_depth = max(self.max_depth, child_directory.max_depth)
        self.max_atime = max(self.max_atime, child_directory.max_atime)
        self.max_ctime = max(self.max_ctime, child_directory.max_ctime)
        self.max_mtime = max(self.max_mtime, child_directory.max_mtime)
        self.num_blocks = self.num_blocks + child_directory.num_blocks
        self.num_bytes = self.num_bytes + child_directory.num_bytes
        self.num_files = self.num_files + child_directory.num_files
        self.num_dirs = self.num_dirs + child_directory.num_dirs
        self.num_symlinks = self.num_symlinks + child_directory.num_symlinks
        self.num_specials = self.num_specials + child_directory.num_specials
        self.num_multi_links = (self.num_multi_links +
                                child_directory.num_multi_links)
        self.num_exceptions = (self.num_exceptions +
                               child_directory.num_exceptions)

    def set_last_updated(self):
        """Set to now"""
        result = self.last_updated = int(time.time())
        return result
