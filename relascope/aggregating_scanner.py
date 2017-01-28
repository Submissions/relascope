"""A generator that recursively scans a directory depth-first accumulating
cumulative statistics for each directory."""


import logging
import os
from pathlib import PurePath
import time


logger = logging.getLogger(__name__)

# The properties of a directory excluding its location (path & parent):
ATTRIBUTES = (
    ('depth', 0),
    ('max_depth', 0),

    ('scan_started', -1),
    ('scan_finished', -1),

    ('last_updated', -1),

    ('max_atime', -1),
    ('max_ctime', -1),
    ('max_mtime', -1),

    ('num_blocks', 0),
    ('num_bytes', 0),
    ('num_files', 0),
    ('num_dirs', 0),
    ('num_symlinks', 0),
    ('num_specials', 0),
    ('num_multi_links', 0),
    ('num_exceptions', 0),
)


class Directory:
    def __init__(self, path, parent=None, depth=None):
        self.path = path
        if parent is not None:
            self.parent = parent
        elif path == '/':
            self.parent = None
        else:
            self.parent = os.path.dirname(path) or None
        if depth is not None:
            self.depth = depth
        else:
            self.depth = len(PurePath(path).parents)
        self.max_depth = self.depth
        self.clear()

    def clear(self):
        for name, default in ATTRIBUTES[4:]:
            setattr(self, name, default)

    def __repr__(self):
        return 'Directory(path=%r)' % (self.path)

    def scan(self):
        """Recursively scan this directory depth-first, updating totals and
        generating Directory instances yielding self last."""
        self.clear()
        self.scan_started = int(time.time())
        try:
            generator = os.scandir(self.path)
        except Exception as e:
            logger.exception('unable to scan %r', self.path)
            self.num_exceptions += 1
        else:
            for entry in generator:
                self.add_dir_entry(entry)
                if entry.is_dir(follow_symlinks=False):
                    child_directory = Directory(entry.path,
                                                self.path,
                                                self.depth + 1)
                    yield from child_directory.scan()
                    self.add_child_directory(child_directory)
        self.scan_finished = self.last_updated = int(time.time())
        yield self

    def add_local_contents(self):
        """Refresh to only the local contents of this directory. Does not scan
        child directories. Returns list of immediate child directories."""
        self.clear()
        # TODO: Too much overlap with scan().
        child_directory_paths = []
        try:
            generator = os.scandir(self.path)
        except Exception as e:
            logger.exception('unable to scan %r', self.path)
            self.num_exceptions += 1
        else:
            for entry in generator:
                self.add_dir_entry(entry)
                if entry.is_dir(follow_symlinks=False):
                    child_directory_paths.append(entry.path)
        return child_directory_paths

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
