"""A generator that recursively scans a directory depth-first accumulating
cumulative statistics for each directory."""


import os


# The properties of a directory excluding its location (path & parent):
ATTRIBUTES = (
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
)


class Directory:
    def __init__(self, path, parent=None):
        self.path = path
        if parent:
            self.parent = parent
        elif parent == '/':
            self.parent = None
        else:
            self.parent = os.path.dirname(path) or None
        self.clear()

    def clear(self):
        for name, default in ATTRIBUTES:
            setattr(self, name, default)

    def __repr__(self):
        return 'Directory(path=%r)' % (self.path)

    def scan(self):
        """Recursively scan this directory depth-first, updating totals and
        generating Directory instances yielding self last."""
        self.clear()
        for entry in os.scandir(self.path):
            self.add_dir_entry(entry)
            if entry.is_dir(follow_symlinks=False):
                child_directory = Directory(entry.path, self.path)
                yield from child_directory.scan()
                self.add_child_directory(child_directory)
        yield self

    def add_local_contents(self):
        """Refresh to only the local contents of this directory. Does not scan
        child directories. Returns list of immediate child directories."""
        self.clear()
        child_directory_paths = []
        for entry in os.scandir(self.path):
            self.add_dir_entry(entry)
            if entry.is_dir(follow_symlinks=False):
                child_directory_paths.append(entry.path)
        return child_directory_paths

    def add_dir_entry(self, dir_entry):
        stat = dir_entry.stat(follow_symlinks=False)
        self.max_atime = max(self.max_atime, stat.st_atime)
        self.max_ctime = max(self.max_ctime, stat.st_ctime)
        self.max_mtime = max(self.max_mtime, stat.st_mtime)
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
