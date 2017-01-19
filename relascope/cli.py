# -*- coding: utf-8 -*-

"""relascope inventories a forest of directory trees."""


import argparse
import logging
import os

from .aggregating_scanner import Directory
from .sqlalchemy import SqlABackend


logger = logging.getLogger(__name__)  # used if file imported as module


def main():
    args = parse_args()
    config_logging(args)
    run(args)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-d', '--db_path',
                        default='du.db',
                        help='path to sqlite database file'
                        ' defaults to "du.db"')
    parser.add_argument('root_dirs', nargs='+', help='starting points')
    args = parser.parse_args()
    return args


def config_logging(args):
    global logger
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level)
    logger = logging.getLogger('relascope')


def run(args):
    logger.debug('args: %r', vars(args))
    db_path = args.db_path
    root_dirs = args.root_dirs
    for root_dir in root_dirs:
        assert os.path.isdir(root_dir), root_dir
    backend = SqlABackend('sqlite:///' + db_path)
    for root_dir in root_dirs:
        backend.delete_tree(root_dir)
        backend.add_tree(root_dir)
    for d in backend.query():
        print('{}\t{}\t{}\t{}'.format(
            d.num_blocks, d.num_multi_links, d.path, d.parent
        ))


if __name__ == "__main__":
    main()
