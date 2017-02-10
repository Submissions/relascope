# -*- coding: utf-8 -*-

"""relascope inventories a forest of directory trees."""


import argparse
import logging
import os
import sys
from time import localtime, strftime

from .aggregating_scanner import Directory
from .sqlalchemy import SqlABackend, ATTRIBUTES


logger = logging.getLogger(__name__)  # used if file imported as module


def main():
    args = parse_args()
    config_logging(args)
    logger.debug('args: %r', vars(args))
    args.backend = make_backend(args)
    try:
        args.func(args)
    except BrokenPipeError as e:
        pass  # ignore
    sys.stderr.close()


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-d', '--db_path',
                        default='du.db',
                        help='path to sqlite database file'
                        ' defaults to "du.db"')
    subparsers = parser.add_subparsers(help='sub-commands')

    # create the parser for the "scan" command
    parser_scan = subparsers.add_parser('scan', help='populate or refresh')
    parser_scan.add_argument('root_dirs', nargs='+', help='starting points')
    parser_scan.set_defaults(func=scan)

    # create the parser for the "dump" command
    parser_dump = subparsers.add_parser('dump', help='dump to TSV')
    parser_dump.set_defaults(func=dump)

    args = parser.parse_args()
    return args


def config_logging(args):
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level,
                        datefmt='%Y-%m-%d %H:%M:%S',
                        format='%(asctime)s %(levelname)-8s %(message)s')


def make_backend(args):
    db_path = args.db_path
    backend = SqlABackend('sqlite:///' + db_path)
    return backend


def scan(args):
    root_dirs = args.root_dirs
    for root_dir in root_dirs:
        assert os.path.isdir(root_dir), root_dir
    logger.info('starting')
    for root_dir in root_dirs:
        args.backend.hybrid_refresh(root_dir)
    logger.info('finished')


def dump(args):
    attributes = ['path', 'parent']
    attributes.extend(a for a, d in ATTRIBUTES)
    transforms = [str] * len(attributes)
    for i in range(4, 10):
        transforms[i] = format_date
    rules = list(zip(attributes, transforms))
    print('kb', *attributes, sep='\t')
    for d in args.backend.query().order_by(True):
        kb = d.num_blocks // 2
        row = [t(getattr(d, a)) for a, t in rules]
        print(kb, *row, sep='\t')


def format_date(timestamp):
    if timestamp == -1:
        result = ''
    else:
        result = strftime("%Y-%m-%d %H:%M:%S", localtime(timestamp))
    return result


if __name__ == "__main__":
    main()
