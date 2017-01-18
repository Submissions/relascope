# -*- coding: utf-8 -*-

"""relascope inventories a forest of directory trees."""


import argparse
import logging
import os

from .aggregating_scanner import scan


logger = logging.getLogger(__name__)  # used if file imported as module


def main():
    args = parse_args()
    config_logging(args)
    run(args)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-v', '--verbose', action='store_true')
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
    root_dirs = args.root_dirs
    for root_dir in root_dirs:
        assert os.path.isdir(root_dir), root_dir
    for root_dir in root_dirs:
        gen = scan(root_dir)
        for d in gen:
            print('{:8} {} {:<40} {}'.format(
                d.num_blocks, d.num_multi_links, d.path, d.parent
            ))
    pass  # TODO


if __name__ == "__main__":
    main()
