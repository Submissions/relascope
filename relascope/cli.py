# -*- coding: utf-8 -*-

"""relascope inventories a forest of directory trees."""


import argparse
import logging


logger = logging.getLogger(__name__)  # used if file imported as module


def main():
    args = parse_args()
    config_logging(args)
    run(args)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    return args


def config_logging(args):
    global logger
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level)
    logger = logging.getLogger('relascope')


def run(args):
    logger.debug('args: %r', vars(args))
    pass  # TODO


if __name__ == "__main__":
    main()
