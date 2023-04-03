from argparse import ArgumentParser
import logging
import os
from . import Makefile

def main():
    parser = ArgumentParser()
    parser.add_argument('task')
    parser.add_argument('--sqmakefile', default='sqmake.yml', help='Path to SQMake file (default: sqmake.yml)')
    parser.add_argument('--echo', action='store_true', help='echo SQL commands')
    parser.add_argument('--database', help="Database to use (if not specified in sqmakefile)")
    parser.add_argument('--cache-dir', default=os.getcwd(), help="Where to cache files (i.e. working directory for tasks)")
    args = parser.parse_args()

    # set up logging
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.INFO)
    rootLogger.addHandler(logging.StreamHandler())

    makefile = Makefile.from_yaml(args.sqmakefile, dbname=args.database, echo=args.echo)
    makefile.run(args.task, cache_dir=args.cache_dir)
