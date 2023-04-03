from argparse import ArgumentParser
import logging
from . import Makefile

def main():
    parser = ArgumentParser()
    parser.add_argument('task')
    parser.add_argument('--sqmakefile', default='sqmake.yml', help='Path to SQMake file (default: sqmake.yml)')
    parser.add_argument('--echo', action='store_true', help='echo SQL commands')
    parser.add_argument('--database', help="Database to use (if not specified in sqmakefile)")
    args = parser.parse_args()

    # set up logging
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.INFO)
    rootLogger.addHandler(logging.StreamHandler())

    makefile = Makefile.from_yaml(args.sqmakefile, dbname=args["database"], echo=args.echo)
    makefile.run(args.task)
