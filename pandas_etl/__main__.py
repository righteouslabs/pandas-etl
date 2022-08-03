import os, sys
import argparse
from .etl import pandas_etl_pipeline, pandas_etl_test_pipeline


def cl_args_parser():

    parser = argparse.ArgumentParser(
        prog="pandas-etl",
        # usage='',
        description="A utility for running YAML config as Pandas based ETL code",
        epilog="For Example: python -m pandas_etl --file '<YAML_FILE_PATH>' --var varName1=varValue1 varName2=varValue2 --imports 'xyz.yml' --var varName3=varValue3 ...",
    )

    parser.add_argument(
        "-f",
        "--file",
        help="Path to yaml config file",
        required=True,
        metavar="'usr/dir/file.yaml'",
    )
    parser.add_argument(
        "-i",
        "--imports",
        action="extend",
        nargs="*",
        help="Add or override imports",
        metavar="'./vars/sql-db1.yaml'",
    )
    parser.add_argument(
        "-v",
        "--var",
        action="extend",
        nargs="*",
        help="Define new variables or overwrite existing variables",
        metavar="varName1=varValue1",
    )

    return parser


if __name__ == "__main__":
    parser = cl_args_parser()
    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        sys.exit(0)

    try:
        if os.path.exists(args.file):
            if args.file.endswith((".yml", ".yaml")):
                with open(args.file) as f:
                    data = f.read()
            else:
                raise ValueError(f"Wrong file extension for file: {args.file}")
        else:
            raise FileNotFoundError(f"No such file: {args.file}")
    except:
        raise ValueError(f"Wrong Yaml file Path input: {args.file}")

    # pandas_etl_pipeline(data=data, var=args.var, imports=args.imports)
    pandas_etl_test_pipeline(data=data, var=args.var, imports=args.imports)
