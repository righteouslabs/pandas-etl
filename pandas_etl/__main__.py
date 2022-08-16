import sys
import argparse
from .etl import Pipeline
from .etl import parse_command_line_variables


def cl_args_parser():

    parser = argparse.ArgumentParser(
        prog="pandas-etl",
        # usage='',
        description="A utility for running YAML config as Pandas based ETL code",
        epilog="""For Example: python -m pandas_etl --file "<YAML_FILE_PATH>" --var "varName1=varValue1" "varName2=varValue2 --imports "<IMPORT_FILE_PATH>" --var "varName3=varValue3" ...""",
    )

    parser.add_argument(
        "-f",
        "--file",
        help="Path to yaml config file",
        metavar='"usr/dir/file.yaml"',
    )
    parser.add_argument(
        "-i",
        "--imports",
        action="extend",
        nargs="*",
        help="Add or override imports",
        metavar='"./vars/sql-db1.yaml"',
    )
    parser.add_argument(
        "-v",
        "--var",
        action="extend",
        nargs="*",
        help="Define new variables or overwrite existing variables",
        metavar='"varName1=varValue1"',
    )

    return parser


if __name__ == "__main__":
    parser = cl_args_parser()

    args = parser.parse_args()

    if not args.file:
        parser.print_help()
        print("-f/--file arguments are required")
        sys.exit(0)

    pipelineObj = Pipeline(
        yamlData=args.file,
        overrideVariables=parse_command_line_variables(variables=args.var)
        if args.var
        else {},
        includeImports=args.imports,
    )
    pipelineObj.run()
