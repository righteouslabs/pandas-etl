from . import etl
import sys
import argparse


def cl_args_parser():

    parser = argparse.ArgumentParser(
        prog="pandas-etl",
        # usage='',
        description="A utility for running YAML config as Pandas based ETL code",
        epilog="For Example: python -m pandas-etl --file '<YAML_FILE_PATH>' --var varName1=varValue1 varName2=varValue2 --var varName3=varValue3 ...",
    )

    parser.add_argument(
        "-f",
        "--file",
        nargs="*",
        help="Path to yaml config file",
        required=True,
        metavar="'usr/dir/file.yaml'",
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

    yaml_file_objects = etl.open_yaml_file(args)

    for obj in yaml_file_objects:
        yamlData = etl.add_argument_variables(args, obj)
        yamlData = etl.find_and_replace_variables(yamlData)
        etl.find_and_execute_script(yamlData)
        etl.execute_steps(yamlData)
