from .etl import *


def cl_args_parser():

    parser = argparse.ArgumentParser(
        prog="pandas-etl",
        # usage='',
        description="A utility for running YAML config as Pandas based ETL code",
        epilog="For Example: python -m pandas-etl --file '<YAML_FILE_PATH>' --var varName1=varValue1 varName2=varValue2 --import xyz --var varName3=varValue3 ...",
    )

    parser.add_argument(
        "-f",
        "--file",
        nargs="*",
        help="Path to yaml config file",
        required=True,
        metavar="'usr/dir/file.yaml'",
    )
    # TODO: support imports
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


@functiontrace
def main(args):
    # TODO: Reconfigure function to handle yaml
    yaml_file_objects = open_yaml_file(args)

    for obj in yaml_file_objects:
        yamlData = add_argument_variables(args, obj)
        yamlData = resolve_imports(yamlData)
        yamlData = find_and_replace_variables(yamlData)
        find_and_execute_script(yamlData)
        execute_steps(yamlData)


if __name__ == "__main__":
    parser = cl_args_parser()
    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        sys.exit(0)

    main(args)
