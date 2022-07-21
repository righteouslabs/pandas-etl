from .etl import *


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


@functiontrace
def pandas_etl_pipeline(data: str, var: list, imports: list):

    yamlData = to_yaml(data)
    yamlData = add_argument_variables(var=var, yamlData=yamlData)
    yamlData = add_argument_imports(imports=imports, yamlData=yamlData)
    yamlData = resolve_imports(yamlData)
    yamlData = find_and_replace_variables(yamlData)
    create_engine_connection(yamlData)
    find_and_execute_script(yamlData)
    execute_steps(yamlData)
    traceInfo(f"Successfully finished pipeline")


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

    pandas_etl_pipeline(data=data, var=args.var, imports=args.imports)
