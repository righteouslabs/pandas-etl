from asyncio import as_completed
from multiprocessing.connection import wait
from .etl import *
import concurrent.futures


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

    traceInfo(f"Starting pipeline execution")

    yamlData = to_yaml(data)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        if var is not None:
            if "variables" not in yamlData.keys():
                yamlData["variables"] = {}

            [executor.submit(add_argument_variables, v, yamlData) for v in var]

        if imports is not None:
            if "imports" not in yamlData.keys():
                yamlData["imports"] = []

            [executor.submit(add_argument_imports, imp, yamlData) for imp in imports]

        yamlData = resolve_imports(yamlData)

        yamlData = find_and_replace_variables(yamlData)

        if "connections" in yamlData.keys():
            [
                executor.submit(create_engine_connection, conn, yamlData)
                for conn in yamlData["connections"]
            ]

        find_and_execute_script(yamlData)

        execute_steps(yamlData)

        executor.shutdown(wait=True)

    traceInfo(f"Successfully finished pipeline execution")


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
