from . import etl

import argparse

parser = argparse.ArgumentParser(
    prog="pandas-etl",
    # usage='',
    description="Process some integers.",
    # epilog='',
)

parser.add_argument("-f", "--file", nargs="*", help="Path to yaml config file")

if __name__ == "__main__":
    args = parser.parse_args()
    filePaths = args.file
    yaml_file_objects = etl.open_yaml_file(filePaths)

    for obj in yaml_file_objects:
        yamlData = etl.find_and_replace_variables(obj)
        etl.find_and_execute_script(yamlData)
        etl.execute_steps(yamlData)
