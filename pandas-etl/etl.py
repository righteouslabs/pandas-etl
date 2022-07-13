from argparse import Namespace
import os
import sys
import yaml
from calltraces.linetrace import traceInfo, traceError
from calltraces.functiontrace import functiontrace
from tqdm.auto import tqdm
import re
from sqlalchemy import create_engine
import argparse


@functiontrace
def open_yaml_file(args: Namespace) -> list:
    """Open a YAML file from the specified file path"""
    filePaths = args.file
    try:
        data = []
        for i in range(len(filePaths)):
            if os.path.exists(filePaths[i]):
                if filePaths[i].endswith((".yml", ".yaml")):
                    with open(filePaths[i]) as f:
                        data.append(yaml.load(f, Loader=yaml.FullLoader))
                else:
                    raise ValueError(f"Wrong file extension for file: {filePaths[i]}")
            else:
                raise FileNotFoundError(f"No such file: {filePaths[i]}")
        return data
    except:
        raise ValueError(f"Wrong filePaths input: {filePaths}")


@functiontrace
def add_argument_variables(args, yamlData: dict) -> dict:
    """This function adds variables defined in the arguments to the yaml file variables"""
    if args.var is not None:
        if "variables" not in yamlData.keys():
            yamlData["variables"] = {}
        for v in args.var:
            argKey, argValue = v.split("=")
            for k, v in list(yamlData.get("variables", {}).items()):
                if k == argKey:
                    k = argValue
            yamlData["variables"][argKey] = argValue
    return yamlData


@functiontrace
def find_and_replace_variables(yamlData: dict) -> dict:
    """This function helps find and replace variables"""
    fieldValue = yaml.dump(yamlData)
    yamlVariables = yamlData.get("variables", {})
    variables_regex = re.compile(r"\{\{ var\.(.*?) \}\}")
    variables_matched = re.findall(pattern=variables_regex, string=fieldValue)
    for v in variables_matched:
        if v not in yamlVariables.keys():
            raise ValueError(f"Unknown variable {v} found")
        else:
            fieldValue = re.sub(
                pattern=r"\{\{ var\." + v + r" \}\}",
                repl=yamlVariables[v],
                string=fieldValue,
            )
    output = yaml.load(fieldValue, Loader=yaml.FullLoader)
    return output


@functiontrace
def resolve_imports(yamlData: dict):
    """This function will resolve imports if any"""
    if "imports" in yamlData.keys():
        for imp in yamlData.get("imports", {}):
            if os.path.exists(imp):
                if imp.endswith((".yml", ".yaml")):
                    with open(imp) as f:
                        import_yamlData = yaml.load(f, Loader=yaml.FullLoader)
                else:
                    raise ValueError(f"Wrong file extension for the import {imp}")
            else:
                raise FileNotFoundError(f"No such file: {imp}")
            for key in yamlData.keys():
                if key == "imports":
                    return resolve_imports(import_yamlData)
                elif key in ["steps", "connections"]:
                    import_yamlData.get(key, {}).append(yamlData[key])
                else:
                    import_yamlData.get(key, {}).update(yamlData[key])
        return import_yamlData
    return yamlData


@functiontrace
def create_engine_connection(yamlData: dict):
    """This function will find and create engine connection"""
    if "connections" in yamlData.keys():
        for conn in yamlData.get("connections", {}):
            conn["engine"] = create_engine(conn["connStr"])


@functiontrace
def find_and_execute_script(yamlData: dict):
    """This function will find and execute script under pre-flight"""
    script = yamlData.get("pre-flight", {}).get("script", "")
    exec(script, globals())


@functiontrace
def find_key_by_value_and_assign(data: dict, target: str, assign):
    """This function finds the key by value and assigns a new value to the key"""
    for k, v in data.items():
        if v == target:
            k = assign
        elif isinstance(v, dict):
            return find_key_by_value_and_assign(v, target, assign)
        elif isinstance(v, list):
            for i in v:
                if isinstance(i, dict):
                    return find_key_by_value_and_assign(i, target, assign)


@functiontrace
def replace_steps_output(
    steps_output_matched: list, step: dict, yamlData: dict
) -> dict:
    """This function replaces the steps key value pairs with output"""
    for s in steps_output_matched:
        for x in range(len(yamlData["steps"])):
            try:
                if yamlData["steps"][x]["name"] == s:
                    find_key_by_value_and_assign(
                        step,
                        "{{ steps['" + s + "'].output }}",
                        yamlData["steps"][x]["output"],
                    )
            except:
                raise ValueError(f"NO step output found for step name: {s}")
    return step


@functiontrace
def resolve_connections_variables(
    conn_matched: list, step: dict, yamlData: dict
) -> dict:
    """This function helps resolve connection variables for sql queries"""
    for c in conn_matched:
        for x in range(len(yamlData["connections"])):
            try:
                if yamlData["connections"][x]["name"] == c:
                    find_key_by_value_and_assign(
                        step,
                        "{{ conn." + c + " }}",
                        yamlData["connections"][x]["engine"],
                    )
            except:
                raise ValueError(f"NO connection engine found for name: {c}")
    return step


@functiontrace
def execute_steps(yamlData: dict):
    """This function executes the steps specified"""
    tqdm_function_list = tqdm(
        iterable=yamlData["steps"], unit=" function", desc="YAML Steps", colour="green"
    )
    for step in tqdm_function_list:
        tqdm_function_list.set_postfix_str(
            "Function Name: "
            + step["name"]
            + "; Function Description: "
            + step.get("description", "No description provided")
        )
        fieldValue = yaml.dump(step)

        # Check if the step requires output from the previous steps
        steps_output_regex = re.compile(r"\{\{ steps\[\'(.*?)\'\]\.output \}\}")
        steps_output_matched = re.findall(steps_output_regex, string=fieldValue)
        if len(steps_output_matched) > 0:
            step = replace_steps_output(steps_output_matched, step, yamlData)

        # Check if the step execution requires connections engine to the database
        conn_regex = re.compile(r"\{\{ conn\.(.*) \}\}")
        conn_matched = re.findall(conn_regex, string=fieldValue)
        if len(conn_matched) > 0:
            step = resolve_connections_variables(conn_matched, step, yamlData)

        # Find the correct function and get output
        output = eval(f"{step['function']}('{step['args'][0]}')")

        # Assign the output of the function to the step output
        step["output"] = output


# def find_and_execute_all_steps(yamlData: dict):
#     """This function will find and execute all steps"""
#     for step in yamlData["steps"]:
#         print(
#             f"Step name: {step['name']}; Step description: {step.get('description', 'No description provided')}"
#         )
#         output = eval(f"{step['function']}('{step['args'][0]}')")
#         step["output"] = output


# def find_and_replace_step_output(yamlData: dict) -> dict:
#     """This function finds and replaces the step output"""
#     fieldValue = yaml.dump(yamlData)
#     steps_output_regex = re.compile(r"\{\{ steps\[\'(.*?)\'\]\.output \}\}")
#     steps_output_matched = re.findall(steps_output_regex, string=fieldValue)
#     for s in steps_output_matched:
#         for x in range(len(yamlData["steps"])):
#             try:
#                 if yamlData["steps"][x]["name"] == s:
#                     fieldValue = re.sub(
#                         pattern=r"\{\{ steps\[\'" + s + r"\'\]\.output \}\}",
#                         repl=yamlData["steps"][x]["output"],
#                         string=fieldValue,
#                     )
#             except:
#                 raise ValueError(f"NO step output found for step name: {s}")
#     output = yaml.load(fieldValue, Loader=yaml.FullLoader)
#     return output
