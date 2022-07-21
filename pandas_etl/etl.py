from inspect import trace
import os
from shutil import ExecError
import sys
import yaml
from calltraces.linetrace import traceInfo, traceError
from calltraces.functiontrace import functiontrace
from tqdm.auto import tqdm
import re
from sqlalchemy import create_engine, false
import argparse


@functiontrace
def to_yaml(data: str) -> dict:
    """Open a YAML file from the specified file path"""
    try:
        yamlData = yaml.load(data, Loader=yaml.FullLoader)
        traceInfo(f"The yaml config is loaded!")
        return yamlData
    except:
        raise ValueError(f"Wrong input")


@functiontrace
def add_argument_variables(var: list, yamlData: dict) -> dict:
    """This function adds variables defined in the arguments to the yaml file variables"""
    if var is not None:
        if "variables" not in yamlData.keys():
            yamlData["variables"] = {}
        for v in var:
            argKey, argValue = v.split("=")
            yamlData["variables"][argKey] = argValue
            traceInfo(f"Imported the variable: {argKey}: {argValue}")
    return yamlData


@functiontrace
def add_argument_imports(imports: list, yamlData: dict) -> dict:
    """This function adds imports defined in the arguments to the yaml file imports"""
    if imports is not None:
        if "imports" not in yamlData.keys():
            yamlData["imports"] = {}
        yamlData["imports"] = imports + yamlData["imports"]
        traceInfo(f"Added imports: {imports}, at the top of yaml config")
    return yamlData


@functiontrace
def find_and_replace_variables(yamlData: dict) -> dict:
    """This function helps find and replace variables"""
    fieldValue = yaml.dump(yamlData)
    yamlVariables = yamlData.get("variables", {})
    variables_regex = re.compile(r"\$\{var\.(.*?)\}")
    variables_matched = re.findall(pattern=variables_regex, string=fieldValue)
    for v in variables_matched:
        if v not in yamlVariables.keys():
            raise ValueError(f"Unknown variable '{v}' found")
        else:
            fieldValue = re.sub(
                pattern=r"\$\{var\." + v + r"\}",
                repl=yamlVariables[v],
                string=fieldValue,
            )
        traceInfo(f"Substituted Variable '{v}' in yaml config")
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
                        traceInfo(f"Imported file: {imp}")
                else:
                    raise ValueError(f"Wrong file extension for the import {imp}")
            else:
                raise FileNotFoundError(f"No such file: {imp}")
            for key in import_yamlData.keys():
                if key == "imports":
                    return resolve_imports(import_yamlData)
                elif key in ["steps", "connections"]:
                    import_yamlData.get(key, {}).append(yamlData.get(key, []))
                    traceInfo(
                        f"Appended {key} property from import file {imp} to the top"
                    )
                else:
                    import_yamlData.get(key, {}).update(yamlData.get(key, {}))
                    traceInfo(f"Updated {key} property from import file {imp}")
        return import_yamlData
    return yamlData


@functiontrace
def create_engine_connection(yamlData: dict):
    """This function will find and create engine connection"""
    for conn in yamlData.get("connections", []):
        conn["engine"] = create_engine(conn["connStr"])
        traceInfo(f"Created an engine for connection: {conn['name']}")


@functiontrace
def find_and_execute_script(yamlData: dict):
    """This function will find and execute script under pre-flight"""
    script = yamlData.get("pre-flight", {}).get("script", "")
    try:
        exec(script, globals())
        traceInfo(f"Finished executing script from pre-flight!")
    except:
        raise ExecError(f"Failed to execute script: {script}")


@functiontrace
def find_key_by_value_and_assign(data: dict, target: str, assign):
    """This function finds the key by value and assigns a new value to the key"""
    for k, v in data.items():
        if v == target:
            data[k] = assign
            break
        elif isinstance(v, dict):
            data[k] = find_key_by_value_and_assign(v, target, assign)
        elif isinstance(v, list):
            x = 0
            for i in v:
                if isinstance(i, dict):
                    data[k][x] = find_key_by_value_and_assign(i, target, assign)
                elif i == target:
                    data[k][x] = assign
                    break
                x += 1
    return data


@functiontrace
def replace_steps_output(
    steps_output_matched: list, step: dict, yamlData: dict
) -> dict:
    """This function replaces the steps key value pairs with output"""
    for s in steps_output_matched:
        try:
            found = False
            for x in range(len(yamlData["steps"])):
                if s in yamlData["steps"][x].keys():
                    find_key_by_value_and_assign(
                        step,
                        "${steps['" + s + "'].output}",
                        yamlData["steps"][x]["output"],
                    )
                    found = True
                    break
                elif yamlData["steps"][x].get("name", None) == s:
                    find_key_by_value_and_assign(
                        step,
                        "${steps['" + s + "'].output}",
                        yamlData["steps"][x]["output"],
                    )
                    found = True
                    break
            if not found:
                raise ValueError(f"NO step output found for step name: {s}")
        except:
            raise ValueError(f"NO step output found for step name: {s}")
    return step


@functiontrace
def resolve_connections_variables(
    conn_matched: list, step: dict, yamlData: dict
) -> dict:
    """This function helps resolve connection variables for sql queries and returns step with corresponding engine connection"""
    for c in conn_matched:
        for x in range(len(yamlData["connections"])):
            try:
                if yamlData["connections"][x]["name"] == c:
                    find_key_by_value_and_assign(
                        step,
                        "${conn." + c + "}",
                        yamlData["connections"][x]["engine"],
                    )
                    break
                else:
                    raise ValueError(f"NO connection engine found for name: {c}")
            except:
                raise ValueError(f"NO connection engine found for name: {c}")
    return step


@functiontrace
def return_function_object(function_matched: list, yamlData: dict) -> dict:
    for f in function_matched:
        for x in range(len(yamlData["steps"])):
            try:
                if f in yamlData["steps"][x].keys():
                    function_object = yamlData["steps"][x]["output"]
                    break
                elif yamlData["steps"][x].get("name", None) == f:
                    function_object = yamlData["steps"][x]["output"]
                    break
                else:
                    raise ValueError(f"NO step output found for step name: {f}")
            except:
                raise ValueError(f"NO step output found for step name: {f}")
    return function_object


@functiontrace
def execute_steps(yamlData: dict):
    """This function executes the steps specified"""
    tqdm_function_list = tqdm(
        iterable=yamlData["steps"], unit=" function", desc="YAML Steps", colour="green"
    )
    for step in tqdm_function_list:
        if "name" in step.keys():
            function_name = step["name"]
        else:
            function_name = list(step.keys())[0]

        tqdm_function_list.set_postfix_str(
            "Function Name: "
            + function_name
            + "; Function Description: "
            + step.get("description", "No description provided")
        )
        fieldValue = yaml.dump(step)

        # Check if the step requires output from the previous steps
        steps_output_regex = re.compile(r"\$\{steps\[\'(.*?)\'\]\.output\}")
        steps_output_matched = re.findall(steps_output_regex, string=fieldValue)
        if len(steps_output_matched) > 0:
            step = replace_steps_output(steps_output_matched, step, yamlData)
            traceInfo(f"Successfully replaced outputs needed for step!")

        # Check if the step execution requires connections engine to the database
        conn_regex = re.compile(r"\$\{conn\.(.*)\}")
        conn_matched = re.findall(conn_regex, string=fieldValue)
        if len(conn_matched) > 0:
            step = resolve_connections_variables(conn_matched, step, yamlData)
            traceInfo(f"Successfully replaced connections needed for step!")

        # Check if the step has only one property
        if len(step.keys()) == 1:
            function = list(step.keys())[0]
            if type(step.get(function)) == dict:
                kwargs = step.get(function, {})
            else:
                args = step.get(function, [])
            function_regex = re.compile(r"steps\[\'(.*?)\'\]\.output")
            function_matched = re.findall(function_regex, function)
            if len(function_matched) > 0:
                function_object = return_function_object(function_matched, yamlData)
                step["name"] = function_object + "." + function.split(".")[-1]
                function = getattr(function_object, function.split(".")[-1])
        elif "function" in step.keys():
            if type(step["function"]) is not dict:
                method = step["function"]
                function = f"{method}"
            else:
                method = step["function"]["name"]
                function = f"{step['function']['object']}.{method}"

            if "args" in step.keys():
                if type(step.get("args")) == dict:
                    kwargs = step.get("args", {})
                else:
                    args = step.get("args", [])
        else:
            raise ValueError(f"Invalid step type: {step}")

        # Find the correct function and get output
        funcHandle = eval(function)
        if args is not None:
            traceInfo(f"Executing function: '{function_name}', with arguments: {args}")
            try:
                output = funcHandle(*args)
            except:
                raise ExecError(
                    f"Error executing function: {function_name}, , with arguments: {args}"
                )
        else:
            traceInfo(
                f"Executing function: '{function_name}', with arguments: {kwargs}"
            )
            try:
                output = funcHandle(**kwargs)
            except:
                raise ExecError(
                    f"Error executing function: {function_name}, with arguments: {kwargs}"
                )

        # Assign the output of the function to the step output
        step["output"] = output
        traceInfo(f"Successfully executed step and stored as output!")
    traceInfo(f"Successfully executed all the steps!")


########## YAML Constructor #################################
# TODO: Implement YAML constructor

# connection_regex = re.compile(r"\$\{conn.*?\}")
# variables_regex = re.compile(r"\$\{var.*?\}")
# steps_output_regex = re.compile(r"\$\{steps\[\'.*?\'\]\.output\}")

# # def connection_representer(dumper, data):
# #     return dumper.represent_scalar(u'!connection', u'{%s}' % data)

# # yaml.add_representer(str, connection_representer)


# def connection_constructor(loader, node):
#     print("inside parameter_constructor")
#     value = loader.construct_scalar(node)
#     a = map(object, re.findall(connection_regex, value))
#     return "connections"


# yaml.add_constructor("!connection", connection_constructor)
# yaml.add_implicit_resolver("!connection", connection_regex)


# def variables_constructor(loader, node):
#     print("inside variables_constructor")
#     value = loader.construct_scalar(node)
#     a = map(object, re.findall(variables_regex, value))
#     return "variables"


# yaml.add_constructor("!variables", variables_constructor)
# yaml.add_implicit_resolver("!variables", variables_regex)


# def steps_output_constructor(loader, node):
#     print("inside step_output_constructor")
#     value = loader.construct_scalar(node)
#     a = map(object, re.findall(steps_output_regex, value))
#     return "steps_output"


# yaml.add_constructor("!steps_output", steps_output_constructor)
# yaml.add_implicit_resolver("!steps_output", steps_output_regex)
