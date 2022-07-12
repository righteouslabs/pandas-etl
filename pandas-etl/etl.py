import os
import sys
from click import FileError
import yaml
from calltraces.linetrace import traceInfo
from calltraces.functiontrace import functiontrace
import tqdm
import re
import argparse


@functiontrace
def open_yaml_file(filePaths: list) -> list:
    """Open a YAML file from the specified file path"""
    try:
        data = []
        for i in range(len(filePaths)):
            if os.path.exists(filePaths[i]):
                if filePaths[i].endswith((".yml", ".yaml")):
                    with open(filePaths[i]) as f:
                        data.append(yaml.load(f, Loader=yaml.FullLoader))
                else:
                    raise FileError("Wrong file extension")
            else:
                raise FileNotFoundError("No such file")
        return data
    except:
        raise ValueError("Wrong filePaths input")


def find_and_replace_variables(yamlData: dict) -> dict:
    """This function helps find and replace variables"""
    import re
    import yaml

    fieldValue = yaml.dump(yamlData)
    yamlVariables = yamlData.get("variables", {})
    variables_regex = re.compile(r"\{\{var\.(\w+)\}\}")
    variables_matched = re.findall(pattern=variables_regex, string=fieldValue)
    for v in variables_matched:
        if v not in yamlVariables.keys():
            raise ValueError(f"Unknown variable {v} found")
        else:
            fieldValue = re.sub(
                pattern=r"\{\{var\." + v + r"\}\}",
                repl=yamlVariables[v],
                string=fieldValue,
            )
    output = yaml.load(fieldValue, Loader=yaml.FullLoader)
    return output


def find_and_execute_script(yamlData: dict):
    """This function will find and execute script under pre-flight"""
    script = yamlData.get("pre-flight", {}).get("script", "")
    exec(script, globals())


def find_and_execute_all_steps(yamlData: dict):
    """This function will find and execute all steps"""
    for step in yamlData["steps"]:
        print(
            f"Step name: {step['name']}; Step description: {step.get('description', 'No description provided')}"
        )
        output = eval(f"{step['function']}('{step['args'][0]}')")
        step["output"] = output


def find_and_replace_step_output(yamlData: dict) -> dict:
    """This function finds and replaces the step output"""
    import yaml
    import re

    fieldValue = yaml.dump(yamlData)
    steps_output_regex = re.compile(r"\{\{steps\[\'(.*?)\'\]\.output\}\}")
    steps_output_matched = re.findall(steps_output_regex, string=fieldValue)
    for s in steps_output_matched:
        for x in range(len(yamlData["steps"])):
            try:
                if yamlData["steps"][x]["name"] == s:
                    fieldValue = re.sub(
                        pattern=r"\{\{steps\[\'" + s + r"\'\]\.output\}\}",
                        repl=yamlData["steps"][x]["output"],
                        string=fieldValue,
                    )
            except:
                raise ValueError(f"NO step output found for step name: {s}")
    output = yaml.load(fieldValue, Loader=yaml.FullLoader)
    return output


@functiontrace
def execute_steps(yamlData: dict):
    """This function executes the steps specified"""
    for step in yamlData["steps"]:
        print(
            f"Step name: {step['name']}; Step description: {step.get('description', 'No description provided')}"
        )

        # Check if the step requires output from the previous steps.
        steps_output_regex = re.compile(r"\{\{steps\[\'(.*?)\'\]\.output\}\}")
        fieldValue = yaml.dump(step)
        steps_output_matched = re.findall(steps_output_regex, string=fieldValue)
        for s in steps_output_matched:
            for x in range(len(yamlData["steps"])):
                try:
                    if yamlData["steps"][x]["name"] == s:
                        find_key_by_value_and_assign(
                            step,
                            "{{steps['" + s + "'].output}}",
                            yamlData["steps"][x]["output"],
                        )
                except:
                    raise ValueError(f"NO step output found for step name: {s}")

        # Find the correct function and get output
        output = eval(f"{step['function']}('{step['args'][0]}')")

        # Assign the output of the function to the step output
        step["output"] = output


# Assisting Function for Function execute_steps
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
