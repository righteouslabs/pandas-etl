import concurrent.futures
import logging
import os
import re
import yaml
import networkx as nx
import pandas as pd
from calltraces.classtrace import classtrace
from calltraces.linetrace import traceInfo
from sqlalchemy import create_engine
from tqdm.auto import tqdm


def parse_command_line_variables(variables: list[str]) -> dict[str, str]:
    """
    Parse command line variables and convert to dictionary

    Args:
        variables (list[str], optional): Values as `name=value` pairs. Defaults to [].

    Raises:
        ValueError: If format is not as `name=value` pairs

    Returns:
        dict[str, str]: Returns a dictionary of `{key:value}` pairs after parsing command line
    """
    output = {}
    for var in variables:
        varSplits = var.split("=")
        if len(varSplits) != 2:
            raise ValueError(
                f"Invalid command line for variable '{var}' Expected format as varName=varValue"
            )
        varName = varSplits[0].strip()
        varValue = varSplits[1].strip()
        output[varName] = varValue
    return output


def _processStringForExpressions(input: any) -> any:
    """
    Private function to process a string and replace placeholder `${expression}` with `expression value`

    If a string ony contains the text `"${expression}"`, then this function will return the evaluated expression instead of a string representation of it.

    Args:
        input (str|dict|object|any): Input string or dictionary or object to be evaluated

    Returns:
        str: Output string or dictionary (always same type as input)
    """
    if isinstance(input, str):
        # Reference: https://docs.python.org/3/howto/regex.html#greedy-versus-non-greedy
        expression_regex = re.compile(r"(.*)\$\{(.*?)\}(.*)")
        expression_matched = re.findall(pattern=expression_regex, string=input)
        output = input
        for (
            text_before_expression,
            expression,
            text_after_expression,
        ) in expression_matched:

            if (
                len(expression_matched) == 1
                and len(text_before_expression) == 0
                and len(text_after_expression) == 0
            ):
                # The expression is the only part of the string.
                # So it is not expected to interpreted as a string,
                # but an object instead. So return object directly.
                return eval(expression)

            # Use simple string replace instead of regex replace because expressions themselves may break regex rules
            # E.g. if expression is "steps['pd.read_csv']" then the square brackets cause problems for regex
            output = output.replace(
                "${" + expression + "}",
                str(eval(expression)),
            )

            if re.findall(pattern=expression_regex, string=output):
                output = _processStringForExpressions(input=output)

        return output

    elif isinstance(input, dict):
        output = {k: _processStringForExpressions(v) for k, v in input.items()}
        return output

    elif isinstance(input, list):
        output = input
        for i in range(len(input)):
            output[i] = _processStringForExpressions(input[i])
        return output

    else:
        return input


@classtrace
class Pipeline(object):
    """
    ETL Pipeline class
    """

    # -------------------------------------------------------------------------

    # region Class Methods

    def __init__(
        self,
        yamlData: str or dict,
        includeImports: list = [],
        overrideVariables: dict = {},
    ):
        """
        Initialize Pipeline class

        Args:
            yamlData (str | dict): Either file name of YAML or the YAML directory
            includeImports (list, optional): imports to add from YAML. Defaults to [].
            overrideVariables (dict[str, str], optional): variables to override from YAML. Defaults to {}.
        """

        if isinstance(yamlData, str):
            if os.path.exists(yamlData):
                traceInfo(f"Loading yaml config from file {yamlData}")
                with open(yamlData, mode="r", encoding="utf-8") as f:
                    yamlData = f.read()

            else:
                traceInfo("Parsing YAML from memory")
            # Parse YAML string to in-memory object
            yamlData = Pipeline.from_yaml_to_dict(yamlStr=yamlData)
            traceInfo(f"Main YAML definition loaded from: {yamlData}")

        if includeImports:
            # Properties we parse from command line or expect code to have
            argumentProperties = {
                "imports": includeImports,
            }

            # Merge argument properties with YAML file properties
            yamlData = Pipeline.__merge_yaml_dict(
                main_yaml=yamlData, to_be_imported_yaml=argumentProperties
            )

        # Update dictionary with imported values
        yamlData = Pipeline.resolve_imports(yamlData=yamlData)

        if overrideVariables:
            # Properties we parse from command line or expect code to have
            argumentProperties = {
                "variables": overrideVariables,
            }

            # Merge argument properties with YAML file properties
            yamlData = Pipeline.__merge_yaml_dict(
                main_yaml=yamlData, to_be_imported_yaml=argumentProperties
            )

        # Rename yamlData to Python object called 'properties'
        properties = yamlData

        # Set the properties of the YAML to this Class instance's properties
        # So they can be accessed like this: `self.imports` or `self.preFlight`
        self.__dict__.update(properties)

        # Set variable property for this Class to help resolve variable values
        self.variables = Pipeline._Variables(vars=properties.get("variables", {}))

        # Create a global called `var` such that:
        #   Given `{varName:varValue}` dictionary
        #   And saved as Pipeline._Variables class
        #   Then `var.varName` evaluates to `varValue`
        globals()["var"] = self.variables

        # Set preFlight property to this Class
        exec(self.__dict__.get("preFlight", {}).get("script", ""), globals())

        # Set connections property for this Class to help resolve variable values
        self.connections = Pipeline._Connections(
            conns=properties.get("connections", {})
        )

        # Create a global called `conn` such that:
        #   Given `{connName:connObj}` dictionary
        #   And saved as Pipeline._Connections class
        #   Then `conn.connName` evaluates to `connObj`
        globals()["conn"] = self.connections

        # Set steps property for this Class to help resolve values
        self.steps = Pipeline._Steps(steps=properties.get("steps", []))

        # Create a global called `steps` such that:
        #   Given `[{stepName:stepObj}]` list
        #   And saved as Pipeline._Steps class
        #   Then `steps['stepName]` evaluates to `stepObj`
        globals()["steps"] = self.steps

        for key, value in self.__dict__.items():
            globals()[key] = value

        traceInfo("Successfully loaded pipeline!")

    # endregion Class Methods

    # -------------------------------------------------------------------------

    def from_yaml_to_dict(yamlStr: str) -> dict:
        """
        Load pipeline from yaml string representation.

        Parameters
        ----------
        yamlStr : str
            String representation of pipeline YAML

        Returns
        -------
        """
        output = yaml.load(yamlStr, Loader=yaml.FullLoader)
        return output

    def __merge_yaml_dict(
        main_yaml: dict,
        to_be_imported_yaml: dict,
        to_be_imported_yaml_file_name: str = None,
    ) -> dict:
        """Generalized merge of YAML dictionary

        Args:
            main_yaml (dict): The main YAML to execute in this pipeline
            to_be_imported_yaml (dict): The YAML file to be imported

        Raises:
            ValueError: If there are type mismatch between main_yaml and to_be_imported_yaml

        Returns:
            dict: Merged YAML properties
        """
        # Reference: https://stackoverflow.com/a/58742155/9168936
        for key, val in main_yaml.items():

            if key in to_be_imported_yaml and type(to_be_imported_yaml[key]) != type(
                val
            ):
                if isinstance(to_be_imported_yaml[key], type(None)):
                    continue
                else:
                    raise ValueError(
                        f"Type mismatch in imported YAML file. Expected for property '{key}' type '{type(val)}' but got type '{type(to_be_imported_yaml[key])}'"
                    )

            if isinstance(val, dict):
                if key in to_be_imported_yaml:
                    main_yaml[key].update(
                        Pipeline.__merge_yaml_dict(
                            main_yaml=main_yaml[key],
                            to_be_imported_yaml=to_be_imported_yaml[key],
                            to_be_imported_yaml_file_name=to_be_imported_yaml_file_name,
                        )
                    )
            elif isinstance(val, list):
                if key in to_be_imported_yaml:
                    # Add imported list items to the beginning of the list
                    main_yaml[key] = to_be_imported_yaml[key] + main_yaml[key]
            elif isinstance(val, str):
                if key in to_be_imported_yaml:
                    numberOfLines = val.count("\n")
                    if numberOfLines > 0:
                        # Add imported text to the beginning of multi-line text
                        main_yaml[key] = (
                            (
                                f"# Below imported from: {to_be_imported_yaml_file_name}\n"
                                if to_be_imported_yaml_file_name is not None
                                else ""
                            )
                            + to_be_imported_yaml[key]
                            + (
                                f"\n# Above imported from: {to_be_imported_yaml_file_name}\n"
                                if to_be_imported_yaml_file_name is not None
                                else ""
                            )
                            + main_yaml[key]
                        )
                    else:
                        # Else replace entire text with incoming text
                        main_yaml[key] = to_be_imported_yaml[key]
            else:
                if key in to_be_imported_yaml:
                    # Replace value entirely (this should be only for numerical fields)
                    main_yaml[key] = to_be_imported_yaml[key]

        for key, val in to_be_imported_yaml.items():
            if key not in main_yaml:
                # Set new properties that are not in main_yaml but in to_be_imported_yaml
                main_yaml[key] = val

        return main_yaml

    def resolve_imports(yamlData: dict) -> dict:
        """This function will resolve imports if any

        Args:
            yamlData (dict): Input yaml to process imports (could be main yaml or an imported yaml with nested imports)

        Raises:
            ValueError: If file extension is not .yml or .yaml
            FileNotFoundError: If import YAML not found

        Returns:
            dict: Returns a processed YAML with all imports loaded
        """
        if "imports" in yamlData.keys():
            for imp in yamlData.get("imports", []):
                if os.path.exists(imp):
                    if imp.endswith((".yml", ".yaml")):
                        with open(imp) as f:
                            traceInfo(f"Importing file: {imp}")
                            import_yamlData = Pipeline.from_yaml_to_dict(f)
                    else:
                        raise ValueError(f"Wrong file extension for the import: {imp}")
                else:
                    raise FileNotFoundError(f"No such file: {imp}")

                # Run nested import
                import_yamlData = Pipeline.resolve_imports(import_yamlData)

                # Generalized merge of YAML properties
                yamlData = Pipeline.__merge_yaml_dict(
                    main_yaml=yamlData,
                    to_be_imported_yaml=import_yamlData,
                    to_be_imported_yaml_file_name=imp,
                )

        return yamlData

    # endregion Static functions

    # -------------------------------------------------------------------------

    # region Nested Classes

    @classtrace
    class _Variables(object):
        """
        A simple placeholder class for variables.

        It is only used to dynamically pull variable names using the `var` global variable

        The internal dictionary of this class (i.e. `__dict__`) is used to access variables
        """

        def __init__(self, vars: dict = {}):
            # Merge existing object's properties with incoming properties
            self.__dict__.update(vars)

        def get_names(self) -> list[str]:
            return self.__dict__.keys()

    # ---------------------------------

    @classtrace
    class _Connections(object):
        """
        A simple placeholder class for connections.

        It is only used to dynamically pull connections names using the `conn` global variable

        The internal dictionary of this class (i.e. `__dict__`) is used to access connections
        """

        def __init__(self, conns: dict = {}):
            connectionDictionary = {
                connName: create_engine(url=_processStringForExpressions(input=connObj))
                if isinstance(connObj, str)
                else create_engine(**_processStringForExpressions(input=connObj))
                for connName, connObj in conns.items()
            }
            # Merge existing object's properties with incoming properties
            self.__dict__.update(connectionDictionary)

    # @classtrace
    class _Steps(object):
        def __init__(self, steps: list = []):
            self._dg = nx.DiGraph()

            for stepDefinition in steps:
                stepObj = Pipeline._Steps._Step(
                    stepDefinition=stepDefinition,
                )

                # Do not replace function, only step name
                stepObj.name = self.__setup_dependencies_from_string_input(
                    input=stepObj.name,
                    input_type="stepName",
                    stepName=stepObj.name,
                )
                self._dg.add_node(node_for_adding=stepObj.name, **{"stepObj": stepObj})

                self.__setup_dependencies_from_string_input(
                    input=stepObj.function,
                    input_type="function",
                    stepName=stepObj.name,
                )

                if stepObj.args is not None and isinstance(stepObj.args, dict):
                    # Do not replace arg. Just track dependency
                    for arg, value in stepObj.args.items():
                        self.__setup_dependencies_from_string_input(
                            input=value, input_type="args", stepName=stepObj.name
                        )
                elif stepObj.args is not None and isinstance(stepObj.args, list):
                    for value in stepObj.args:
                        self.__setup_dependencies_from_string_input(
                            input=value, input_type="args", stepName=stepObj.name
                        )
                elif stepObj.args is not None:
                    self.__setup_dependencies_from_string_input(
                        input=stepObj.args, input_type="args", stepName=stepObj.name
                    )

                # Merge existing object's properties with incoming properties
                self.__dict__.update({stepObj.name: stepObj})

            try:
                graph_dependency_cycles = list(
                    nx.find_cycle(self._dg, orientation="original")
                )
                if any(graph_dependency_cycles):
                    raise RuntimeError(
                        f"Found cycles in dependencies of steps. Check this dependency cycle: {graph_dependency_cycles}"
                    )
            except nx.NetworkXNoCycle:
                traceInfo(
                    "No cycles detected in dependency graph! This is good to have.",
                    logLevel=logging.DEBUG,
                )

        def __setup_dependencies_from_string_input(
            self, input: str or list, input_type: str, stepName: str
        ) -> str:

            if isinstance(input, str):
                expression_regex = re.compile(
                    r"\$\{(.*)steps\[(.*?)\]\.output(\.)?(\w*?)(.*)\}"
                )
                expression_matched = re.findall(pattern=expression_regex, string=input)
                if len(expression_matched) > 0:

                    for (
                        before_steps_keyword,
                        step_name_in_brackets,
                        expression_to_call_referenced_function,
                        referenced_function_name,
                        after_steps_referenced_function,
                    ) in expression_matched:

                        dependentStepName = step_name_in_brackets
                        dependentStepName = dependentStepName.strip()
                        dependentStepName = dependentStepName.strip('"')
                        dependentStepName = dependentStepName.strip("'")

                        newStepNamePart = expression_to_call_referenced_function.join(
                            [dependentStepName, after_steps_referenced_function]
                        ).strip()

                        if dependentStepName not in self._dg:
                            raise ValueError(
                                f"_Step name '{dependentStepName}' not found. "
                                f"Expected it to be defined before processing '{input}'. "
                                f"Change the order of steps so that '{dependentStepName}' is defined before processing '{input}."
                            )

                        input = input.replace(
                            "${"
                            + before_steps_keyword
                            + "steps["
                            + step_name_in_brackets
                            + "].output"
                            + expression_to_call_referenced_function
                            + referenced_function_name
                            + after_steps_referenced_function
                            + "}",
                            newStepNamePart,
                        )

                    if input_type == "stepName":
                        self._dg.add_edge(dependentStepName, input)
                    elif input_type == "args" or input_type == "function":
                        self._dg.add_edge(dependentStepName, stepName)
                return input
            elif isinstance(input, list):
                for i in range(len(input)):
                    if not isinstance(input[i], str):
                        continue
                    else:
                        self.__setup_dependencies_from_string_input(
                            input=input[i], input_type=input_type, stepName=stepName
                        )
            else:
                return input

        # @classtrace
        class _Step(object):
            def __init__(
                self,
                stepDefinition: dict = {},
            ):
                if not isinstance(stepDefinition, dict):
                    raise ValueError(
                        "Expected step to be like a dictionary of keys:value pairs"
                    )

                if len(stepDefinition.keys()) == 1:
                    stepName = list(stepDefinition.keys())[0]
                    stepDefinition = {
                        "name": stepName,
                        "function": stepName,
                        # the sub-properties at this dictionary key
                        "args": stepDefinition.get(stepName, {}),
                    }

                # Create step definition with default values if none provided from user
                # This ensures properties exist for downstream processing
                defaultStepDefinitionValues = {
                    "args": {},
                    "resumeFromSaved": True,
                    "saveProgress": "",
                }

                stepDefinition = Pipeline._Pipeline__merge_yaml_dict(
                    main_yaml=defaultStepDefinitionValues,
                    to_be_imported_yaml=stepDefinition,
                )

                # Merge existing object's properties with incoming properties
                self.__dict__.update(stepDefinition)

            def run(self) -> None:
                functionHandle = _processStringForExpressions(input=self.function)
                if isinstance(functionHandle, str):
                    functionHandle = eval(functionHandle)

                traceInfo(f"Starting pipeline steps['{self.name}']")

                # Always set arguments to empty dictionary
                self.args = {} if self.args is None else self.args
                # Interpret all the arguments for any evaluated expressions
                self.args = _processStringForExpressions(input=self.args)

                if isinstance(self.args, dict):
                    self.output = functionHandle(**self.args)
                elif isinstance(self.args, list):
                    self.output = functionHandle(*self.args)
                else:
                    self.output = functionHandle(self.args)

                if self.saveProgress:
                    path_or_buf = _processStringForExpressions(self.saveProgress)
                    dataframe = self.output
                    if path_or_buf.split(".")[-1] == "csv":
                        dataframe.to_csv(path_or_buf)
                        traceInfo(
                            f"Saving output of steps['{self.name}'] as CSV to: {path_or_buf}"
                        )

                traceInfo(f"Finished pipeline steps['{self.name}']")

            # -------------------------------------------------------------------------

        def run(self):
            """Run all the steps in the pipeline

            Args:
                nodeName (str, optional): The starting node. Defaults to None.
            """
            tqdm_list = tqdm(
                total=len(self._dg.nodes),
                unit="node",
                desc="YAML step",
                colour="green",
            )
            while any(self._dg.nodes):
                nodeNames = [
                    # Get all nodes that have no dependencies
                    node
                    for node in self._dg.nodes
                    if self._dg.in_degree(node) == 0
                ]
                tqdm_list.set_postfix_str(nodeNames)

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    for node in nodeNames:
                        # Check if the step has already been executed before and resumeFromSaved
                        if self[node].resumeFromSaved and os.path.exists(
                            path=_processStringForExpressions(self[node].saveProgress)
                        ):
                            self[node].output = pd.read_csv(
                                _processStringForExpressions(self[node].saveProgress)
                            )
                            traceInfo(
                                f"Skipped execution of pipeline steps['{self[node].name}'], retrieved from '{self[node].saveProgress}' of previous execution"
                            )

                        else:
                            # Run this step
                            executor.submit(self[node].run)

                        # Remove the node from the Directed Graph:
                        #   This means that we run nodes with no dependencies first and remove from them graph after execution
                        #   and continue to discover more nodes with no dependencies until there are no more nodes left
                        self._dg.remove_node(node)

                    executor.shutdown(wait=True)

                tqdm_list.update(len(nodeNames))

        # region Python data slicers for accessing properties dynamically

        # Reference: https://docs.python.org/3/reference/datamodel.html#object.__getitem__
        def __getitem__(self, name):
            return getattr(self, name)

        # Reference: https://docs.python.org/3/reference/datamodel.html#object.__setitem__
        def __setitem__(self, name, value):
            return setattr(self, name, value)

        # Reference: https://docs.python.org/3/reference/datamodel.html#object.__delitem__
        def __delitem__(self, name):
            return delattr(self, name)

        # Reference: https://docs.python.org/3/reference/datamodel.html#object.__contains__
        def __contains__(self, name):
            return hasattr(self, name)

        # endregion Python data slicers for accessing properties dynamically

        # -------------------------------------------------------------------------

    # endregion Nested Classes

    # -------------------------------------------------------------------------

    # region Execution Methods

    def run(self) -> None:
        self.steps.run()

    # endregion Execution Methods

    # -------------------------------------------------------------------------
