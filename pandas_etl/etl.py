import os
from shutil import ExecError
import yaml
from calltraces.linetrace import traceInfo, traceError
from calltraces.functiontrace import functiontrace
from calltraces.classtrace import classtrace
from tqdm.auto import tqdm
import re
from sqlalchemy import create_engine
import concurrent.futures
import asyncio


def parse_command_line_variables(variables: list[str] = []) -> dict[str, str]:
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


def _processStringForExpressions(input: str | dict) -> str | dict:
    """
    Private function to process a string and replace placeholder `${expression}` with `expression value`

    Args:
        input (str|dict): Input string or dictionary

    Returns:
        str: Output string or dictionary (always same time as input)
    """
    if type(input) == str:
        # Reference: https://docs.python.org/3/howto/regex.html#greedy-versus-non-greedy
        expression_regex = re.compile(r"\$\{(.*?)\}")
        expression_matched = re.findall(pattern=expression_regex, string=input)
        output = input
        for exp in expression_matched:
            output = re.sub(
                pattern=r"\$\{" + exp + r"\}",
                repl=str(eval(exp)),
                string=output,
            )
        return output
    elif input is dict:
        output = {k: _processStringForExpressions(v) for k, v in input.items()}
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
        yamlData: str | dict,
        overrideImports: list = [],
        overrideVariables: dict[str, str] = {},
    ):
        """
        Initialize Pipeline class

        Args:
            yamlData (str | dict): Either file name of YAML or the YAML directory
            overrideImports (list, optional): imports to add from YAML. Defaults to [].
            overrideVariables (dict[str, str], optional): variables to override from YAML. Defaults to {}.
        """

        if type(yamlData) == str:
            # Parse YAML string to in-memory object
            yamlData = Pipeline.from_yaml_to_dict(yamlStr=yamlData)
            traceInfo(f"Main YAML definition loaded from: {yamlData}")

        if overrideImports:
            # Properties we parse from command line or expect code to have
            argumentProperties = {
                "imports": overrideImports,
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
        self.variables = Pipeline.Variables(vars=properties.get("variables", {}))

        # Cerate a global called `var` such that:
        #   Given `{varName:varValue}` dictionary
        #   And saved as Pipeline.Variables class
        #   Then `var.varName` evauluates to `varValue`
        globals()["var"] = self.variables
        # TODO: @rrmistry/@msuthar to discuss if `var` can be renamed to `variables` to synchronize YAML with globals()

        # Set preFlight property to this Class
        setattr(self, "preFlight", properties.get("preFlight", {}))
        exec(self.preFlight.get("script", ""), globals())

        # Set connections property for this Class to help resolve variable values
        self.connections = Pipeline.Connections(conns=properties.get("connections", {}))

        # Cerate a global called `conn` such that:
        #   Given `{connName:connObj}` dictionary
        #   And saved as Pipeline.Connections class
        #   Then `conn.connName` evauluates to `connObj`
        globals()["conn"] = self.connections
        # TODO: @rrmistry/@msuthar to discuss if `conn` can be renamed to `connections` to synchronize YAML with globals()

        traceInfo(f"Successfully loaded pipeline!")

    def run() -> None:
        # TODO: @rrmistry/@msuthar to discuss
        raise NotImplementedError("Run is not implemented yet")

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
                raise ValueError(
                    f"Type mismatch in imported YAML file. Expected for property '{key}' type '{type(val)}' but got type '{type(to_be_imported_yaml[key])}'"
                )

            if type(val) == dict:
                if key in to_be_imported_yaml:
                    main_yaml[key].update(
                        Pipeline.__merge_yaml_dict(
                            main_yaml=main_yaml[key],
                            to_be_imported_yaml=to_be_imported_yaml[key],
                            to_be_imported_yaml_file_name=to_be_imported_yaml_file_name,
                        )
                    )
            elif type(val) == list:
                if key in to_be_imported_yaml:
                    # Add imported list items to the beginning of the list
                    main_yaml[key] = to_be_imported_yaml[key] + main_yaml[key]
            elif type(val) == str:
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
                                +f"\n# Above imported from: {to_be_imported_yaml_file_name}\n"
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
            if not key in main_yaml:
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

    # region Nested Classes

    @classtrace
    class Variables(object):
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
    class Connections(object):
        """
        A simple placeholder class for connections.

        It is only used to dynamically pull connections names using the `conn` global variable

        The internal dictionary of this class (i.e. `__dict__`) is used to access connections
        """

        def __init__(self, conns: dict = {}):
            connectionDictionary = {
                connName: create_engine(url=_processStringForExpressions(input=connObj))
                if type(connObj) == str
                else create_engine(**_processStringForExpressions(input=connObj))
                for connName, connObj in conns.items()
            }
            # Merge existing object's properties with incoming properties
            self.__dict__.update(connectionDictionary)

    @classtrace
    class Steps(object):
        def __init__(self, steps: list = []):
            pass

        pass

    # endregion Nested Classes

    # -------------------------------------------------------------------------


@functiontrace
def create_engine_connection(conn: dict, yamlData: dict):
    """This function will find and create engine connection"""
    conn["engine"] = create_engine(conn["connStr"])
    traceInfo(f"Created an engine for connection: {conn['name']}")


@functiontrace
def find_and_execute_script(yamlData: dict):
    """This function will find and execute script under preFlight"""
    script = yamlData.get("preFlight", {}).get("script", "")
    try:
        exec(script, globals())
        traceInfo(f"Finished executing script from preFlight!")
    except:
        raise ExecError(f"Failed to execute script: {script}")


@functiontrace
def find_key_by_value_and_assign(data: dict, target: str, assign) -> dict:
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
                elif isinstance(i, list):
                    y = 0
                    for z in i:
                        if z == target:
                            data[k][x][y] = assign
                            break
                        y += 1
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
        iterable=yamlData["steps"],
        unit=" function",
        desc="YAML Steps",
        colour="green",
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
                args = None
            else:
                args = step.get(function, [])
                kwargs = None
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
                    args = None
                else:
                    args = step.get("args", [])
                    kwargs = None
        else:
            raise ValueError(f"Invalid step type: {step}")

        funcHandle = eval(function)
        if args is not None:
            traceInfo(f"Executing function: '{function_name}', with arguments: {args}")
            try:
                output = funcHandle(args)
            except:
                raise ExecError(
                    f"Error executing function: {function_name}, , with arguments: {args}"
                )
        elif kwargs is not None:
            traceInfo(
                f"Executing function: '{function_name}', with arguments: {kwargs}"
            )
            try:
                output = funcHandle(kwargs)
            except:
                raise ExecError(
                    f"Error executing function: {function_name}, with arguments: {kwargs}"
                )
        else:
            traceInfo(f"Executing function: '{function_name}', with no arguments")
            try:
                output = funcHandle()
            except:
                raise ExecError(
                    f"Error executing function: {function_name}, with no arguments"
                )

        # Assign the output of the function to the step output
        step["output"] = output
        traceInfo(f"Successfully executed step and stored as output!")
    traceInfo(f"Successfully executed all the steps!")


####################################### Class Based Functions #####################################


@functiontrace
def pandas_etl_test_pipeline(data: str, var: list = [], imports: list = []):

    traceInfo(f"Starting pipeline execution")

    yamlData = YamlData(data)
    # yamlData = yamlData.to_yaml()

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


@classtrace
class YamlData:
    def __init__(self, data):
        self.data = data
        self.to_yaml()

    def to_yaml(self) -> dict:
        """Open a YAML file from the specified file path"""
        try:
            self.yamldata = yaml.load(self.data, Loader=yaml.FullLoader)
            traceInfo(f"The yaml config is loaded!")
            return self.yamldata
        except:
            raise ValueError(f"Wrong input")

    def to_lower(yamlData: dict) -> dict:
        """This function converts the specified yaml data into lower case"""
        yamlData_lower = {}
        for k, v in yamlData.items():
            yamlData_lower[k.lower()] = v
            if isinstance(v, dict):
                yamlData_lower[k.lower()] = to_lower(v)
            elif isinstance(v, list):
                for i in v:
                    if isinstance(i, dict):
                        yamlData_lower[k.lower()] = to_lower(i)
        return yamlData_lower

    def classification(self):
        if "variables" in self.yamldata.keys():
            for k, v in self.yamldata["variables"].items():
                k = Variables(k, v)

        if "imports" in self.yamldata.keys():
            for i in self.yamldata["imports"]:
                imps = [Imports(i)]

        if "connections" in self.yamldata.keys():
            for k, v in (i.items() for i in self.yamldata["connections"]):
                k = Connections(k, v)

        if "preFlight" in self.yamldata.keys():
            for k, v in self.yamldata["preFlight"].items():
                k = preFlight(v)

        if "steps" in self.yamldata.keys():
            for i in self.yamldata["steps"]:
                istep = [Steps(i)]


@classtrace
class Imports(YamlData):
    def __init__(self, path):
        self.path = path

    def add_argument_imports(imports: str, yamlData: dict):
        """This function adds imports defined in the arguments to the yaml file imports"""
        yamlData["imports"] = [imports] + yamlData["imports"]
        traceInfo(f"Added imports: {imports}, at the top of yaml config")

    def resolve_imports(yamlData: dict) -> dict:
        """This function will resolve imports if any"""
        if "imports" in yamlData.keys():
            for imp in yamlData.get("imports", []):
                if os.path.exists(imp):
                    if imp.endswith((".yml", ".yaml")):
                        with open(imp) as f:
                            import_yamlData = yaml.load(f, Loader=yaml.FullLoader)
                            traceInfo(f"Imported file: {imp}")
                    else:
                        raise ValueError(f"Wrong file extension for the import: {imp}")
                else:
                    raise FileNotFoundError(f"No such file: {imp}")
                for key in yamlData.keys():
                    if key == "imports":
                        continue
                    elif key in ["steps", "connections"]:
                        if key not in import_yamlData.keys():
                            import_yamlData[key] = []
                        import_yamlData.get(key).extend(yamlData.get(key, []))
                        traceInfo(
                            f"Appended {key} property from import file {imp} to the top"
                        )
                    else:
                        if key not in import_yamlData.keys():
                            import_yamlData[key] = {}
                        import_yamlData.get(key).update(yamlData.get(key, {}))
                        traceInfo(f"Updated {key} property from import file {imp}")
                if "imports" in import_yamlData.keys():
                    import_yamlData = resolve_imports(import_yamlData)
                return import_yamlData
        return yamlData


@classtrace
class Connections(YamlData):
    def __init__(self, name, connection_string):
        self.name = name
        self.connStr = connection_string

    def create_engine_connection(self):
        """This function will find and create engine connection"""
        self.engine = create_engine(self.connStr)
        traceInfo(f"Created an engine for connection: {self.name}")

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


@classtrace
class preFlight(YamlData):
    def __init__(self, script):
        self.script = script

    def find_and_execute_script(self):
        """This function will find and execute script under preFlight"""
        try:
            exec(self.script, globals())
            traceInfo(f"Finished executing script from preFlight!")
        except:
            raise ExecError(f"Failed to execute script: {self.script}")


@classtrace
class Steps(YamlData):
    def __init__(self, step):
        self.step = step

        if "name" in step.keys():
            self.name = self.step["name"]
        else:
            self.name = list(self.step.keys())[0]

        self.description = self.step.get("description", "No description provided")

        # Check if the step requires output from the previous steps
        steps_output_regex = re.compile(r"\$\{steps\[\'(.*?)\'\]\.output\}")
        steps_output_matched = re.findall(
            steps_output_regex, string=yaml.dump(self.step)
        )
        if len(steps_output_matched) > 0:
            self.step = replace_steps_output(
                steps_output_matched, self.step, self.yamldata
            )
            traceInfo(f"Successfully replaced outputs needed for step!")

        # Check if the step execution requires connections engine to the database
        conn_regex = re.compile(r"\$\{conn\.(.*)\}")
        conn_matched = re.findall(conn_regex, string=yaml.dump(self.step))
        if len(conn_matched) > 0:
            self.step = resolve_connections_variables(
                conn_matched, self.step, self.yamldata
            )
            traceInfo(f"Successfully replaced connections needed for step!")

        # Check if the step has only one property
        if len(self.step.keys()) == 1:
            self.function = list(self.step.keys())[0]
            if type(self.step.get(self.function)) == dict:
                self.kwargs = self.step.get(self.function, {})
                args = None
            else:
                self.args = self.step.get(self.function, [])
                kwargs = None
            function_regex = re.compile(r"steps\[\'(.*?)\'\]\.output")
            function_matched = re.findall(function_regex, self.function)
            if len(function_matched) > 0:
                self.function_object = return_function_object(
                    function_matched, self.yamldata
                )
                self.step["name"] = (
                    self.function_object + "." + self.function.split(".")[-1]
                )
                self.function = getattr(
                    self.function_object, self.function.split(".")[-1]
                )
        elif "function" in self.step.keys():
            if type(self.step["function"]) is not dict:
                self.method = self.step["function"]
                self.function = f"{self.method}"
            else:
                self.method = self.step["function"]["name"]
                self.function = f"{self.step['function']['object']}.{self.method}"

            if "args" in self.step.keys():
                if type(self.step.get("args")) == dict:
                    self.kwargs = self.step.get("args", {})
                    args = None
                else:
                    self.args = self.step.get("args", [])
                    kwargs = None
        else:
            raise ValueError(f"Invalid step type: {self.step}")

    def execute_steps(yamlData: dict):
        """This function executes the steps specified"""
        tqdm_function_list = tqdm(
            iterable=yamlData["steps"],
            unit=" function",
            desc="YAML Steps",
            colour="green",
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
                    args = None
                else:
                    args = step.get(function, [])
                    kwargs = None
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
                        args = None
                    else:
                        args = step.get("args", [])
                        kwargs = None
            else:
                raise ValueError(f"Invalid step type: {step}")

            funcHandle = eval(function)
            if args is not None:
                traceInfo(
                    f"Executing function: '{function_name}', with arguments: {args}"
                )
                try:
                    output = funcHandle(args)
                except:
                    raise ExecError(
                        f"Error executing function: {function_name}, , with arguments: {args}"
                    )
            elif kwargs is not None:
                traceInfo(
                    f"Executing function: '{function_name}', with arguments: {kwargs}"
                )
                try:
                    output = funcHandle(kwargs)
                except:
                    raise ExecError(
                        f"Error executing function: {function_name}, with arguments: {kwargs}"
                    )
            else:
                traceInfo(f"Executing function: '{function_name}', with no arguments")
                try:
                    output = funcHandle()
                except:
                    raise ExecError(
                        f"Error executing function: {function_name}, with no arguments"
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
