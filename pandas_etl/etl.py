import inspect
import os
from shutil import ExecError
import yaml
from calltraces.linetrace import traceInfo, traceError
from calltraces.functiontrace import functiontrace
from calltraces.classtrace import classtrace
from calltraces import commonTraceSettings
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
        includeImports: list = [],
        overrideVariables: dict[str, str] = {},
    ):
        """
        Initialize Pipeline class

        Args:
            yamlData (str | dict): Either file name of YAML or the YAML directory
            includeImports (list, optional): imports to add from YAML. Defaults to [].
            overrideVariables (dict[str, str], optional): variables to override from YAML. Defaults to {}.
        """

        if type(yamlData) == str:
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
        self.variables = Pipeline.Variables(vars=properties.get("variables", {}))

        # Cerate a global called `var` such that:
        #   Given `{varName:varValue}` dictionary
        #   And saved as Pipeline.Variables class
        #   Then `var.varName` evaluates to `varValue`
        globals()["var"] = self.variables
        # TODO: @rrmistry/@msuthar to discuss if `var` can be renamed to `variables` to synchronize YAML with globals()

        # Set preFlight property to this Class
        exec(self.__dict__.get("preFlight", {}).get("script", ""), globals())

        # Set connections property for this Class to help resolve variable values
        self.connections = Pipeline.Connections(conns=properties.get("connections", {}))

        # Cerate a global called `conn` such that:
        #   Given `{connName:connObj}` dictionary
        #   And saved as Pipeline.Connections class
        #   Then `conn.connName` evaluates to `connObj`
        globals()["conn"] = self.connections
        # TODO: @rrmistry/@msuthar to discuss if `conn` can be renamed to `connections` to synchronize YAML with globals()

        # Set steps property for this Class to help resolve values
        self.steps = Pipeline.Steps(steps=properties.get("steps", {}))

        # Cerate a global called `var` such that:
        #   Given `{varName:varValue}` dictionary
        #   And saved as Pipeline.Variables class
        #   Then `var.varName` evauluates to `varValue`
        globals()["steps"] = self.steps

        traceInfo(f"Successfully loaded pipeline!")

    async def run(self) -> None:
        # TODO: @rrmistry/@msuthar to discuss
        # raise NotImplementedError("Run is not implemented yet")

        masterLoop = asyncio.get_running_loop()

        # Just for out local debugging we want to print all objects going into and out of functions
        # This is not recommended for production workloads as objects can be huge in memory and fill up console output making it hard to interpret
        # commonTraceSettings.printArguments = True
        commonTraceSettings.printOutputs = True

        traceInfo(f"Starting setting up Futures")
        # Below will happen as-is in existing pandas-etl code
        tqdm_function_list = tqdm(
            iterable=steps,
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

            steps.i["output"] = Pipeline.get_function_future(
                loop=masterLoop, funcName=steps.i["function"], input=steps.i["args"]
            )

        traceInfo(f"Finished setting up Futures")

        traceInfo(f"Starting to evaluate future now")
        finalOutput = await Output
        traceInfo(f"Finished evaluating future now")

        print(f"Final output = {finalOutput}")

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

    # ----------------------------------------------------------------

    # region Futures

    async def run_function(fut: asyncio.Future, functionHandle: any, input: dict):

        # Evaluate other futures when executing
        inputEvaluated = {
            k: input[k]
            if not (
                asyncio.isfuture(input[k])
                or asyncio.iscoroutinefunction(input[k])
                or inspect.iscoroutine(input[k])
            )
            else await input[k]
            for k in input.keys()
        }

        value = functionHandle(**inputEvaluated)

        fut.set_result(value)

    def get_function_future(
        loop: asyncio.AbstractEventLoop, funcName: str, input: dict
    ) -> asyncio.Future:
        # Create a new Future object.
        functionFuture = loop.create_future()

        # Get the function handle as defined in Python
        functionHandle = eval(funcName)

        # Remove the symbol name if not already added
        if functionHandle.__name__ in commonTraceSettings.ignoreSymbols:
            commonTraceSettings.ignoreSymbols.remove(functionHandle.__name__)

        # Trace the function execution when it starts and finishes
        functionHandle = functiontrace(functionHandle)

        # Run "run_function()" coroutine in a parallel Task.
        # We are using the low-level "loop.create_task()" API here because
        # we already have a reference to the event loop at hand.
        # Otherwise we could have just used "asyncio.create_task()".
        loop.create_task(
            coro=Pipeline.run_function(
                fut=functionFuture, functionHandle=functionHandle, input=input
            )
        )

        return functionFuture

    # endregion Futures

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
        def __init__(self, steps: dict = {}):
            procssedStepsDictionary = {
                stepName: Pipeline.Steps.__processStep(step=stepDefinition)
                for stepName, stepDefinition in steps.items()
            }
            # Merge existing object's properties with incoming properties
            self.__dict__.update(procssedStepsDictionary)

        def __processStep(step: dict = {}):
            if type(step) != dict or len(step.keys()) <= 0:
                raise ValueError(
                    "Expected step to be like a dictionary of keys:value pairs"
                )

            if len(step.keys()) == 1:
                stepName = step.keys()[0]
                outputStep = {
                    "name": stepName,
                    "function": stepName,
                    "args": step[0],  # the sub-properties at this dictionary key
                }
            else:
                outputStep = step

            return outputStep

    # endregion Nested Classes

    # -------------------------------------------------------------------------
