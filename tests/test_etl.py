import logging
import os
import pytest
from pandas_etl import etl
import sqlalchemy
import uuid
from calltraces.linetrace import traceWarning


@pytest.fixture(scope="session")
def file(pytestconfig):
    return pytestconfig.getoption("file")


@pytest.fixture(scope="session")
def imports(pytestconfig):
    return pytestconfig.getoption("imports")


@pytest.fixture(scope="session")
def var(pytestconfig):
    return pytestconfig.getoption("var")


class TestCommandLineArguments:
    # Usage: pytest -v -s tests/test_etl.py --file "./tests/mockup.yaml" --var "varName1=varValue1" --imports "xyz.yaml"

    def test_cl_argument_file(self, file):
        if file:
            assert file == "./tests/mockup.yaml"

    def test_cl_argument_imports(self, imports):
        if imports:
            assert imports == ["xyz.yaml"]

    def test_cl_argument_variables(self, var):
        if var:
            assert var == ["varName1=varValue1"]


class TestAddArgumentVariables:
    def setup_class(self):
        self.overrideVariables = {
            "database": str(uuid.uuid4()),
            "server": str(uuid.uuid4()),
        }
        self.pipelineTestObj = etl.Pipeline(
            yamlData="""
            imports:
            - ./tests/etl_definition_folder/variables/postgresql_database_variables.yaml
            """,
            overrideVariables=self.overrideVariables,
        )
        self.commandLineVariables = ["var1=value1", "var2=value2"]

    def test_add_argument_variables(self):
        """Confirm that variable override of different YAML files will result in a final merged variable list"""
        assert self.pipelineTestObj.variables.server == self.overrideVariables["server"]
        assert (
            self.pipelineTestObj.variables.database
            == self.overrideVariables["database"]
        )

    def test_parse_command_line_variables(self):
        result = etl.parse_command_line_variables(self.commandLineVariables)
        expected = {"var1": "value1", "var2": "value2"}
        assert result == expected

    def test_invalid_variable(self):
        with pytest.raises(ValueError) as error:
            etl.parse_command_line_variables(["var1=value1=value2"])
        assert (
            error.value.args[0]
            == "Invalid command line for variable 'var1=value1=value2' Expected format as varName=varValue"
        )

    def test_unknown_variable(self):
        with pytest.raises(AttributeError) as error:
            # No variables defined
            self.pipelineTestObj2 = etl.Pipeline(
                yamlData="""
                imports:
                - ./tests/etl_definition_folder/variables/postgresql_database_variables.yaml
                connections:
                  my_source: postgresql+psycopg2://${var.host}/${var.database}
                """
            )
        assert error.value.args[0] == "'_Variables' object has no attribute 'host'"


class TestAddArgumentImports:
    def setup_class(self):
        self.pipelineTestObj = etl.Pipeline(
            yamlData="""
            imports:
            - ./tests/etl_definition_folder/variables/postgresql_database_variables.yaml
            - ./tests/etl_definition_folder/connections/postgresql_sql_connections.yaml
            """,
            includeImports=[
                "./tests/etl_definition_folder/variables/secrets/postgresql_database-secret_variables.yaml"
            ],
        )

    def test_add_argument_imports(self):
        """Confirm that imports of different YAML files will result in a final merged variable list"""
        assert set(list(self.pipelineTestObj.variables.get_names())) == set(
            [
                "server",
                "database",
                "username",
                "password",
            ]
        )


class TestCreateEngineConnection:
    def setup_class(self):
        self.pipelineTestObj = etl.Pipeline(
            yamlData="""
            imports:
            - ./tests/etl_definition_folder/variables/postgresql_database_variables.yaml
            connections:
              my_database: postgresql+psycopg2://${var.username}:${var.password}@${var.server}:${var.postgresql_port}/${var.database}
            """,
            includeImports=[
                "./tests/etl_definition_folder/variables/secrets/postgresql_database-secret_variables.yaml"
            ],
            overrideVariables={"postgresql_port": 9999},
        )

    def test_create_engine_connection(self):
        assert (
            type(self.pipelineTestObj.connections.my_database)
            == sqlalchemy.engine.base.Engine
        )
        assert (
            str(self.pipelineTestObj.connections.my_database)
            == "Engine(postgresql+psycopg2://postgres:***@localhost:9999/pandas_etl_test_db)"
        )


class TestPipelineRun:
    def test_run_pipeline(self):
        pipelineObj = etl.Pipeline(
            yamlData={
                "preFlight": {
                    "script": "def python_function_A(inputA: int = 0, inputB: int = 0, inputC: int = 0) -> int:\n    return inputA + inputB + inputC\n\ndef python_function_B(inputA: int = 0, inputB: int = 0, inputC: int = 0) -> int:\n    return inputA + inputB - inputC\n\ndef python_function_C(inputA: int = 1, inputB: int = 1, inputC: int = 1) -> int:\n    return inputA * inputB * inputC\n"
                },
                "steps": [
                    {"python_function_A": {"inputA": 1, "inputB": 2, "inputC": 3}},
                    {"python_function_B": {"inputA": 3, "inputB": 2, "inputC": 1}},
                    {"python_function_C": {"inputA": 2, "inputB": 3, "inputC": 1}},
                    {
                        "name": "finalOutputOne",
                        "function": "python_function_A",
                        "args": {
                            "inputA": "${steps['python_function_A'].output}",
                            "inputB": "${steps['python_function_B'].output}",
                            "inputC": "${steps['python_function_C'].output}",
                        },
                    },
                    {
                        "name": "finalOutputTwo",
                        "function": "python_function_C",
                        "args": {
                            "inputA": "${steps['python_function_C'].output}",
                            "inputB": "${steps['python_function_B'].output}",
                            "inputC": "${steps['python_function_A'].output}",
                        },
                    },
                ],
            }
        )
        pipelineObj.run()
        assert pipelineObj.steps["finalOutputOne"].output == (
            (1 + 2 + 3) + (3 + 2 - 1) + (2 * 3 * 1)
        )
        assert pipelineObj.steps["finalOutputTwo"].output == (
            (1 + 2 + 3) * (3 + 2 - 1) * (2 * 3 * 1)
        )

    def test_run_pandas_pipeline(self):
        # No variables defined
        pipelineObj = etl.Pipeline(
            yamlData="""
            # Define user functions that should be included in tests
            preFlight:
              script: |
                import os
                import pandas as pd

            steps:
            - pd.read_csv:
                filepath_or_buffer: ./tests/data/test.csv

            - ${ steps['pd.read_csv'].output.groupby }:
                by: AB

            - ${ steps['pd.read_csv.groupby'].output.max }:

            - ${ steps['pd.read_csv.groupby.max'].output.to_csv }:
                path_or_buf: ${os.path.dirname(steps['pd.read_csv'].args['filepath_or_buffer'])}/output_in_same_folder_as_input.csv
            """
        )

        # Define the path of the expected output file
        expected_output_file_path = "./tests/data/output_in_same_folder_as_input.csv"

        # Remove the file before running pipeline
        if os.path.exists(path=expected_output_file_path):
            os.remove(path=expected_output_file_path)

        # Run the pipeline
        pipelineObj.run()

        # Check that the output file has been created
        assert os.path.exists(path=expected_output_file_path)

        # Delete the output file for cleanup
        os.remove(path=expected_output_file_path)

    def test_mismatch_type(self):
        with pytest.raises(ValueError) as error:
            # No variables defined
            etl.Pipeline(
                yamlData="""
                steps:
                - name:             pd.read_csv.groupby_Instance1
                function:         long_running_function
                args:
                   - df:             ${ steps['pd.read_csv'].output }",
                """,
                includeImports=[
                    "./tests/etl_definition_folder/pipelines/pandas_pipeline_recovery_1.yaml"
                ],
            )
        assert (
            error.value.args[0]
            == "Type mismatch in imported YAML file. Expected for property 'args' type '<class 'dict'>' but got type '<class 'str'>'"
        )


class TestPipelineRunRecovery:
    def test_run_pandas_recovery_pipeline(self, caplog):
        root_yaml_folder = "./tests/etl_definition_folder/pipelines"
        root_yaml_file_path = f"{root_yaml_folder}/pandas_pipeline_recovery_1.yaml"
        # Define the path of the expected output file
        expected_output_file_path = "./tests/data/max.csv"
        expected_output_file_path1 = "./tests/data/groupby_Instance1.csv"
        expected_output_file_path2 = "./tests/data/groupby_Instance2.csv"

        # Remove files before running pipeline
        if os.path.exists(path=expected_output_file_path):
            os.remove(path=expected_output_file_path)
        if os.path.exists(path=expected_output_file_path1):
            os.remove(path=expected_output_file_path1)
        if os.path.exists(path=expected_output_file_path2):
            os.remove(path=expected_output_file_path2)

        # Run the pipeline first time
        # Reference: https://docs.pytest.org/en/6.2.x/logging.html#caplog-fixture
        with caplog.at_level(logging.INFO):
            pipelineObj = etl.Pipeline(yamlData=root_yaml_file_path)
            pipelineObj.run()

            assert (
                any(
                    [
                        record
                        for record in caplog.records
                        if record.message == "Starting long_running_function..."
                    ]
                )
                is True
            )

        assert os.path.exists(path=expected_output_file_path)

        if not os.path.exists(path=expected_output_file_path):
            traceWarning(f"Expected file {expected_output_file_path} to exist")
        else:
            # Delete the output file for cleanup
            os.remove(path=expected_output_file_path)

        # Remove all the logs from records
        caplog.clear()

        # Run the pipeline second time
        # Reference: https://docs.pytest.org/en/6.2.x/logging.html#caplog-fixture
        with caplog.at_level(logging.INFO):
            pipelineObj = etl.Pipeline(yamlData=root_yaml_file_path)
            pipelineObj.run()

            assert (
                any(
                    [
                        record
                        for record in caplog.records
                        if record.message == "Starting long_running_function..."
                    ]
                )
                is False
            )

        # Remove files after running pipeline
        if os.path.exists(path=expected_output_file_path):
            os.remove(path=expected_output_file_path)
        if os.path.exists(path=expected_output_file_path1):
            os.remove(path=expected_output_file_path1)
        if os.path.exists(path=expected_output_file_path2):
            os.remove(path=expected_output_file_path2)
