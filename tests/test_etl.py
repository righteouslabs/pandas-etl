import os
import pytest
import yaml
from pandas_etl import etl
import sqlalchemy
import uuid
from sqlalchemy.engine.base import Engine as Engine


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

    def test_add_argument_variables(self):
        """Confirm that variable override of different YAML files will result in a final merged variable list"""
        assert self.pipelineTestObj.variables.server == self.overrideVariables["server"]
        assert (
            self.pipelineTestObj.variables.database
            == self.overrideVariables["database"]
        )


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

    def test_unknown_variable(self):
        with pytest.raises(AttributeError) as error:
            # No variables defined
            self.pipelineTestObj2 = etl.Pipeline(
                yamlData="""
                connections:
                  my_source: postgresql+psycopg2://${var.host}/${var.database}
                """
            )
        assert error.value.args[0] == f"'Variables' object has no attribute 'host'"


class TestPipelineRun:
    def test_run_pipeline(self):
        pipelineObj = etl.Pipeline(
            yamlData="""
            # Define user functions that should be included in tests
            preFlight:
              script: |
                def python_function_A(inputA: int = 0, inputB: int = 0, inputC: int = 0) -> int:
                    return inputA + inputB + inputC

                def python_function_B(inputA: int = 0, inputB: int = 0, inputC: int = 0) -> int:
                    return inputA + inputB - inputC

                def python_function_C(inputA: int = 1, inputB: int = 1, inputC: int = 1) -> int:
                    return inputA * inputB * inputC

            steps:
            - python_function_A:
                inputA: 1
                inputB: 2
                inputC: 3

            - python_function_B:
                inputA: 3
                inputB: 2
                inputC: 1

            - python_function_C:
                inputA: 2
                inputB: 3
                inputC: 1

            - name: finalOutputOne
              function: python_function_A
              args:
                inputA: ${steps['python_function_A'].output}
                inputB: ${steps['python_function_B'].output}
                inputC: ${steps['python_function_C'].output}

            - name: finalOutputTwo
              function: python_function_C
              args:
                inputA: ${steps['python_function_C'].output}
                inputB: ${steps['python_function_B'].output}
                inputC: ${steps['python_function_A'].output}
            """
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

            - ${steps['pd.read_csv'].output.groupby}:
                by: AB

            - ${steps['pd.read_csv.groupby'].output.max}:

            - ${steps['pd.read_csv.groupby.max'].output.to_csv}:
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
