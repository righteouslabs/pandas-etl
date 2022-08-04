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


class TestFindAndExecuteScript:
    def setup_class(self):
        self.yamlData = {
            "preFlight": {"script": "import numpy as np\n"},
            "steps": [
                {"name": "multiply by 10", "function": "np.prod", "args": [2, 10]},
            ],
        }

    def test_find_and_execute_script(self):
        etl.find_and_execute_script(yamlData=self.yamlData)
        etl.execute_steps(yamlData=self.yamlData)
        assert self.yamlData["steps"][0]["output"] == 20


class TestReplaceStepsOutput:
    def setup_class(self):
        self.yamlData = {
            "steps": [
                {
                    "pd.read_sql": {
                        "sql": "SELECT\n  int_column,\n  date_column\nFROM\n  test_data\n",
                        "con": "${conn.my-source}",
                        "index_col": "int_column",
                        "parse_dates": {"date_column": {"format": "%d/%m/%y"}},
                    },
                    "output": "pd.read_sql.output",
                },
                {
                    "pd.Grouper": {"key": "date_column", "freq": "W-MON"},
                    "output": "pd.Grouper.output",
                },
                {
                    "name": "group-data",
                    "description": "Group data by int and date columns every week",
                    "function": {
                        "object": "${steps['pd.read_sql'].output}",
                        "name": "groupby",
                    },
                    "args": {
                        "by": "${steps['pd.Grouper'].output}",
                        "axis": "columns",
                        "dropna": False,
                    },
                },
            ]
        }

    def test_replace_steps_output(self):
        result = etl.replace_steps_output(
            steps_output_matched=["pd.read_sql", "pd.Grouper"],
            step=self.yamlData["steps"][2],
            yamlData=self.yamlData,
        )
        expected = {
            "name": "group-data",
            "description": "Group data by int and date columns every week",
            "function": {"object": "pd.read_sql.output", "name": "groupby"},
            "args": {"by": "pd.Grouper.output", "axis": "columns", "dropna": False},
        }
        assert result == expected

    def test_no_step_output(self):
        with pytest.raises(ValueError) as error:
            etl.replace_steps_output(
                steps_output_matched=["pd.read_sql", "pd.Grouper", "pd.groupby"],
                step=self.yamlData["steps"][2],
                yamlData=self.yamlData,
            )
        assert error.value.args[0] == f"NO step output found for step name: pd.groupby"


class TestReturnFunctionObject:
    def setup_class(self):
        self.yamlData = {
            "steps": [
                {
                    "pd.read_sql": {
                        "sql": "SELECT\n  int_column,\n  date_column\nFROM\n  test_data\n",
                        "con": "${conn.my-source}",
                        "index_col": "int_column",
                        "parse_dates": {"date_column": {"format": "%d/%m/%y"}},
                    },
                    "output": "pd.read_sql.output",
                },
                {
                    "pd.Grouper": {"key": "date_column", "freq": "W-MON"},
                    "output": "pd.Grouper.output",
                },
                {
                    "steps['pd.read_sql'].output.groupby": {
                        "by": "${steps['pd.Grouper'].output}",
                        "axis": "columns",
                        "dropna": False,
                    }
                },
                {"steps['pd.read_sql.groupby'].output.max": None},
            ]
        }

    def test_return_function_object(self):
        result = etl.return_function_object(["pd.read_sql"], yamlData=self.yamlData)
        expected = "pd.read_sql.output"
        assert result == expected

    def test_no_step_output(self):
        with pytest.raises(ValueError) as error:
            etl.return_function_object(
                ["pd.read_sql.groupby"],
                yamlData=self.yamlData,
            )
        assert (
            error.value.args[0]
            == f"NO step output found for step name: pd.read_sql.groupby"
        )


class TestResolveConnectionsVariables:
    def setup_class(self):
        self.yamlData = {
            "connections": [
                {
                    "name": "my-source",
                    "connStr": "postgresql+psycopg2://MY_SERVER_NAME.MYDOMAIN.COM/MY_DATABASE",
                    "engine": "Engine",
                }
            ],
            "steps": [
                {
                    "pd.read_sql": {
                        "sql": "SELECT\n  int_column,\n  date_column\nFROM\n  test_data\n",
                        "con": "${conn.my-source}",
                        "index_col": "int_column",
                        "parse_dates": {"date_column": {"format": "%d/%m/%y"}},
                    },
                },
            ],
        }

    def test_resolve_connections_variables(self):
        result = etl.resolve_connections_variables(
            conn_matched=["my-source"],
            step=self.yamlData["steps"][0],
            yamlData=self.yamlData,
        )
        expected = {
            "pd.read_sql": {
                "sql": "SELECT\n  int_column,\n  date_column\nFROM\n  test_data\n",
                "con": "Engine",
                "index_col": "int_column",
                "parse_dates": {"date_column": {"format": "%d/%m/%y"}},
            },
        }
        assert result == expected

    def test_no_connection_engine(self):
        with pytest.raises(ValueError) as error:
            etl.resolve_connections_variables(
                conn_matched=["new-source"],
                step=self.yamlData["steps"][0],
                yamlData=self.yamlData,
            )
        assert error.value.args[0] == f"NO connection engine found for name: new-source"


class TestExecuteSteps:
    def setup_class(self):
        self.yamlData = {
            "variables": {
                "server": "MY_SERVER_NAME.MYDOMAIN.COM",
                "database": "MY_DATABASE",
            },
            "preFlight": {"script": "import pandas as pd\n"},
            "connections": [
                {
                    "name": "my-source",
                    "connStr": "postgresql+psycopg2://${var.server}/${var.database}",
                }
            ],
            "steps": [
                {
                    "pd.read_sql": {
                        "sql": "SELECT\n  int_column,\n  date_column\nFROM\n  test_data\n",
                        "con": "${conn.my-source}",
                        "index_col": "int_column",
                        "parse_dates": {"date_column": {"format": "%d/%m/%y"}},
                    }
                },
                {"pd.Grouper": {"key": "date_column", "freq": "W-MON"}},
                {
                    "steps['pd.read_sql'].output.groupby": {
                        "by": "${steps['pd.Grouper'].output}",
                        "axis": "columns",
                        "dropna": False,
                    }
                },
                {"steps['pd.read_sql.groupby'].output.max": None},
                {
                    "steps['pd.read_sql.groupby.max'].output.to_csv": [
                        "./my-aggregated-data.csv"
                    ]
                },
            ],
        }
