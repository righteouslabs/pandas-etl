import os
import pytest
import yaml
from pandas_etl import etl
import sqlalchemy
from sqlalchemy.engine.base import Engine as Engine


class TestToYaml:
    def setup_class(self):
        self.string = """
        variables:
            server: MY_SERVER_NAME.MYDOMAIN.COM
            database: MY_DATABASE

        pre-flight:
            script: |
                import pandas as pd
        """

    def test_to_yaml(self):
        result = etl.to_yaml(self.string)
        expected = {
            "variables": {
                "server": "MY_SERVER_NAME.MYDOMAIN.COM",
                "database": "MY_DATABASE",
            },
            "pre-flight": {"script": "import pandas as pd\n"},
        }
        assert result == expected

    def test_non_str_input(self):
        with pytest.raises(ValueError) as error:
            etl.to_yaml(["some command"])
        assert error.value.args[0] == "Wrong input"


class TestAddArgumentVariables:
    def setup_class(self):
        self.yamlData = {
            "variables": {
                "server": "MY_SERVER_NAME.MYDOMAIN.COM",
                "database": "MY_DATABASE",
            },
            "pre-flight": {"script": "import pandas as pd\n"},
        }
        self.var = ["varname1=varvalue1", "varname2=varvalue2"]

    def test_add_argument_variables(self):
        result = etl.add_argument_variables(var=self.var, yamlData=self.yamlData)
        expected = {
            "variables": {
                "server": "MY_SERVER_NAME.MYDOMAIN.COM",
                "database": "MY_DATABASE",
                "varname1": "varvalue1",
                "varname2": "varvalue2",
            },
            "pre-flight": {"script": "import pandas as pd\n"},
        }
        assert result == expected


class TestAddArgumentImports:
    def setup_class(self):
        self.yamlData = {
            "imports": [
                "./etl_definition_folder/connections/sql_connections.yaml",
                "./etl_definition_folder/connections/database_variables.yaml",
            ],
            "variables": {
                "server": "MY_SERVER_NAME.MYDOMAIN.COM",
                "database": "MY_DATABASE",
                "varname1": "varvalue1",
                "varname2": "varvalue2",
            },
            "pre-flight": {"script": "import pandas as pd\n"},
        }
        self.imports = ["./vars/sql-db1.yaml"]

    def test_add_argument_imports(self):
        result = etl.add_argument_imports(imports=self.imports, yamlData=self.yamlData)
        expected = {
            "imports": [
                "./vars/sql-db1.yaml",
                "./etl_definition_folder/connections/sql_connections.yaml",
                "./etl_definition_folder/connections/database_variables.yaml",
            ],
            "variables": {
                "server": "MY_SERVER_NAME.MYDOMAIN.COM",
                "database": "MY_DATABASE",
                "varname1": "varvalue1",
                "varname2": "varvalue2",
            },
            "pre-flight": {"script": "import pandas as pd\n"},
        }
        assert result == expected
        assert result["imports"][0] == "./vars/sql-db1.yaml"


class TestFindAndReplaceVariables:
    def setup_class(self):
        self.yamlData = {
            "variables": {
                "server": "MY_SERVER_NAME.MYDOMAIN.COM",
                "database": "MY_DATABASE",
            },
            "pre-flight": {"script": "import pandas as pd\n"},
            "connections": [
                {
                    "name": "my-source",
                    "connStr": "postgresql+psycopg2://${var.server}/${var.database}",
                }
            ],
        }
        self.yamlData2 = {
            "variables": {
                "server": "MY_SERVER_NAME.MYDOMAIN.COM",
                "database": "MY_DATABASE",
            },
            "pre-flight": {"script": "import pandas as pd\n"},
            "connections": [
                {
                    "name": "my-source",
                    "connStr": "${var.host}://${var.server}/${var.database}",
                }
            ],
        }

    def test_find_and_replace_variables(self):
        result = etl.find_and_replace_variables(yamlData=self.yamlData)
        expected = {
            "variables": {
                "server": "MY_SERVER_NAME.MYDOMAIN.COM",
                "database": "MY_DATABASE",
            },
            "pre-flight": {"script": "import pandas as pd\n"},
            "connections": [
                {
                    "name": "my-source",
                    "connStr": "postgresql+psycopg2://MY_SERVER_NAME.MYDOMAIN.COM/MY_DATABASE",
                }
            ],
        }
        assert result == expected

    def test_unknown_variable(self):
        with pytest.raises(ValueError) as error:
            etl.find_and_replace_variables(yamlData=self.yamlData2)
        assert error.value.args[0] == f"Unknown variable 'host' found"


class TestResolveImports:
    def setup_class(self):
        with open("./tests/database_variables.yaml", "w") as f:
            yaml.dump(
                {
                    "imports": [
                        "./tests/sql_connections.yaml",
                    ],
                    "variables": {
                        "varname1": "varvalue1",
                        "varname2": "varvalue2",
                    },
                },
                f,
            )
        with open("./tests/sql_connections.yaml", "w") as f:
            yaml.dump(
                {
                    "connections": [
                        {
                            "name": "my-source",
                            "connStr": "postgresql+psycopg2://${var.server}/${var.database}",
                        }
                    ],
                },
                f,
            )
        self.yamlData = {
            "imports": [
                "./tests/database_variables.yaml",
            ],
            "variables": {
                "server": "MY_SERVER_NAME.MYDOMAIN.COM",
                "database": "MY_DATABASE",
            },
            "pre-flight": {"script": "import pandas as pd\n"},
        }
        self.yamlData2 = {
            "imports": [
                "./tests/sql_connection.yaml",
            ],
        }
        self.f = open("./tests/database_variables.txt", "x")
        self.yamlData3 = {
            "imports": [
                "./tests/database_variables.txt",
            ],
        }

    def teardown_class(self):
        if os.path.exists("./tests/database_variables.txt"):
            os.remove("./tests/database_variables.txt")
        if os.path.exists("./tests/database_variables.yaml"):
            os.remove("./tests/database_variables.yaml")
        if os.path.exists("./tests/sql_connections.yaml"):
            os.remove("./tests/sql_connections.yaml")

    def test_resolve_imports(self):
        result = etl.resolve_imports(yamlData=self.yamlData)
        expected = {
            "connections": [
                {
                    "name": "my-source",
                    "connStr": "postgresql+psycopg2://${var.server}/${var.database}",
                }
            ],
            "variables": {
                "server": "MY_SERVER_NAME.MYDOMAIN.COM",
                "database": "MY_DATABASE",
                "varname1": "varvalue1",
                "varname2": "varvalue2",
            },
            "pre-flight": {"script": "import pandas as pd\n"},
        }
        print(result)
        assert result == expected

    def test_wrong_file_extension(self):
        with pytest.raises(ValueError) as error:
            etl.resolve_imports(
                yamlData=self.yamlData3,
            )
        assert (
            error.value.args[0]
            == f"Wrong file extension for the import: ./tests/database_variables.txt"
        )

    def test_no_file_found(self):
        with pytest.raises(FileNotFoundError) as error:
            etl.resolve_imports(
                yamlData=self.yamlData2,
            )
        assert error.value.args[0] == f"No such file: ./tests/sql_connection.yaml"


class TestCreateEngineConnection:
    def setup_class(self):
        self.yamlData = {
            "variables": {
                "server": "MY_SERVER_NAME.MYDOMAIN.COM",
                "database": "MY_DATABASE",
            },
            "pre-flight": {"script": "import pandas as pd\n"},
            "connections": [
                {
                    "name": "my-source",
                    "connStr": "postgresql+psycopg2://MY_SERVER_NAME.MYDOMAIN.COM/MY_DATABASE",
                }
            ],
        }

    def test_create_engine_connection(self):
        etl.create_engine_connection(yamlData=self.yamlData)
        assert (
            type(self.yamlData["connections"][0]["engine"])
            == sqlalchemy.engine.base.Engine
        )
        assert (
            str(self.yamlData["connections"][0]["engine"])
            == "Engine(postgresql+psycopg2://MY_SERVER_NAME.MYDOMAIN.COM/MY_DATABASE)"
        )


class TestFindAndExecuteScript:
    def setup_class(self):
        self.yamlData = {
            "variables": {
                "server": "MY_SERVER_NAME.MYDOMAIN.COM",
                "database": "MY_DATABASE",
            },
            "pre-flight": {"script": "import numpy as np\n"},
            "connections": [
                {
                    "name": "my-source",
                    "connStr": "postgresql+psycopg2://MY_SERVER_NAME.MYDOMAIN.COM/MY_DATABASE",
                }
            ],
        }

    def test_find_and_execute_script(self):
        result = etl.find_and_execute_script(yamlData=self.yamlData)
        # assert "np" in globals()


class TestFindKeyByValueAndAssign:
    def setup_class(self):
        self.yamlData = {
            "variables": {
                "server": "MY_SERVER_NAME.MYDOMAIN.COM",
                "database": "MY_DATABASE",
            },
            "pre-flight": {"script": "import pandas as pd\n"},
            "connections": [
                {
                    "name": "my-source",
                    "connStr": "postgresql+psycopg2://MY_SERVER_NAME.MYDOMAIN.COM/MY_DATABASE",
                }
            ],
        }

    def test_find_key_by_value_and_assign(self):
        result = etl.find_key_by_value_and_assign(
            data=self.yamlData, target="my-source", assign="new-source"
        )
        expected = {
            "variables": {
                "server": "MY_SERVER_NAME.MYDOMAIN.COM",
                "database": "MY_DATABASE",
            },
            "pre-flight": {"script": "import pandas as pd\n"},
            "connections": [
                {
                    "name": "new-source",
                    "connStr": "postgresql+psycopg2://MY_SERVER_NAME.MYDOMAIN.COM/MY_DATABASE",
                }
            ],
        }
        assert result == expected


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
            "pre-flight": {"script": "import pandas as pd\n"},
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
