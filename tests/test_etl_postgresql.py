import os
import pytest
import time

import pandas as pd
import sqlalchemy.exc
from sqlalchemy import create_engine
from sqlalchemy.engine import URL as create_engine_conn_url

from pandas_etl import etl


@pytest.fixture(scope="session")
def docker_compose_file():
    return "tests/docker-compose.yaml"


@pytest.fixture(scope="session")
def docker_compose_project_name():
    return "pandas-etl-tests-msuthar09-testing"


@pytest.fixture(scope="session")
def get_postgresql_engine(docker_services):
    for retry in range(0, 10):
        try:
            engine = create_engine(
                url=create_engine_conn_url.create(
                    drivername="postgresql+psycopg2",
                    host="localhost",
                    database="pandas_etl_test_db",
                    username="postgres",
                    password="password",
                    port=5432,
                ),
                isolation_level="REPEATABLE READ",
            )
            with engine.connect() as conn:
                conn = conn.execution_options(
                    isolation_level="SERIALIZABLE",
                    postgresql_readonly=True,
                    postgresql_deferrable=True,
                )
                with conn.begin():
                    yield conn
                    return
        except sqlalchemy.exc.OperationalError:
            print("Waiting 1 second for postgresql to startup")
            time.sleep(1)


def test_postgre_sql(get_postgresql_engine):

    pipelineTestObj = etl.Pipeline(
        yamlData="""
            imports:
            - ./tests/etl_definition_folder/variables/postgresql_database_variables.yaml
            - ./tests/etl_definition_folder/pipelines/pandas_pipeline_recovery_1.yaml
            connections:
              postgre_sql: postgresql+psycopg2://${var.username}:${var.password}@${var.server}:${var.postgresql_port}/${var.database}

            steps:
            - ${ steps['pd.concat.max'].output.to_sql }:
                name:         "pytest_output_table"
                if_exists:    "replace"
                index:        False
                con:          ${ conn.postgre_sql }
            """,
        includeImports=[
            "./tests/etl_definition_folder/variables/secrets/postgresql_database-secret_variables.yaml"
        ],
        overrideVariables={"postgresql_port": 5432},
    )
    pipelineTestObj.run()

    df = pd.read_sql("SELECT * FROM pytest_output_table", get_postgresql_engine)
    assert isinstance(df, pd.DataFrame)

    # Define the path of the expected output file
    expected_output_file_path = "./tests/data/max.csv"
    expected_output_file_path1 = "./tests/data/groupby_Instance1.csv"
    expected_output_file_path2 = "./tests/data/groupby_Instance2.csv"

    assert os.path.exists(path=expected_output_file_path)

    # Remove files after running pipeline
    if os.path.exists(path=expected_output_file_path):
        os.remove(path=expected_output_file_path)
    if os.path.exists(path=expected_output_file_path1):
        os.remove(path=expected_output_file_path1)
    if os.path.exists(path=expected_output_file_path2):
        os.remove(path=expected_output_file_path2)
