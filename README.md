# pandas-etl

[![Package Tests](https://github.com/righteouslabs/pandas-etl/actions/workflows/test-deploy.yml/badge.svg?branch=main)](https://github.com/righteouslabs/pandas-etl/actions/workflows/test-deploy.yml)
[![codecov](https://codecov.io/gh/righteouslabs/pandas-etl/branch/main/graph/badge.svg?token=Y33PFLH2HS)](https://codecov.io/gh/righteouslabs/pandas-etl)
![PyPI](https://img.shields.io/pypi/v/pandas-etl)
![Azure DevOps tests](https://img.shields.io/azure-devops/tests/righteous-ai/Python-Repos/6?compact_message)
![PyPI - Downloads](https://img.shields.io/pypi/dd/pandas-etl)
![PyPI - Format](https://img.shields.io/pypi/format/pandas-etl)
![GitHub](https://img.shields.io/github/license/righteouslabs/pandas-etl)
![GitHub language count](https://img.shields.io/github/languages/count/righteouslabs/pandas-etl)
![GitHub top language](https://img.shields.io/github/languages/top/righteouslabs/pandas-etl)
![Snyk Vulnerabilities for GitHub Repo](https://img.shields.io/snyk/vulnerabilities/github/righteouslabs/pandas-etl)
![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/righteouslabs/pandas-etl)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pandas-etl)

A utility for running YAML config as Pandas based ETL code

## Installing ⏬

```bash
pip install pandas-etl
```

## Usage 📝

#### YAML Config:

`my-run.yaml`
```YAML
variables:
  server: MY_SERVER_NAME.MYDOMAIN.COM
  database: MY_DATABASE

preFlight:
  script: |
    import pandas as pd

connections:
  my_source: postgresql+psycopg2://${var.server}/${var.database}

steps:

- name:           source-df # Unique step name in pipeline
  description:    Read from my PostgreSQL server
  function:       pd.read_sql # Call static function
  args:           # Key-word based parameter input
    sql:          |
                  SELECT int_column, date_column
                  FROM test_data
    con:          ${ conn.my_source }
    index_col:    int_column
    parse_dates:  { "date_column": { "format": "%d/%m/%y" } }

- name:           Grouper for date column
  function:       pd.Grouper
  args:
    key:          date_column
    freq:         W-MON

- name:           group-data
  description:    Group data by int and date columns every week
  function:       ${ steps['source-df'].output.groupby }
  args:
    by:           ${steps['Grouper for date column'].output}
    axis:         columns
    dropna:       false

- name:           aggregate-data
  function:       ${ steps['group-data'].output.max }

- name:           save-data
  function:       ${ steps['aggregate-data'].output.to_csv }
  args:
                  - ./my-aggregated-data.csv
```

Running this YAML config

```bash
python -m pandas_etl --file "./my-run.yaml"
```

## YAML Config (short-hand format)
Functions can be written in short-hand to optimize readability and minimize overall size of config file.

Below is working example with Imports `--imports` and override Variables `--var`:

### `my-run.yaml`
```YAML
preFlight:
  script: |
    import pandas as

imports:
- ./etl_definition_folder/variables/postgresql_database_variables.yaml

connections:
  my_database: postgresql+psycopg2://${var.username}:${var.password}@${var.server}:${var.postgresql_port}/${var.database}

steps:

- pd.read_sql:
    sql:          |
                  SELECT int_column, date_column
                  FROM test_data
    con:          ${ conn.my_database }
    index_col:    int_column
    parse_dates:  { "date_column": { "format": "%d/%m/%y" } }

- pd.Grouper:
    key:          date_column
    freq:         W-MON

- ${ steps['pd.read_csv'].output.groupby }:
    by:           ${steps['pd.Grouper'].output}
    axis:         columns
    dropna:       false

- ${ steps['pd.read_csv.groupby'].output.max }:

- ${ steps['pd.read_csv.groupby.max'].output.to_csv }:
    path_or_buf:  ./my-aggregated-data.csv
```

## Variables:

### `postgresql_database_variables.yaml`
```YAML
variables:
  server:   MY_SERVER_NAME.MYDOMAIN.COM
  database: MY_DATABASE
```

### `postgresql_database-secret_variables.yaml`
```YAML
variables:
  username: postgres
  password: password
```

## Running this YAML config from command line:

```bash
python -m pandas_etl --file "./my-run.yaml" --imports "./etl_definition_folder/variables/secrets/postgresql_database-secret_variables.yaml" --var "postgresql_port=9999"
```
