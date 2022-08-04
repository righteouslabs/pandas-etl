# pandas-etl
A utility for running YAML config as Pandas based ETL code

## Installing ⏬

```bash
pip install pandas-etl
```

## Usage 📝

YAML Config (`my-run.yaml` file):

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

- name: source-df # Unique step name in pipeline
  description: Read from my PostgreSQL server
  function: pd.read_sql # Call static function
  args: # Key-word based parameter input
    sql: |
      SELECT
        int_column,
        date_column
      FROM
        test_data
    con: ${conn.my-source}
    index_col: int_column
    parse_dates: {"date_column": {"format": "%d/%m/%y"}}

- name: Grouper for date column
  function: pd.Grouper
  args:
    key: date_column
    freq: W-MON

- name: group-data
  description: Group data by int and date columns every week
  function: # Call object specific function
    object: ${steps['source-df'].output}
    name: groupby
  args:
    by: ${steps['Grouper for date column'].output}
    axis: columns
    dropna: false

- name: aggregate-data
  function: # Call object specific function
    object: ${steps['group-data'].output}
    name: max

- name: save-data
  function: # Call object specific function
    object: ${steps['aggregate-data'].output}
    name: to_csv
  args: # Non-key-word based parameter input
  - ./my-aggregated-data.csv
```

Running this YAML config

```bash
python -m pandas_etl --file "./my-run.yaml"
```
