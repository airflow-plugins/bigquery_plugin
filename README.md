# Plugin - Custom Bigquery

## Hooks

### CustomBigQueryHook
This is an extension of the default BigQueryHook [here](https://github.com/apache/incubator-airflow/blob/master/airflow/contrib/hooks/bigquery_hook.py) that enables the json key for google cloud to be directly read in from the connection. 

Useful for cloud hosted type set ups.

As of Airflow 1.9 and above this behavior will be default as per [this issue](https://issues.apache.org/jira/browse/AIRFLOW-1635) .   

## Operators

### CustomBigQueryOperator

This operator is an extension of the default BigQueryOperator [here](https://github.com/apache/incubator-airflow/blob/master/airflow/contrib/operators/bigquery_operator.py) with the addition of:

- A query to the relevant \_\_TABLES\_\_ meta table to log destination table info before (if relevant) and after the main task of the operator (uses [pandas.read_gbq()](http://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_gbq.html) and [pandas_gbq.GbqConnector.schema()](https://github.com/pydata/pandas-gbq/blob/master/pandas_gbq/gbq.py)).
- Sending a custom event type record for 'before' and 'after' to a dag and destination dataset specific table in an "airflow" dataset in BQ. 
