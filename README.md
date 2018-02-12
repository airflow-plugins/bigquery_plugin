# Plugin - Custom Bigquery

This plugin moves data from the [Trello](https://developers.trello.com/v1.0) API to S3. Implemented for  camapigns, connected-sites connected-sites-details, conversations,  conversations-details, lists, lists-details, reports, reports-details.

## Hooks

### CustomBigQueryHook
This is an extension of the default BigQueryHook [here](https://github.com/apache/incubator-airflow/blob/master/airflow/contrib/hooks/bigquery_hook.py) that enables the json key for google cloud to be directly read in from the connection. 

Useful for cloud hosted type set ups.

As of Airflow 1.9 and above this behavior will be default as per [this issue](https://issues.apache.org/jira/browse/AIRFLOW-1635) .   

## Operators

### CustomBigQueryOperator

This operator is an extension of the default BigQueryOperator [here](https://github.com/apache/incubator-airflow/blob/master/airflow/contrib/operators/bigquery_operator.py) with the addition of:

- A query to the relevant __TABLES__ meta table to get destination table info before (if relevant) and after.
- Sending a custom event type record for 'before' and 'after' to a dag and destination dataset specific specific table in "airflow" dataset. 