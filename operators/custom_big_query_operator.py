# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.utils.decorators import apply_defaults
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from plugins.CustomBigQueryPlugin.hooks.custom_big_query_hook import CustomBigQueryHook
import pandas as pd
import pandas_gbq as pdgbq
import re
import pprint
import datetime
import time

class CustomBigQueryOperator(BigQueryOperator):

    @apply_defaults
    def __init__(self,
                 bql,
                 destination_dataset_table=False,
                 write_disposition='WRITE_EMPTY',
                 allow_large_results=False,
                 bigquery_conn_id='bigquery_default',
                 delegate_to=None,
                 udf_config=False,
                 use_legacy_sql=True,
                 # maximum_billing_tier=None,
                 create_disposition='CREATE_IF_NEEDED',
                 schema_update_options=(),
                 query_params=None,
                 *args,
                 **kwargs):
        super(BigQueryOperator, self).__init__(*args, **kwargs)
        self.bql = bql
        self.destination_dataset_table = destination_dataset_table
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.allow_large_results = allow_large_results
        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to
        self.udf_config = udf_config
        self.use_legacy_sql = use_legacy_sql
        # self.maximum_billing_tier = maximum_billing_tier
        self.schema_update_options = schema_update_options
        self.query_params = query_params
        self.bq_cursor = None

    
    def execute(self, context):
    
        #########################################################
        # VARIABLES
        #########################################################    
    
        # define some objects to be used by various functions
        pp = pprint.PrettyPrinter(indent=4)
        bq_connection = AstroBigQueryHook.get_connection("bigquery_default")
        destination_project = re.search('(.*?):', self.destination_dataset_table).group(0).replace(':','').replace('.','')
        destination_dataset = re.search(':(.*?)\.', self.destination_dataset_table).group(0).replace(':','').replace('.','')
        destination_table = self.destination_dataset_table[1+self.destination_dataset_table.find('.'):]
        #pp.pprint(context)
        airflow_event_dag_run = context['dag_run']
        airflow_event_task = context['task']
        airflow_event_task_instance = context['task_instance']
        airflow_as_at_yyyymmdd = datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d')
        airflow_event_destination_table = "airflow.events_{}__{}_{}".format(airflow_event_dag_run.dag_id, destination_dataset, airflow_as_at_yyyymmdd)
        
        #########################################################
        # FUNCTIONS
        #########################################################
        
        def print_my_msg(my_msg):
            """
            Helper function to print messages to logs in a way that jumps out easy
            """
            my_msg_w = 50
            print("\n" + "-" * my_msg_w + "\n" + my_msg + "\n" + "-" * my_msg_w + "\n")
        
        
        def get_table_info(mode):
            """
            Make calls to get table info and schema using pandas and pandas-gbq libs.
            Print the results to logs. 
            """
            N_TRY_TABLE_INFO = 3
            N_TRY_TABLE_SCHEMA = 3
            
            # define query to get meta info
            table_info_qry = """
            SELECT 
              *, 
              msec_to_timestamp(creation_time) as creation_timestamp, 
              msec_to_timestamp(last_modified_time) as last_modified_timestamp, 
              size_bytes/(1024*1048576) as size_gb, 
              concat(string(current_date()),' ',string(current_time())) as info_as_at, 
              (now()-TIMESTAMP_TO_USEC(MSEC_TO_TIMESTAMP(last_modified_time)))/1000000/60/60 as hours_since_modified 
            FROM 
              {destination_dataset}.__TABLES__ 
            WHERE 
              dataset_id='{destination_dataset}' 
              AND 
              table_id='{destination_table}'
            """.format(
              destination_dataset = destination_dataset, 
              destination_table = destination_table 
              )
            
            # try get table info a few times 
            # (work around for quota limits in BQ) 
            # (rather retry in here as opposed to kill whole task instance)
            for i in range(N_TRY_TABLE_INFO):
                try:
                    print_my_msg("...get table info {} , try {}...".format(mode,str(i+1)))
                    # get meta info        
                    table_info_df = pd.read_gbq(table_info_qry, destination_project, private_key = bq_connection.extra_dejson.get('extra__google_cloud_platform__key_path'))                    
                except Exception as e:
                    print(e)
                    print_my_msg("...RETRY: table info {}...".format(mode))
                    if i < N_TRY_TABLE_INFO - 1:
                        continue
                    else:
                        raise    
                break
            
            # try get schema a few times if needed 
            # (rather retry in here as opposed to kill whole task instance)
            for i in range(N_TRY_TABLE_SCHEMA):
                try:
                    print_my_msg("...get table schema {} , try {}...".format(mode,str(i+1)))
                    # get schema
                    if mode == 'before':                        
                        try:
                            schema = pdgbq.gbq.GbqConnector.schema(pdgbq.gbq.GbqConnector(destination_project, 
                                                                                          private_key = bq_connection.extra_dejson.get('extra__google_cloud_platform__key_path') ), 
                                                                                          dataset_id = destination_dataset, table_id = destination_table
                                                                                          )
                        except Exception as e:
                            print(e)
                            schema = None
                    else:
                        schema = pdgbq.gbq.GbqConnector.schema(pdgbq.gbq.GbqConnector(destination_project, 
                                                                                      private_key = bq_connection.extra_dejson.get('extra__google_cloud_platform__key_path') ), 
                                                                                      dataset_id = destination_dataset, table_id = destination_table
                                                                                      )
                except Exception as e:                    
                    print(e)
                    print_my_msg("...RETRY: table schema {}...".format(mode))
                    if i < N_TRY_TABLE_SCHEMA - 1:
                        continue
                    else:
                        raise                    
                break
            
            # print table info message
            print_my_msg("...table info {}".format(mode))
            print(table_info_df.head())
            print_my_msg("...")
            
            # print table schema message
            print_my_msg("...schema {}".format(mode))
            pp.pprint(schema)
            print_my_msg("...")

        
        def send_airflow_event_to_bq(mode):
            """
            Send an event record to relevant bq table (based on dag, destination table and date) with info on current state
            """
            
            if self.use_legacy_sql:
                seconds_since_dag_execution = "(parse_utc_usec(task_start_date)-parse_utc_usec(dag_execution_date))/1000000"
            else:
                seconds_since_dag_execution = "(UNIX_MICROS(safe_cast(task_start_date as timestamp))-UNIX_MICROS(safe_cast(dag_execution_date as timestamp)))/1000000"
            
            airflow_event_sql_template = """
            select 
              *,
              {seconds_since_dag_execution} as seconds_since_dag_execution,
              if(task_hostname='airflow_scheduler','local','astro_prod') as environment              
            from
              (
              select
                "{airflow_event_info}" as info,
                "{airflow_event_dag_run_dag_id}" as dag_id,
                "{airflow_event_dag_run_execution_date}" as dag_execution_date,
                "{airflow_event_dag_run_run_id}" as dag_run_id,
                "{airflow_event_dag_run_state}" as dag_state,
                "{context_task_instance_key_str}" as context_task_instance_key_str,
                "{context_ts}" as context_ts,
                "{context_ds}" as context_ds,
                "{airflow_event_task_instance_execution_date}" as task_execution_date,
                "{airflow_event_task_instance_start_date}" as task_start_date,
                "{airflow_event_task_instance_job_id}" as task_job_id,
                "{airflow_event_task_instance_task_id}" as task_task_id,
                "{airflow_event_task_instance_pid}" as task_pid,
                "{airflow_event_task_instance_try_number}" as task_try_number,
                "{airflow_event_task_instance_state}" as task_state,
                "{airflow_event_task_instance_hostname}" as task_hostname,
                "{airflow_event_task_instance_log_filepath}" as task_log_filepath,
                "{airflow_event_task_instance_log_url}" as task_log_url,
                "{airflow_event_task_instance_run_as_user}" as task_run_as_user,
                "{airflow_event_task_instance_unixname}" as task_unixname,
                "{airflow_as_at}" as airflow_as_at,
                current_timestamp() as bq_as_at,
                "{task_length}" as task_execution_length_seconds,
                "{destination_project}" as destination_project,
                "{destination_dataset}" as destination_dataset,
                "{destination_table}" as destination_table
              ) base_event_data
            """
            
            if mode == 'after':
                
                airflow_as_at = airflow_as_at_after
                task_length = ( datetime.datetime.strptime(airflow_as_at_after,'%Y-%m-%d %H:%M:%S') - datetime.datetime.strptime(airflow_as_at_before,'%Y-%m-%d %H:%M:%S') ).total_seconds()
            
            else:
            
                airflow_as_at = airflow_as_at_before
                task_length = ""
            
            airflow_as_at = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            
            airflow_event_sql = airflow_event_sql_template.format(
                                  airflow_event_info = mode,
                                  airflow_event_dag_run_dag_id = airflow_event_dag_run.dag_id,
                                  airflow_event_dag_run_execution_date = airflow_event_dag_run.execution_date,
                                  airflow_event_dag_run_run_id = airflow_event_dag_run.run_id,
                                  airflow_event_dag_run_state = airflow_event_dag_run.state,
                                  context_task_instance_key_str = context['task_instance_key_str'],
                                  context_ts = context['ts'],
                                  context_ds = context['ds'],
                                  airflow_event_task_instance_execution_date = airflow_event_task_instance.execution_date,
                                  airflow_event_task_instance_start_date = airflow_event_task_instance.start_date,
                                  airflow_event_task_instance_job_id = airflow_event_task_instance.job_id,
                                  airflow_event_task_instance_task_id = airflow_event_task_instance.task_id,
                                  airflow_event_task_instance_pid = airflow_event_task_instance.pid,
                                  airflow_event_task_instance_try_number = airflow_event_task_instance.try_number,
                                  airflow_event_task_instance_state = airflow_event_task_instance.state,
                                  airflow_as_at = airflow_as_at,
                                  task_length = task_length,
                                  airflow_event_task_instance_hostname = airflow_event_task_instance.hostname,
                                  destination_project = destination_project,
                                  destination_dataset = destination_dataset,
                                  destination_table = destination_table,
                                  airflow_event_task_instance_log_filepath = airflow_event_task_instance.log_filepath,
                                  airflow_event_task_instance_log_url = airflow_event_task_instance.log_url,
                                  airflow_event_task_instance_run_as_user = airflow_event_task_instance.run_as_user,
                                  airflow_event_task_instance_unixname = airflow_event_task_instance.unixname,
                                  seconds_since_dag_execution = seconds_since_dag_execution
                                )
        
            print_my_msg("...send '{}' event to bq...".format(mode))
        
            # send event to bq
            self.bq_cursor.run_query(
                airflow_event_sql,
                destination_dataset_table = airflow_event_destination_table,
                write_disposition = "WRITE_APPEND",
                allow_large_results = self.allow_large_results,
                udf_config = self.udf_config,
                use_legacy_sql = self.use_legacy_sql
                )            
    

        #########################################################
        # CODE
        #########################################################    
        
        if self.bq_cursor is None:
            # self.log.info('Executing: %s', self.bql)
            hook = AstroBigQueryHook(
                conn_id=self.bigquery_conn_id,
                delegate_to=self.delegate_to)
            conn = hook.get_conn()
            self.bq_cursor = conn.cursor()
            
        get_table_info('before')    
        
        airflow_as_at_before = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        
        send_airflow_event_to_bq('before')
        
        print_my_msg("...run main query...")
        
        # run main task query
        self.bq_cursor.run_query(
            self.bql,
            destination_dataset_table=self.destination_dataset_table,
            write_disposition=self.write_disposition,
            allow_large_results=self.allow_large_results,
            udf_config=self.udf_config,
            use_legacy_sql=self.use_legacy_sql,
            )
            
        airflow_as_at_after = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        
        send_airflow_event_to_bq('after')
        
        get_table_info('after')

    
    def on_kill(self):
        super(BigQueryOperator, self).on_kill()
        if self.bq_cursor is not None:
            self.log.info('Canceling running query due to execution timeout')
            self.bq_cursor.cancel_query()
