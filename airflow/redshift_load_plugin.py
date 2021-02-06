import os
from airflow.hooks.postgres_hook import PostgresHook
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
 
 
class S3ToRedshiftOperator(BaseOperator):
  """
  Executes a LOAD command on a s3 CSV file into a Redshift table
  :param redshift_conn_id: reference to a specific redshift database
  :type redshift_conn_id: string

  :param stg_table: reference to a specific stage table in redshift database
  :type stg_table: string

  :param s3_bucket: reference to a specific S3 bucket
  :type s3_bucket: string

  :param s3_path: reference to a specific S3 path
  :type s3_path: string

  :param iam_role: aws IAM_ROLE
  :type iam_role: string
  For IAM role chaining, provide the IAM roles separated by comma eg# a,b,c where a is the redshift role
  https://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html

  :param region: location of the s3 bucket (eg. 'eu-central-1' or 'us-east-1')
  :type region: string

  :param file_format: format of the file to be copied
  :type file_format: string

  """
  @apply_defaults
  def __init__(self, redshift_conn_id,stg_table,s3_bucket,s3_path,region,iam_role,file_format,*args, **kwargs):
   
    self.redshift_conn_id = redshift_conn_id
    self.stg_table = stg_table
    self.s3_bucket = s3_bucket
    self.s3_path = s3_path
    self.region = region
    self.iam_role=iam_role
    self.file_format=file_format
    
       
    super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
       
       
  def execute(self, context):
    self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
    conn = self.hook.get_conn() 
    cursor = conn.cursor()
    self.log.info("Connected with " + self.redshift_conn_id)
   
    load_statement = """
      truncate {0};
      copy
      {0}
      from 's3://{1}/{2}'
      IAM_ROLE '{3}'
      FORMAT AS {4} """.format(
    self.stg_table, self.s3_bucket, self.s3_path,
    self.iam_role,self.file_format)
   
    cursor.execute(load_statement)
    cursor.close()
    conn.commit()
    self.log.info("Load command completed")
    
    return True
   
 
class S3ToRedshiftOperatorPlugin(AirflowPlugin):
  name = "redshift_load_plugin"
  operators = [S3ToRedshiftOperator]
