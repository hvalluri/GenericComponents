######################################################################################
# Author: Hanu Valluri
# Create Date: 11/10/2020
# Purpose: EMR library to spin up the clusters and submit steps using boto3 and SC
######################################################################################
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
import boto3, json, time, logging, traceback
import botocore.exceptions
from botocore.config import Config
from datetime import datetime


def get_region():
    # by defualt things are in us-west-2 (oregon)
    return 'us-west-2'


def client(region_name, profile_name='', airflow_conn_id = ''):
    global emr
    global session
    global sc
    global sts_client
    config = Config(
        retries = dict(
            max_attempts = 10
        )
    )
    try:
        if airflow_conn_id:
            logging.info("::::::::::::::creating boto3 client using airflow connection id::::::::::::::", airflow_conn_id)
            conn = AwsHook(aws_conn_id=airflow_conn_id)
            emr = conn.get_client_type('emr', region_name=region_name, config=config)
            sc = conn.get_client_type('servicecatalog', region_name=region_name, config=config)
            sts_client = conn.get_client_type('sts', region_name=region_name, config=config)
            logging.info(":::::::::::::: account info::::::::::::::", sts_client.get_caller_identity())
        elif profile_name:
            logging.info("::::::::::::::creating boto3 client using profile_name ::::::::::::::", profile_name)
            session = boto3.Session(profile_name=profile_name)
            emr = session.client('emr', region_name=region_name, config=config)
            sc = session.client('servicecatalog', region_name=region_name, config=config)
        else:
            raise ValueError("Please provide either airflow_conn_id or profile_name from credentials file")
    except Exception as e:
        raise ValueError("Error while creating boto3 client..", str(e))


#######################################################


def get_path_id(service_catalog_product_id):
    try:
        path_list = sc.list_launch_paths(ProductId=service_catalog_product_id)
        path_id = path_list['LaunchPathSummaries'][0]['Id']
        return path_id
    except Exception as e:
        raise ValueError("Exception while getting PathID\n", e)


#######################################################


def get_artifact_id(artifact_name,name):
    try:
        artifacts_list = sc.describe_product(AcceptLanguage='en', Name=name).get('ProvisioningArtifacts')
        for artifact in artifacts_list:
            if artifact.get('Name')== artifact_name:
                return artifact.get('Id')
    except Exception as e:
        raise ValueError("Exception while getting Artifact ID from Name \n", e)


#######################################################


def read_config(emr_conf):
    config={}
    logging.info(f"reading log file/string-->{emr_conf}")
    if emr_conf:
        try:
            config_text = open(emr_conf, "r").read()
            config = json.loads(config_text)
            return config
        except Exception as err:
            try:
                config = json.loads(emr_conf)
                return config
            except Exception as e:
                raise ValueError("Error while parsing SC config: ", str(e))


#######################################################


def create_app_json_string(app_list_str):
    default_application_list = ["hadoop", "spark", "hive", "livy", "zeppelin", "sqoop"]
    if app_list_str:
        final_app_set = set(default_application_list + app_list_str.split(','))
    else:
        final_app_set = default_application_list
    json_string = """["""
    for app in final_app_set:
        json_string += f"""{{"Name":"{app}"}},"""
    json_string = json_string[:-1] + "]"
    print(f"created applist-->{json_string}")
    return json_string


#######################################################


def create_cluster(region_name='', emr_name='', sc_config_path=None, profile_name=None, airflow_conn_id='', **kwargs):
    current_time = datetime.now().strftime("%Y%m%d%H%M%S")
    global cluster_name
    logging.info("::::::::::::::calling client method::::::::::::::")
    client(region_name, profile_name=profile_name, airflow_conn_id=airflow_conn_id)
    config = read_config(sc_config_path)
    boto3_version_list = list(map(int,boto3.__version__.split('.')))
    service_catalog_product_version = config.get('ProvisioningArtifactName',config.get('ProductVersion','1.5.2'))
    service_catalog_artifact_id = config.get('ProvisioningArtifactId', get_artifact_id(service_catalog_product_version,'EMR'))
    service_catalog_product_id = config.get('ProductId')
    path_id = config.get('PathId',get_path_id(service_catalog_product_id))
    provisioned_product_name = config.get('ProvisionedProductName',"Airflow-SC-") + current_time
    print(f"::::::::::::::provisioned product name : {provisioned_product_name} ::::::::::::::")
    #EMR specific parameters
    cluster_name = emr_name if emr_name else config.get('ClusterName', "airflow" + current_time)
    logging.info("::::::::::::::creating cluster::::::::::::::", cluster_name)
    ec2_key_name = config.get('KeyName', 'bastionsshkey')
    master_instance_type = config.get('MasterInstanceType','m5.xlarge')
    core_instance_type = config.get('CoreInstanceType', 'r5.xlarge')
    num_cores = config.get('NumberOfCoreInstances','2')
    subnet_id = config.get('SubnetID')
    master_security_groups = config.get('MasterSecurityGroupIds') # Type: String, Ex: " sg1,sg2 "
    slave_security_groups = config.get('SlaveSecurityGroupIds')  # Type: String, Ex: " sg1,sg2 "
    release_label = config.get('ReleaseLabel','emr-5.30.0')
    root_volume_size = config.get('EbsRootVolumeSize','10')
    ami_image_id = config.get('AMIImageId','/GoldenAMI/gd-amzn2/latest')
    # Custom application list/default
    custom_json_application_list = create_app_json_string(config.get('CustomApplicationListJSON',""))

    glue_spark_hive_setting = """[
   {
      "Classification":"hive-site",
      "ConfigurationProperties":{
         "hive.metastore.schema.verification":"false",
         "hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
      }
   },
   {
      "Classification":"spark-hive-site",
      "ConfigurationProperties":{
         "hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
      }
   }] """
    custom_json_configuration_list = config.get('CustomConfigurationsJSON', glue_spark_hive_setting)
    tags_json = config.get('TagsJSON', """[]""")
    bootstrap_file_path = config.get('BootstrapActionFilePath','s3://test-bucket/init')
    #Included in 1.5.2
    custom_job_flow_role = config.get('CustomJobFlowRoleNameSuffix','')
    custom_service_role = config.get('CustomServiceRoleNameSuffix', '')
    ProvisioningParameters = [
                                 {
                                     'Key': 'ClusterName',
                                     'Value': cluster_name
                                 },
                                 {
                                     'Key': 'MasterInstanceType',
                                     'Value': master_instance_type
                                 },
                                 {
                                     'Key': 'CoreInstanceType',
                                     'Value': core_instance_type
                                 },
                                 {
                                     'Key': 'NumberOfCoreInstances',
                                     'Value': num_cores
                                 },
                                {
                                    'Key': 'EbsRootVolumeSize',
                                    'Value': root_volume_size
                                },
                                 {
                                     'Key': 'SubnetID',
                                     'Value': subnet_id
                                 },
                                 {
                                     'Key': 'MasterSecurityGroupIds',
                                     'Value': master_security_groups
                                 },
                                 {
                                     'Key': 'SlaveSecurityGroupIds',
                                     'Value': slave_security_groups
                                 },
                                 {
                                     'Key': 'ReleaseLabel',
                                     'Value': release_label
                                 },
                                 {
                                     'Key': 'AMIImageId',
                                     'Value': ami_image_id
                                 },
                                 {
                                     'Key': 'CustomApplicationListJSON',
                                     'Value': custom_json_application_list
                                 },
                                 {
                                     'Key': 'CustomConfigurationsJSON',
                                     'Value': custom_json_configuration_list
                                 },
                                 {
                                     'Key': 'BootstrapActionFilePath',
                                     'Value': bootstrap_file_path
                                 },
                                 {
                                     'Key': 'CustomJobFlowRoleNameSuffix',
                                     'Value': custom_job_flow_role
                                 },
                                 {
                                    'Key': 'CustomServiceRoleNameSuffix',
                                    'Value': custom_service_role
                                 },
                                 {
                                     'Key': 'TagsJSON',
                                     'Value': tags_json
                                 }
                             ]
    if service_catalog_product_version == "1.4.0":
        ProvisioningParameters.append(
            {
                'Key': 'KeyName',
                'Value': ec2_key_name
            })
        ProvisioningParameters.remove({
                                     'Key': 'CustomJobFlowRoleNameSuffix',
                                     'Value': custom_job_flow_role
                                 })
        ProvisioningParameters.remove({
                                    'Key': 'CustomServiceRoleNameSuffix',
                                    'Value': custom_service_role
                                 })

    Tags = [
        {
            'Key': 'doNotShutDown',
            'Value': 'true'
        },
    ]
    try:
        # boto3 version 1.14.31 and below doesn't accept ProvisioningArtifactName
        if (boto3_version_list[0]>=1 and boto3_version_list[1] <= 13) or (boto3_version_list[0]==1 and boto3_version_list[1]==14 and boto3_version_list[1]<=31):
            response = sc.provision_product(
                AcceptLanguage='en',
                ProductId=service_catalog_product_id,
                ProvisioningArtifactId=service_catalog_artifact_id,
                PathId=path_id,
                ProvisionedProductName=provisioned_product_name,
                ProvisioningParameters=ProvisioningParameters,
                Tags=Tags
            )
        else:
            response = sc.provision_product(
                AcceptLanguage='en',
                ProductId=service_catalog_product_id,
                ProvisioningArtifactName=service_catalog_product_version,
                PathId=path_id,
                ProvisionedProductName=provisioned_product_name,
                ProvisioningParameters=ProvisioningParameters,
                Tags=Tags
            )
        return response

    except Exception as e:
        raise ValueError(" Exception while creating SC product. Error details are as follows: \n", e)


###################################################################


def get_provisioned_product_id(record):
    return record['RecordDetail']['ProvisionedProductId']


###################################################################


def get_cluster_id(record):
    try:
        while True:
            tmp_record = sc.describe_record(
                Id=record['RecordDetail']['RecordId'],
                PageSize=1
            )

            if tmp_record['RecordDetail']['Status'] == 'SUCCEEDED':
                for pos in record['RecordOutputs']:
                    if pos.get("OutputKey") == 'ClusterId':
                        return pos.get("OutputValue")
            elif tmp_record['RecordDetail']['Status'] in ('IN_PROGRESS_IN_ERROR', 'FAILED'):
                logging.info("::::::::::::::::::::: Status: Failed ::::::::::::::", tmp_record['RecordDetail']['RecordErrors'])
                return None
            time.sleep(10)
    except Exception as e:
        return None


###################################################################


def get_cluster_id_from_emr_client(cluster_name):
    try:
        # Workaround to get around Service Catalog API timeout(I think)
        clusters = emr.list_clusters(
            ClusterStates=[
                'STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING', 'TERMINATING'
            ]
        )
        for cluster in clusters['Clusters']:
            if cluster['Name'] == cluster_name:
                return cluster['Id']
    except:
        return None


###################################################################


def wait_for_cluster_creation(region_name='', profile_name='',airflow_conn_id='',cluster_id=None):
    client(region_name, profile_name=profile_name, airflow_conn_id=airflow_conn_id)
    emr.get_waiter('cluster_running').wait(ClusterId=cluster_id)


###################################################################


def terminate_service_catalog(region_name='', profile_name='',airflow_conn_id='', provisioned_product_id=''):
    client(region_name, profile_name=profile_name, airflow_conn_id=airflow_conn_id)
    if provisioned_product_id:
        sc.terminate_provisioned_product(
    ProvisionedProductId=provisioned_product_id,
    IgnoreErrors=True,
    )
    else:
        raise ValueError(" Provisioned Product ID is not provided")


#############################################################################


# 'Args': ["sh", "/home/hadoop/test.sh", ""]
def create_steps(region_name='',cluster_id='', steps=[], profile_name='', airflow_conn_id=''):
    client(region_name, profile_name=profile_name, airflow_conn_id=airflow_conn_id)
    response = emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=steps
        )
    return response


#############################################################################


def track_step_progress(region_name='', cluster_id='', step_id='', profile_name='', airflow_conn_id=''):
    client(region_name, profile_name=profile_name, airflow_conn_id=airflow_conn_id)
    state = ''
    while state not in ('COMPLETED', 'FAILED', 'ERROR','CANCELLED'):
        try:
            client(region_name, profile_name=profile_name, airflow_conn_id=airflow_conn_id)
            response = emr.describe_step(ClusterId=cluster_id, StepId=step_id)
            state = response['Step']['Status']['State']
            logging.info('Status --> ', state)
            time.sleep(10)
        except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == 'LimitExceededException':
                logging.info('API call limit exceeded; backing off and retrying...')
                time.sleep(10)
            else:
                raise error
    if state in ('FAILED', 'ERROR'):
        raise ValueError('Final Step Status: ' + state)


class CreateEMROperator(BaseOperator):
    """
    Creates an EMR cluster using Service catalog

    :param region_name: region name where the SC catalog product is available
    :type region_name: str
    :param profile_name: Profile name to use from boto3 config/credentials/environment
    :type profile_name: str
    :param sc_config_path: JSON file with SC parameters
    :type sc_config_path: str
    :param airflow_conn_id: Airflow connection with boto config file
    :type airflow_conn_id: str
    Mandatory param: sc_config_path
                and one of the below
                    profile_name
                    airflow_variable
                    airflow_conn_id
                    role_chain

    """

    @apply_defaults
    def __init__(self, region_name='', sc_config_path='',emr_name = '', profile_name='', airflow_conn_id='', *args, **kwargs):
        super(CreateEMROperator, self).__init__(*args, **kwargs)
        self.region_name = get_region() if not region_name else region_name
        self.profile_name = 'default' if not profile_name else profile_name
        self.sc_config_path = sc_config_path
        self.emr_name = emr_name
        self.airflow_conn_id = airflow_conn_id
        self.cluster_id = None
        self.provisioned_product_id = None
        self.kwargs = kwargs
        logging.info(""" Input parameters: \nregion_name:{0}\nsc_config_path:{1}\nemr_name:{2}\nprofile_name:{3}\nairflow_conn_id:{4}\n""".format(
                                                                             self.region_name,
                                                                             self.sc_config_path,
                                                                             self.emr_name,
                                                                             self.profile_name,
                                                                             self.airflow_conn_id))

    def execute(self, context):
        self.log.info(" -----------Started CreateEMROperator Execution-----------")
        if not self.sc_config_path:
            raise ValueError (""" EMR config file path is missing. Please provide""")
        if self.profile_name!='default' and self.airflow_conn_id:
            raise ValueError("""Only one of the following should be passed:
                                profile_name -- To fetch creds from boto3 options ( credential_process=okta-process in boto3 accepted config files) - default profile will be used if not value is passed
                                airflow_conn_id -- To fetch from airflow connection (access and secret keys in a hardcoded config file) """)
        try:
            response = create_cluster(region_name=self.region_name,
                                      emr_name=self.emr_name,
                                      sc_config_path=self.sc_config_path,
                                      profile_name=self.profile_name,
                                      airflow_conn_id=self.airflow_conn_id)
            self.provisioned_product_id = get_provisioned_product_id(response)
            self.cluster_id = get_cluster_id(response)
            if self.cluster_id is None:
                # use cluster_name as it is global variable passed either from DAG or config file
                self.cluster_id = get_cluster_id_from_emr_client(cluster_name)
                if self.cluster_id is not None:
                    wait_for_cluster_creation(region_name=self.region_name,
                                      profile_name=self.profile_name,
                                      airflow_conn_id=self.airflow_conn_id,
                                      cluster_id=self.cluster_id)
                else:
                    raise AirflowException("Failed to create EMR cluster")
        except Exception as e:
            # self.log.info(traceback.print_exc())
            terminate_service_catalog(region_name=self.region_name,
                                      profile_name=self.profile_name,
                                      airflow_conn_id=self.airflow_conn_id,
                                      provisioned_product_id=self.provisioned_product_id)
            raise AirflowException("Failed to create EMR cluster: \n ", e)
        self.log.info(' Provisioned product id --> ', self.provisioned_product_id)
        self.log.info(' EMR Cluster created.. cluster_id --> ', self.cluster_id)
        return self.provisioned_product_id, self.cluster_id


class TerminateEMROperator(BaseOperator):
    """
        Creates an EMR cluster using Service catalog

        :param region_name: region name where the SC catalog product is available
        :type region_name: str
        :param profile_name: Profile name to use from boto3 config/credentials/environment
        :type profile_name: str
        :param airflow_conn_id: Airflow connection with boto config file
        :type airflow_conn_id: str
        :param xcom_task_id: if provisioned_product_id is not provided, pass the task id used to create cluster.
        :type  xcom_task_id: str
        Mandatory param: xcom_task_id or provisioned_product_id
                        and one of the below
                            profile_name
                            airflow_variable
                            airflow_conn_id
                            role_chain
        """
    @apply_defaults
    def __init__(self, region_name='', profile_name='', provisioned_product_id='',  xcom_task_id='', airflow_conn_id='', *args, **kwargs):
        super(TerminateEMROperator, self).__init__(*args, **kwargs)
        self.region_name = get_region() if not region_name else region_name
        self.profile_name = 'default' if not profile_name else profile_name
        self.airflow_conn_id = airflow_conn_id
        self.provisioned_product_id = provisioned_product_id
        self.xcom_task_id = 'create_cluster' if not xcom_task_id else xcom_task_id
        self.cluster_id = None
        logging.info(""" Input parameters: \n{0}\n{1}\n{2}\n{3}""".format(self.region_name,
                                                                             self.xcom_task_id,
                                                                             self.profile_name,
                                                                             self.provisioned_product_id))

    def execute(self, context):
        if self.profile_name != 'default' and self.airflow_conn_id:
            raise ValueError("""Only one of the following should be passed:
                                profile_name -- To fetch creds from boto3 options ( credential_process=okta-process in boto3 accepted config files) - default profile will be used if not value is passed
                                airflow_conn_id -- To fetch from airflow connection (access and secret keys in a hardcoded config file) """)
        try:
            if not self.provisioned_product_id:
                self.provisioned_product_id = context['ti'].xcom_pull(task_ids=self.xcom_task_id)[0]
                self.cluster_id = context['ti'].xcom_pull(task_ids=self.xcom_task_id)[1]
            terminate_service_catalog(region_name=self.region_name,
                                      profile_name=self.profile_name,
                                      airflow_conn_id=self.airflow_conn_id,
                                      provisioned_product_id=self.provisioned_product_id)
        except Exception as e:
            self.log.info(traceback.print_exc())
            raise AirflowException("Failed to terminate Service Catalog product - ", self.provisioned_product_id, '   EMR Cluster ID - ', self.cluster_id, "\n---------Exception: ------------", e)
        return True


class CreateStepsOperator(BaseOperator):
    """
            Creates an EMR cluster using Service catalog

            :param region_name: region name where the SC catalog product is available
            :type region_name: str
            :param profile_name: Profile name to use from boto3 config/credentials/environment
            :type profile_name: str
            :param cluster_id: EMR cluster_id
            :type cluster_id: str
            :param airflow_conn_id: Airflow connection with boto config file
            :type airflow_conn_id: str
            :param xcom_task_id: if provisioned_product_id is not provided, pass the task id used to create cluster.
            :type  xcom_task_id: str
            :param command: Bash command split by space (' ')
            :type  command: list
            :param steps: use this if custom steps list is available
            :type  steps: list
            :param name: Name of the step
            :type name:str
            :param action_on_failure: action to be done on step failure
            :type action_on_failure:str
            Mandatory param: xcom_task_id or cluster_id
                             one of the below
                                profile_name
                                airflow_variable
                                airflow_conn_id
                                role_chain
                             and  command
            """
    @apply_defaults
    def __init__(self,region_name='', cluster_id='', steps=[],  command=[], name='', action_on_failure='',profile_name='', airflow_conn_id='', xcom_task_id='', *args, **kwargs):
        super(CreateStepsOperator, self).__init__(*args, **kwargs)
        self.region_name = get_region() if not region_name else region_name
        self.profile_name = 'default' if not profile_name else profile_name
        self.name = 'Airflow' + str(time.time()) if not name else name
        self.cluster_id = cluster_id
        self.command = command
        self.action_on_failure = 'CONTINUE' if not action_on_failure else action_on_failure
        if steps:
            self.steps = steps
        else:
            self.steps = [{
                'Name': '{0}'.format(self.name),
                'ActionOnFailure': '{0}'.format(self.action_on_failure),
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': self.command
                }
            }]
        self.log.info('Using the following step function \n', self.steps)
        self.step_ids = []
        self.xcom_task_id = 'create_cluster' if not xcom_task_id else xcom_task_id
        self.airflow_conn_id = airflow_conn_id
        logging.info(""" Input parameters to Steps: \n{0}\n{1}\n{2}\n{3}\n{4}\n{5}\n{6}""".format(self.region_name,
                                                                             self.profile_name,
                                                                             self.action_on_failure,
                                                                             self.command,
                                                                             self.steps,
                                                                             self.xcom_task_id,
                                                                             self.cluster_id))

    def execute(self, context):
        try:
            if not self.cluster_id:
                self.cluster_id = context['ti'].xcom_pull(task_ids=self.xcom_task_id)[1]
            step_response = create_steps(region_name=self.region_name,
                                         cluster_id=self.cluster_id,
                                         steps=self.steps,
                                         profile_name=self.profile_name,
                                         airflow_conn_id=self.airflow_conn_id)
            self.step_ids = step_response.get('StepIds')
            self.log.info("---------------Added steps successfully. Step IDs: ", self.step_ids)
            if len(self.step_ids) == 1:
                track_step_progress(region_name=self.region_name,
                                    cluster_id=self.cluster_id,
                                    step_id=self.step_ids[0],
                                    profile_name=self.profile_name,
                                    airflow_conn_id=self.airflow_conn_id)
        except Exception as e:
            self.log.info(traceback.print_exc())
            raise AirflowException("Failed to create steps on cluster - ", self.cluster_id, "\n--Exception: ", e)
        self.log.info(' Steps created.. step ids --> ', self.step_ids)

        return self.step_ids if len(self.step_ids) > 1 else self.step_ids[0]


class TrackStepsOperator(BaseOperator):
    @apply_defaults
    def __init__(self, region_name='',cluster_id='', step_id='',profile_name='', airflow_conn_id='', *args, **kwargs):
        super(TrackStepsOperator, self).__init__(*args, **kwargs)
        self.region_name = get_region() if not region_name else region_name
        self.profile_name = 'default' if not profile_name else profile_name
        self.cluster_id = cluster_id
        self.step_id = step_id
        self.airflow_conn_id = airflow_conn_id

    def execute(self, context):
        try:
            track_step_progress(region_name=self.region_name,
                                cluster_id=self.cluster_id,
                                step_id=self.step_id,
                                profile_name=self.profile_name,
                                airflow_conn_id=self.airflow_conn_id)
        except:
            self.log.info(traceback.print_exc())
            raise AirflowException("Failed to find step: {0} on cluster: {1} ".format(self.step_id, self.cluster_id))

        return True


class EMROperatorPlugins(AirflowPlugin):
    name = 'sc_emr_operators'
    operators = [CreateEMROperator, TerminateEMROperator, CreateStepsOperator, TrackStepsOperator]
