from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser
from pathlib import Path
from typing import Any
from cloud_manager.ec2_manager import EC2Manager
from util.aws_config_util import parse_aws_config_file
from util.logging_util import load_logger, log_message
from util.os_util import check_if_file_exists, find_full_file_name_by_prefix
from util.process_util import execute_command, remotely_execute_command
from util.sparking_cloud_util import parse_sparking_cloud_config_file


class SparkJobExecutor:

    def __init__(self,
                 sparking_cloud_config_file: Path) -> None:
        self.sparking_cloud_config_file = sparking_cloud_config_file
        # Sparking Cloud's Config File Settings.
        self.general_settings = None
        self.logging_settings = None
        self.aws_settings = None
        self.spark_environment_settings = None
        # Other Attributes.
        self.logger = None
        self.execution_mode = None

    def set_attribute(self,
                      attribute_name: str,
                      attribute_value: Any) -> None:
        setattr(self, attribute_name, attribute_value)

    def get_attribute(self,
                      attribute_name: str) -> Any:
        return getattr(self, attribute_name)

    def read_instances_file(self,
                            cluster_name: str) -> list:
        instances_file_root_folder = self.get_attribute("general_settings")["cluster_instances_root_folder"]
        instances_file = Path(instances_file_root_folder).joinpath(cluster_name)
        instances_list_parser = ConfigParser()
        instances_list_parser.optionxform = str
        instances_list_parser.read(filenames=instances_file,
                                   encoding="utf-8")
        instances_list = []
        for section in instances_list_parser.sections():
            if "Instance" in section:
                instance_provider = instances_list_parser.get(section, "provider")
                instance_name = instances_list_parser.get(section, "name")
                instance_id = instances_list_parser.get(section, "id")
                instance_key_name = instances_list_parser.get(section, "key_name")
                instance_username = instances_list_parser.get(section, "username")
                instance_public_ipv4_address = instances_list_parser.get(section, "public_ipv4_address")
                instance_ssh_port = instances_list_parser.get(section, "ssh_port")
                instance_dict = {"provider": instance_provider,
                                 "name": instance_name,
                                 "id": instance_id,
                                 "key_name": instance_key_name,
                                 "username": instance_username,
                                 "public_ipv4_address": instance_public_ipv4_address,
                                 "ssh_port": instance_ssh_port}
                instances_list.append(instance_dict)
        del instances_list_parser
        return instances_list

    def get_first_running_master_instance_dict(self,
                                               instances_list: list) -> dict:
        first_running_master_instance_dict = None
        for instance_dict in instances_list:
            instance_name = instance_dict["name"]
            if "master" in instance_name.lower():
                instance_provider = instance_dict["provider"]
                if instance_provider == "AWS":
                    # Parse AWS Config File.
                    aws_config_file_path = self.get_attribute("aws_settings")["config_file_path"]
                    aws_region, aws_output = parse_aws_config_file(aws_config_file_path)
                    # Get AWS Service Setting (EC2).
                    aws_service = self.get_attribute("aws_settings")["service"]
                    # Init AWS EC2Manager Object.
                    ec2m = EC2Manager(service_name=aws_service, region_name=aws_region)
                    instance_id = instance_dict["id"]
                    instance = ec2m.get_ec2_instance_from_id(instance_id)
                    if ec2m.is_ec2_instance_running(instance):
                        first_running_master_instance_dict = instance_dict
                        del ec2m
                        break
        return first_running_master_instance_dict

    def send_application_to_instance(self,
                                     instance_dict: dict) -> None:
        # Get Logger.
        logger = self.get_attribute("logger")
        # Get Key Root Folder.
        key_root_folder = self.get_attribute("general_settings")["key_root_folder"]
        # Get Instance Settings.
        instance_name = instance_dict["name"]
        instance_key_name = instance_dict["key_name"]
        instance_full_key_name = find_full_file_name_by_prefix(key_root_folder,
                                                               instance_key_name)
        instance_key_file = Path(key_root_folder).joinpath(instance_full_key_name)
        instance_key_file_exists = check_if_file_exists(instance_key_file)
        if not instance_key_file_exists:
            message = "The key '{0}' of instance '{1}' could not be found in the '{2}' folder!" \
                .format(instance_key_name,
                        instance_name,
                        key_root_folder)
            log_message(logger, message, "INFO")
            raise FileNotFoundError(message)
        instance_username = instance_dict["username"]
        instance_public_ipv4_address = instance_dict["public_ipv4_address"]
        instance_ssh_port = instance_dict["ssh_port"]
        # Get Configuration Rules Settings.
        configuration_rules_settings = self.get_attribute("configuration_rules_settings")
        max_tries = configuration_rules_settings["max_tries"]
        time_between_retries_in_seconds = configuration_rules_settings["time_between_retries_in_seconds"]
        application_folder = configuration_rules_settings["application_folder"]
        message = "Sending Application to the remote host {0} ({1})..." \
            .format(instance_public_ipv4_address,
                    instance_name)
        log_message(logger, message, "DEBUG")
        # Remotely Create the Applications Root Folder.
        applications_root_folder = Path("application")
        remote_command = "mkdir -p {0}".format(applications_root_folder)
        remotely_execute_command(key_file=instance_key_file,
                                 username=instance_username,
                                 public_ipv4_address=instance_public_ipv4_address,
                                 ssh_port=instance_ssh_port,
                                 remote_command=remote_command,
                                 on_new_windows=False,
                                 request_tty=False,
                                 max_tries=max_tries,
                                 time_between_retries_in_seconds=time_between_retries_in_seconds,
                                 logger=logger,
                                 logger_level="DEBUG")
        # Remotely Create the Application's Destination Folder.
        application_folder_name = Path(application_folder).name
        application_destination_folder = applications_root_folder.joinpath(application_folder_name)
        remote_command = "mkdir -p {0}".format(application_destination_folder)
        remotely_execute_command(key_file=instance_key_file,
                                 username=instance_username,
                                 public_ipv4_address=instance_public_ipv4_address,
                                 ssh_port=instance_ssh_port,
                                 remote_command=remote_command,
                                 on_new_windows=False,
                                 request_tty=False,
                                 max_tries=max_tries,
                                 time_between_retries_in_seconds=time_between_retries_in_seconds,
                                 logger=logger,
                                 logger_level="DEBUG")
        # Send the Application to the Remote Host.
        local_command = "rsync -q -e 'ssh -i {0}' -r {1} {2}@{3}:~/{4}".format(instance_key_file,
                                                                               application_folder,
                                                                               instance_username,
                                                                               instance_public_ipv4_address,
                                                                               application_destination_folder)
        execute_command(command=local_command,
                        on_new_windows=False,
                        max_tries=max_tries,
                        time_between_retries_in_seconds=time_between_retries_in_seconds,
                        logger=logger,
                        logger_level="DEBUG")

    def send_input_to_instance(self,
                               instance_dict: dict) -> None:
        # Get Logger.
        logger = self.get_attribute("logger")
        # Get Key Root Folder.
        key_root_folder = self.get_attribute("general_settings")["key_root_folder"]
        # Get Instance Settings.
        instance_name = instance_dict["name"]
        instance_key_name = instance_dict["key_name"]
        instance_full_key_name = find_full_file_name_by_prefix(key_root_folder,
                                                               instance_key_name)
        instance_key_file = Path(key_root_folder).joinpath(instance_full_key_name)
        instance_key_file_exists = check_if_file_exists(instance_key_file)
        if not instance_key_file_exists:
            message = "The key '{0}' of instance '{1}' could not be found in the '{2}' folder!" \
                .format(instance_key_name,
                        instance_name,
                        key_root_folder)
            log_message(logger, message, "INFO")
            raise FileNotFoundError(message)
        instance_username = instance_dict["username"]
        instance_public_ipv4_address = instance_dict["public_ipv4_address"]
        instance_ssh_port = instance_dict["ssh_port"]
        # Get Configuration Rules Settings.
        configuration_rules_settings = self.get_attribute("configuration_rules_settings")
        max_tries = configuration_rules_settings["max_tries"]
        time_between_retries_in_seconds = configuration_rules_settings["time_between_retries_in_seconds"]
        application_folder = configuration_rules_settings["application_folder"]
        input_folder = configuration_rules_settings["input_folder"]
        message = "Sending Input to the remote host {0} ({1})..." \
            .format(instance_public_ipv4_address,
                    instance_name)
        log_message(logger, message, "DEBUG")
        # Remotely Create the Inputs Root Folder.
        inputs_root_folder = Path("input")
        remote_command = "mkdir -p {0}".format(inputs_root_folder)
        remotely_execute_command(key_file=instance_key_file,
                                 username=instance_username,
                                 public_ipv4_address=instance_public_ipv4_address,
                                 ssh_port=instance_ssh_port,
                                 remote_command=remote_command,
                                 on_new_windows=False,
                                 request_tty=False,
                                 max_tries=max_tries,
                                 time_between_retries_in_seconds=time_between_retries_in_seconds,
                                 logger=logger,
                                 logger_level="DEBUG")
        # Remotely Create the Application's Input Destination Folder.
        application_folder_name = Path(application_folder).name
        application_input_destination_folder = inputs_root_folder.joinpath(application_folder_name)
        remote_command = "mkdir -p {0}".format(application_input_destination_folder)
        remotely_execute_command(key_file=instance_key_file,
                                 username=instance_username,
                                 public_ipv4_address=instance_public_ipv4_address,
                                 ssh_port=instance_ssh_port,
                                 remote_command=remote_command,
                                 on_new_windows=False,
                                 request_tty=False,
                                 max_tries=max_tries,
                                 time_between_retries_in_seconds=time_between_retries_in_seconds,
                                 logger=logger,
                                 logger_level="DEBUG")
        # Send the Input to the Remote Host.
        local_command = "rsync -q -e 'ssh -i {0}' -r {1} {2}@{3}:~/{4}".format(instance_key_file,
                                                                               input_folder,
                                                                               instance_username,
                                                                               instance_public_ipv4_address,
                                                                               application_input_destination_folder)
        execute_command(command=local_command,
                        on_new_windows=False,
                        max_tries=max_tries,
                        time_between_retries_in_seconds=time_between_retries_in_seconds,
                        logger=logger,
                        logger_level="DEBUG")

    def start_spark_job_on_master_instance(self,
                                           instance_dict: dict) -> None:
        # Get Logger.
        logger = self.get_attribute("logger")
        # Get Key Root Folder.
        key_root_folder = self.get_attribute("general_settings")["key_root_folder"]
        # Get Instance Settings.
        instance_name = instance_dict["name"]
        instance_key_name = instance_dict["key_name"]
        instance_full_key_name = find_full_file_name_by_prefix(key_root_folder,
                                                               instance_key_name)
        instance_key_file = Path(key_root_folder).joinpath(instance_full_key_name)
        instance_key_file_exists = check_if_file_exists(instance_key_file)
        if not instance_key_file_exists:
            message = "The key '{0}' of instance '{1}' could not be found in the '{2}' folder!" \
                .format(instance_key_name,
                        instance_name,
                        key_root_folder)
            log_message(logger, message, "INFO")
            raise FileNotFoundError(message)
        instance_username = instance_dict["username"]
        instance_public_ipv4_address = instance_dict["public_ipv4_address"]
        instance_ssh_port = instance_dict["ssh_port"]
        # Get Configuration Rules Settings.
        configuration_rules_settings = self.get_attribute("configuration_rules_settings")
        max_tries = configuration_rules_settings["max_tries"]
        time_between_retries_in_seconds = configuration_rules_settings["time_between_retries_in_seconds"]
        spark_version = configuration_rules_settings["spark_version"]
        hadoop_version = configuration_rules_settings["hadoop_version"]
        master_port = configuration_rules_settings["master_port"]
        master_properties_file = configuration_rules_settings["master_properties_file"]
        application_entry_point = configuration_rules_settings["application_entry_point"]
        application_arguments = configuration_rules_settings["application_arguments"]
        message = "Launching the Spark application on the remote host {0} ({1})..." \
            .format(instance_public_ipv4_address,
                    instance_name)
        log_message(logger, message, "INFO")
        # Remotely Launch the Spark Application on Master.
        spark_home_directory = Path("spark-{0}-bin-hadoop{1}".format(spark_version, hadoop_version))
        spark_submit_script_folder = spark_home_directory.joinpath("bin")
        spark_submit_script_file = spark_submit_script_folder.joinpath("spark-submit")

        master_url_option = "--master spark://{0}:{1}".format(instance_public_ipv4_address, master_port)
        master_properties_file_option = "--properties-file {0}".format(master_properties_file)
        remote_command = "{0} {1} {2} {3} {4}" \
            .format(spark_submit_script_file,
                    master_url_option,
                    master_properties_file_option,
                    application_entry_point,
                    application_arguments)
        remotely_execute_command(key_file=instance_key_file,
                                 username=instance_username,
                                 public_ipv4_address=instance_public_ipv4_address,
                                 ssh_port=instance_ssh_port,
                                 remote_command=remote_command,
                                 on_new_windows=True,
                                 request_tty=False,
                                 max_tries=max_tries,
                                 time_between_retries_in_seconds=time_between_retries_in_seconds,
                                 logger=logger,
                                 logger_level="DEBUG")

    def execute_spark_job_tasks(self,
                                cluster_name: str) -> None:
        # Get Execution Mode.
        execution_mode = self.get_attribute("execution_mode")
        # Read Cluster's Instances File.
        instances_list = self.read_instances_file(cluster_name)
        # Get the First Running Master Instance.
        first_running_master_instance_dict = self.get_first_running_master_instance_dict(instances_list)
        number_of_instances = len(instances_list)
        if execution_mode == "full":
            # Parallel Send the Spark Application and Input for Instances (Masters and Workers).
            with ThreadPoolExecutor(max_workers=number_of_instances) as thread_pool_executor:
                for instance_dict in instances_list:
                    instance_name = instance_dict["name"]
                    if "master" in instance_name.lower():
                        future = thread_pool_executor.submit(self.send_application_to_instance,
                                                             instance_dict)
                        exception = future.exception()
                        if exception:
                            raise exception
                        future = thread_pool_executor.submit(self.send_input_to_instance,
                                                             instance_dict)
                        exception = future.exception()
                        if exception:
                            raise exception
                    elif "worker" in instance_name.lower():
                        future = thread_pool_executor.submit(self.send_application_to_instance,
                                                             instance_dict)
                        exception = future.exception()
                        if exception:
                            raise exception
                        future = thread_pool_executor.submit(self.send_input_to_instance,
                                                             instance_dict)
                        exception = future.exception()
                        if exception:
                            raise exception
        # Launch the Spark Job on the Master Instance.
        self.start_spark_job_on_master_instance(first_running_master_instance_dict)

    def parallel_execute_spark_jobs(self,
                                    cluster_names: list) -> None:
        # Get Number of Clusters.
        number_of_clusters = len(cluster_names)
        with ThreadPoolExecutor(max_workers=number_of_clusters) as thread_pool_executor:
            for cluster_name in cluster_names:
                # Get Logger.
                logger = self.get_attribute("logger")
                message = "Launching the Spark job on the Cluster '{0}'...".format(cluster_name)
                log_message(logger, message, "INFO")
                future = thread_pool_executor.submit(self.execute_spark_job_tasks,
                                                     cluster_name)
                exception = future.exception()
                if exception:
                    exit(exception)
                message = "The Cluster '{0}' has launched the Spark job successfully!".format(cluster_name)
                log_message(logger, message, "INFO")


def execute_spark_job(arguments_dict: dict) -> None:
    # Get Arguments.
    sparking_cloud_config_file = arguments_dict["sparking_cloud_config_file"]
    cluster_names = arguments_dict["cluster_names"]
    execution_mode = arguments_dict["execution_mode"]
    # Get Cluster Names List.
    cluster_names_list = cluster_names.split(",")
    # Init Config Parser Object.
    cp = ConfigParser()
    cp.optionxform = str
    cp.read(filenames=sparking_cloud_config_file, encoding="utf-8")
    # Init Spark Job Executor Object.
    sje = SparkJobExecutor(sparking_cloud_config_file)
    # Parse Sparking Cloud Config File and Set Attributes.
    sparking_cloud_settings_dict = parse_sparking_cloud_config_file(cp)
    for k, v in sparking_cloud_settings_dict.items():
        sje.set_attribute(k, v)
    # Check if Logging is Enabled.
    enable_logging = sje.get_attribute("general_settings")["enable_logging"]
    # Get Logging Settings.
    logging_settings = sje.get_attribute("logging_settings")
    # Instantiate and Set Logger.
    logger = load_logger(enable_logging, logging_settings)
    sje.set_attribute("logger", logger)
    # Set Execution Mode.
    sje.set_attribute("execution_mode", execution_mode)
    # Parallel Execute Spark Jobs.
    sje.parallel_execute_spark_jobs(cluster_names_list)
    # Unbind Objects (Garbage Collector).
    del cp
    del sje
    del logger


if __name__ == "__main__":
    # Begin.
    # Parse Cluster Configurator Arguments.
    ag = ArgumentParser(description="Cluster Configurator Arguments")
    ag.add_argument("--sparking_cloud_config_file",
                    type=Path,
                    required=False,
                    default=Path("config/sparking_cloud.cfg"),
                    help="Sparking Cloud Config File (default: config/sparking_cloud.cfg)")
    ag.add_argument("--cluster_names",
                    type=str,
                    required=True,
                    help="Cluster Names (no default)")
    ag.add_argument("--execution_mode",
                    type=str,
                    required=True,
                    help="Execution Mode (e.g., full)")
    parsed_args = ag.parse_args()
    # Generate Arguments Dict.
    args_dict = {"sparking_cloud_config_file": Path(parsed_args.sparking_cloud_config_file),
                 "cluster_names": str(parsed_args.cluster_names),
                 "execution_mode": str(parsed_args.execution_mode)}
    # Execute Spark Job.
    execute_spark_job(args_dict)
    # Unbind Objects (Garbage Collector).
    del ag
    # End.
    exit(0)
