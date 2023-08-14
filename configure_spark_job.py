from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor, wait
from configparser import ConfigParser
from pathlib import Path
from typing import Any
from cloud_manager.ec2_manager import EC2Manager
from util.aws_config_util import parse_aws_config_file, parse_aws_credentials_file
from util.logging_util import load_logger, log_message
from util.os_util import check_if_file_exists, find_full_file_name_by_prefix
from util.process_util import execute_command, remotely_execute_command
from util.sparking_cloud_util import parse_sparking_cloud_config_file


class SparkJobConfigurator:

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
        self.configuration_mode = None

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

    def send_application_settings_files_to_instance(self,
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
        # Remotely Create the Spark Defaults Conf File's Destination Folder.
        destination_folder = Path("config")
        remote_command = "mkdir -p {0}".format(destination_folder)
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
        properties_file = configuration_rules_settings["properties_file"]
        pool_properties_file = configuration_rules_settings["pool_properties_file"]
        # Send the Spark Defaults and the Spark Scheduler Allocation Files to the Remote Host.
        local_command = "rsync -q -e 'ssh -i {0}' -r {1} {2} {3}@{4}:~/{5}".format(instance_key_file,
                                                                                   properties_file,
                                                                                   pool_properties_file,
                                                                                   instance_username,
                                                                                   instance_public_ipv4_address,
                                                                                   destination_folder)
        execute_command(command=local_command,
                        on_new_windows=False,
                        max_tries=max_tries,
                        time_between_retries_in_seconds=time_between_retries_in_seconds,
                        logger=logger,
                        logger_level="DEBUG")
        # Remotely Update the Spark Hadoop FS S3A Credentials.
        aws_credentials_file_path = self.get_attribute("aws_settings")["credentials_file_path"]
        aws_access_key_id, aws_secret_access_key = parse_aws_credentials_file(aws_credentials_file_path)
        remote_command = "sed -i 's^{0}^{1}^g' {2}".format("AWS_ACCESS_KEY_ID",
                                                           aws_access_key_id,
                                                           properties_file)
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
        remote_command = "sed -i 's^{0}^{1}^g' {2}".format("AWS_SECRET_ACCESS_KEY",
                                                           aws_secret_access_key,
                                                           properties_file)
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
        local_command = "rsync -q -e 'ssh -i {0}' -r {1}/* {2}@{3}:~/{4}".format(instance_key_file,
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

    def configure_spark_job_tasks(self,
                                  cluster_name: str) -> None:
        # Get Configuration Mode.
        configuration_mode = self.get_attribute("configuration_mode")
        # Read Cluster's Instances File.
        instances_list = self.read_instances_file(cluster_name)
        # Get Input Folder.
        send_local_input_folder = self.get_attribute("configuration_rules_settings")["send_local_input_folder"]
        if configuration_mode == "full":
            # Parallel Send the Spark Application, Settings Files, and Input Files for Master Instance.
            # Parallel Send the Spark Application and Input Files for Worker Instances.
            with ThreadPoolExecutor() as thread_pool_executor:
                for instance_dict in instances_list:
                    instance_name = instance_dict["name"]
                    if "master" in instance_name.lower():
                        thread_pool_executor.submit(self.send_application_to_instance,
                                                    instance_dict)
                        thread_pool_executor.submit(self.send_application_settings_files_to_instance,
                                                    instance_dict)
                        if send_local_input_folder:
                            thread_pool_executor.submit(self.send_input_to_instance,
                                                        instance_dict)
                    elif "worker" in instance_name.lower():
                        thread_pool_executor.submit(self.send_application_to_instance,
                                                    instance_dict)
                        if send_local_input_folder:
                            thread_pool_executor.submit(self.send_input_to_instance,
                                                        instance_dict)

    def parallel_configure_spark_jobs(self,
                                      cluster_names: list) -> None:
        with ThreadPoolExecutor() as thread_pool_executor:
            for cluster_name in cluster_names:
                # Get Logger.
                logger = self.get_attribute("logger")
                message = "Configuring the Spark job on the Cluster '{0}'...".format(cluster_name)
                log_message(logger, message, "INFO")
                future = thread_pool_executor.submit(self.configure_spark_job_tasks,
                                                     cluster_name)
                wait([future])
                message = "The Spark job was successfully configured on the Cluster '{0}'!".format(cluster_name)
                log_message(logger, message, "INFO")


def configure_spark_job(arguments_dict: dict) -> None:
    # Get Arguments.
    sparking_cloud_config_file = arguments_dict["sparking_cloud_config_file"]
    cluster_names = arguments_dict["cluster_names"]
    configuration_mode = arguments_dict["configuration_mode"]
    # Get Cluster Names List.
    cluster_names_list = cluster_names.split(",")
    # Init Config Parser Object.
    cp = ConfigParser()
    cp.optionxform = str
    cp.read(filenames=sparking_cloud_config_file, encoding="utf-8")
    # Init Spark Job Configurator Object.
    sjc = SparkJobConfigurator(sparking_cloud_config_file)
    # Parse Sparking Cloud Config File and Set Attributes.
    sparking_cloud_settings_dict = parse_sparking_cloud_config_file(cp)
    for k, v in sparking_cloud_settings_dict.items():
        sjc.set_attribute(k, v)
    # Check if Logging is Enabled.
    enable_logging = sjc.get_attribute("general_settings")["enable_logging"]
    # Get Logging Settings.
    logging_settings = sjc.get_attribute("logging_settings")
    # Instantiate and Set Logger.
    logger = load_logger(enable_logging, logging_settings)
    sjc.set_attribute("logger", logger)
    # Set Configuration Mode.
    sjc.set_attribute("configuration_mode", configuration_mode)
    # Parallel Configure Spark Jobs.
    sjc.parallel_configure_spark_jobs(cluster_names_list)
    # Unbind Objects (Garbage Collector).
    del cp
    del sjc
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
    ag.add_argument("--configuration_mode",
                    type=str,
                    required=True,
                    help="Configuration Mode (e.g., full)")
    parsed_args = ag.parse_args()
    # Generate Arguments Dict.
    args_dict = {"sparking_cloud_config_file": Path(parsed_args.sparking_cloud_config_file),
                 "cluster_names": str(parsed_args.cluster_names),
                 "configuration_mode": str(parsed_args.configuration_mode)}
    # Configure Spark Job.
    configure_spark_job(args_dict)
    # Unbind Objects (Garbage Collector).
    del ag
    # End.
    exit(0)
