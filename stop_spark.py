from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser
from pathlib import Path
from typing import Any
from util.logging_util import load_logger, log_message
from util.os_util import check_if_file_exists, find_full_file_name_by_prefix
from util.process_util import remotely_execute_command
from util.sparking_cloud_util import parse_sparking_cloud_config_file


class SparkStopper:

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

    def stop_spark_on_master_instance(self,
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
        message = "Stopping Spark on the remote host {0} ({1})...".format(instance_public_ipv4_address, instance_name)
        log_message(logger, message, "DEBUG")
        # Remotely Execute the Spark Stop Master Script.
        spark_home_directory = Path("spark-{0}-bin-hadoop{1}".format(spark_version, hadoop_version))
        spark_scripts_folder = spark_home_directory.joinpath("sbin")
        spark_stop_master_script_file = spark_scripts_folder.joinpath("stop-master.sh")
        remote_command = "bash {0}".format(spark_stop_master_script_file)
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

    def stop_spark_on_worker_instance(self,
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
        message = "Stopping Spark on the remote host {0} ({1})...".format(instance_public_ipv4_address, instance_name)
        log_message(logger, message, "DEBUG")
        # Remotely Execute the Spark Stop Worker Script.
        spark_home_directory = Path("spark-{0}-bin-hadoop{1}".format(spark_version, hadoop_version))
        spark_scripts_folder = spark_home_directory.joinpath("sbin")
        spark_stop_worker_script_file = spark_scripts_folder.joinpath("stop-worker.sh")
        remote_command = "bash {0}".format(spark_stop_worker_script_file)
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

    def stop_spark_cluster_tasks(self,
                                 cluster_name: str) -> None:
        # Read Cluster's Instances File.
        instances_list = self.read_instances_file(cluster_name)
        number_of_instances = len(instances_list)
        # Parallel Remotely Stop Spark on Instances (Masters and Workers).
        with ThreadPoolExecutor(max_workers=number_of_instances) as thread_pool_executor:
            for instance_dict in instances_list:
                instance_name = instance_dict["name"]
                if "master" in instance_name.lower():
                    future = thread_pool_executor.submit(self.stop_spark_on_master_instance,
                                                         instance_dict)
                    exception = future.exception()
                    if exception:
                        raise exception
                elif "worker" in instance_name.lower():
                    future = thread_pool_executor.submit(self.stop_spark_on_worker_instance,
                                                         instance_dict)
                    exception = future.exception()
                    if exception:
                        raise exception

    def parallel_stop_spark_clusters(self,
                                     cluster_names: list) -> None:
        # Get Number of Clusters.
        number_of_clusters = len(cluster_names)
        with ThreadPoolExecutor(max_workers=number_of_clusters) as thread_pool_executor:
            for cluster_name in cluster_names:
                # Get Logger.
                logger = self.get_attribute("logger")
                message = "Stopping Spark on the Cluster '{0}'...".format(cluster_name)
                log_message(logger, message, "INFO")
                future = thread_pool_executor.submit(self.stop_spark_cluster_tasks,
                                                     cluster_name)
                exception = future.exception()
                if exception:
                    exit(exception)
                message = "The Cluster '{0}' has stopped Spark successfully!".format(cluster_name)
                log_message(logger, message, "INFO")


def stop_spark(arguments_dict: dict) -> None:
    # Get Arguments.
    sparking_cloud_config_file = arguments_dict["sparking_cloud_config_file"]
    cluster_names = arguments_dict["cluster_names"]
    # Get Cluster Names List.
    cluster_names_list = cluster_names.split(",")
    # Init Config Parser Object.
    cp = ConfigParser()
    cp.optionxform = str
    cp.read(filenames=sparking_cloud_config_file, encoding="utf-8")
    # Init Spark Stopper Object.
    ss = SparkStopper(sparking_cloud_config_file)
    # Parse Sparking Cloud Config File and Set Attributes.
    sparking_cloud_settings_dict = parse_sparking_cloud_config_file(cp)
    for k, v in sparking_cloud_settings_dict.items():
        ss.set_attribute(k, v)
    # Check if Logging is Enabled.
    enable_logging = ss.get_attribute("general_settings")["enable_logging"]
    # Get Logging Settings.
    logging_settings = ss.get_attribute("logging_settings")
    # Instantiate and Set Logger.
    logger = load_logger(enable_logging, logging_settings)
    ss.set_attribute("logger", logger)
    # Parallel Stop Spark Clusters.
    ss.parallel_stop_spark_clusters(cluster_names_list)
    # Unbind Objects (Garbage Collector).
    del cp
    del ss
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
    parsed_args = ag.parse_args()
    # Generate Arguments Dict.
    args_dict = {"sparking_cloud_config_file": Path(parsed_args.sparking_cloud_config_file),
                 "cluster_names": str(parsed_args.cluster_names)}
    # Stop Spark.
    stop_spark(args_dict)
    # Unbind Objects (Garbage Collector).
    del ag
    # End.
    exit(0)
