from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser
from pathlib import Path
from typing import Any
from cloud_manager.ec2_manager import EC2Manager
from util.aws_config_util import parse_aws_config_file
from util.logging_util import load_logger, log_message
from util.os_util import find_full_file_name_by_prefix
from util.process_util import execute_command, remotely_execute_command
from util.sparking_cloud_util import parse_sparking_cloud_config_file


class SparkStarter:

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

    def start_spark_on_master_instance(self,
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
        master_webui_port = configuration_rules_settings["master_webui_port"]
        master_properties_file = configuration_rules_settings["master_properties_file"]
        master_scheduler_file = configuration_rules_settings["master_scheduler_file"]
        message = "Starting Spark on the remote host {0} ({1})...".format(instance_public_ipv4_address, instance_name)
        log_message(logger, message, "INFO")
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
        # Send the Spark Defaults and the Spark Scheduler Allocation Files to the Remote Host.
        local_command = "rsync -q -e 'ssh -i {0}' -r {1} {2} {3}@{4}:~/{5}".format(instance_key_file,
                                                                                   master_properties_file,
                                                                                   master_scheduler_file,
                                                                                   instance_username,
                                                                                   instance_public_ipv4_address,
                                                                                   destination_folder)
        execute_command(command=local_command,
                        on_new_windows=False,
                        max_tries=max_tries,
                        time_between_retries_in_seconds=time_between_retries_in_seconds,
                        logger=logger,
                        logger_level="DEBUG")
        # Remotely Execute the Spark Start Master Script.
        spark_home_directory = Path("spark-{0}-bin-hadoop{1}".format(spark_version, hadoop_version))
        spark_scripts_folder = spark_home_directory.joinpath("sbin")
        spark_start_master_script_file = spark_scripts_folder.joinpath("start-master.sh")
        master_port_option = "--port {0}".format(master_port)
        master_webui_port_option = "--webui-port {0}".format(master_webui_port)
        master_properties_file_option = "--properties-file {0}".format(master_properties_file)
        remote_command = "bash {0} {1} {2} {3}" \
            .format(spark_start_master_script_file,
                    master_port_option,
                    master_webui_port_option,
                    master_properties_file_option)
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

    def get_number_of_cpu_cores(self,
                                instance_dict: dict) -> int:
        # Get Logger.
        logger = self.get_attribute("logger")
        # Get Key Root Folder.
        key_root_folder = self.get_attribute("general_settings")["key_root_folder"]
        # Get Instance Settings.
        instance_key_name = instance_dict["key_name"]
        instance_full_key_name = find_full_file_name_by_prefix(key_root_folder,
                                                               instance_key_name)
        instance_key_file = Path(key_root_folder).joinpath(instance_full_key_name)
        instance_username = instance_dict["username"]
        instance_public_ipv4_address = instance_dict["public_ipv4_address"]
        instance_ssh_port = instance_dict["ssh_port"]
        # Get Configuration Rules Settings.
        configuration_rules_settings = self.get_attribute("configuration_rules_settings")
        max_tries = configuration_rules_settings["max_tries"]
        time_between_retries_in_seconds = configuration_rules_settings["time_between_retries_in_seconds"]
        remote_command = "grep 'cpu cores' /proc/cpuinfo | awk '{print \\$4;}' | uniq"
        process_stdout = remotely_execute_command(key_file=instance_key_file,
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
        number_of_cpu_cores = int(process_stdout[0])
        return number_of_cpu_cores

    def get_memory_size_in_kilobytes(self,
                                     instance_dict: dict) -> int:
        # Get Logger.
        logger = self.get_attribute("logger")
        # Get Key Root Folder.
        key_root_folder = self.get_attribute("general_settings")["key_root_folder"]
        # Get Instance Settings.
        instance_key_name = instance_dict["key_name"]
        instance_full_key_name = find_full_file_name_by_prefix(key_root_folder,
                                                               instance_key_name)
        instance_key_file = Path(key_root_folder).joinpath(instance_full_key_name)
        instance_username = instance_dict["username"]
        instance_public_ipv4_address = instance_dict["public_ipv4_address"]
        instance_ssh_port = instance_dict["ssh_port"]
        # Get Configuration Rules Settings.
        configuration_rules_settings = self.get_attribute("configuration_rules_settings")
        max_tries = configuration_rules_settings["max_tries"]
        time_between_retries_in_seconds = configuration_rules_settings["time_between_retries_in_seconds"]
        remote_command = "grep 'MemTotal' /proc/meminfo | awk '{print \\$2;}'"
        process_stdout = remotely_execute_command(key_file=instance_key_file,
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
        memory_size_in_kilobytes = int(process_stdout[0])
        return memory_size_in_kilobytes

    def start_spark_on_worker_instance(self,
                                       instance_dict: dict,
                                       master_public_ipv4_address: str) -> None:
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
        worker_cores = configuration_rules_settings["worker_cores"]
        if worker_cores == "maximum":
            worker_cores = self.get_number_of_cpu_cores(instance_dict)
        worker_memory = configuration_rules_settings["worker_memory"]
        if worker_memory == "maximum":
            worker_memory = self.get_memory_size_in_kilobytes(instance_dict)
        worker_memory_unit = configuration_rules_settings["worker_memory_unit"]
        worker_port = configuration_rules_settings["worker_port"]
        worker_webui_port = configuration_rules_settings["worker_webui_port"]
        worker_properties_file = configuration_rules_settings["worker_properties_file"]
        message = "Starting Spark on the remote host {0} ({1})...".format(instance_public_ipv4_address, instance_name)
        log_message(logger, message, "INFO")
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
        # Send the Spark Defaults File to the Remote Host.
        local_command = "rsync -q -e 'ssh -i {0}' -r {1} {2}@{3}:~/{4}".format(instance_key_file,
                                                                               worker_properties_file,
                                                                               instance_username,
                                                                               instance_public_ipv4_address,
                                                                               destination_folder)
        execute_command(command=local_command,
                        on_new_windows=False,
                        max_tries=max_tries,
                        time_between_retries_in_seconds=time_between_retries_in_seconds,
                        logger=logger,
                        logger_level="DEBUG")
        # Remotely Execute the Spark Start Worker Script.
        spark_home_directory = Path("spark-{0}-bin-hadoop{1}".format(spark_version, hadoop_version))
        spark_scripts_folder = spark_home_directory.joinpath("sbin")
        spark_start_worker_script_file = spark_scripts_folder.joinpath("start-worker.sh")
        master_url_option = "spark://{0}:{1}".format(master_public_ipv4_address, master_port)
        worker_cores_option = "--cores {0}".format(worker_cores)
        worker_memory_option = "--memory {0}{1}".format(worker_memory, worker_memory_unit[0])
        worker_port_option = "--port {0}".format(worker_port)
        worker_webui_port_option = "--webui-port {0}".format(worker_webui_port)
        worker_properties_file_option = "--properties-file {0}".format(worker_properties_file)
        remote_command = "bash {0} {1} {2} {3} {4} {5} {6}" \
            .format(spark_start_worker_script_file,
                    master_url_option,
                    worker_cores_option,
                    worker_memory_option,
                    worker_port_option,
                    worker_webui_port_option,
                    worker_properties_file_option)
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

    def start_spark_cluster_tasks(self,
                                  cluster_name: str) -> None:
        # Get Logger.
        logger = self.get_attribute("logger")
        # Read Cluster's Instances File.
        instances_list = self.read_instances_file(cluster_name)
        message = "Starting Spark on the Cluster '{0}'...".format(cluster_name)
        log_message(logger, message, "INFO")
        number_of_instances = len(instances_list)
        # Get the First Running Master Instance.
        first_running_master_instance_dict = self.get_first_running_master_instance_dict(instances_list)
        master_public_ipv4_address = first_running_master_instance_dict["public_ipv4_address"]
        # Parallel Remotely Start Spark on Instances (Masters and Workers).
        with ThreadPoolExecutor(max_workers=number_of_instances) as thread_pool_executor:
            for instance_dict in instances_list:
                instance_name = instance_dict["name"]
                if "master" in instance_name.lower():
                    thread_pool_executor.submit(self.start_spark_on_master_instance,
                                                instance_dict)
                elif "worker" in instance_name.lower():
                    thread_pool_executor.submit(self.start_spark_on_worker_instance,
                                                instance_dict,
                                                master_public_ipv4_address)
        message = "The Cluster '{0}' has started Spark successfully!".format(cluster_name)
        log_message(logger, message, "INFO")

    def parallel_start_spark_clusters(self,
                                      cluster_names: list) -> None:
        # Get Number of Clusters.
        number_of_clusters = len(cluster_names)
        with ThreadPoolExecutor(max_workers=number_of_clusters) as thread_pool_executor:
            for cluster_name in cluster_names:
                thread_pool_executor.submit(self.start_spark_cluster_tasks,
                                            cluster_name)


def main() -> None:
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
    # Get Cluster Configurator Arguments.
    sparking_cloud_config_file = Path(parsed_args.sparking_cloud_config_file)
    cluster_names = str(parsed_args.cluster_names)
    cluster_names_list = cluster_names.split(",")
    # Init Config Parser Object.
    cp = ConfigParser()
    cp.optionxform = str
    cp.read(filenames=sparking_cloud_config_file, encoding="utf-8")
    # Init Spark Starter Object.
    ss = SparkStarter(sparking_cloud_config_file)
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
    # Parallel Start Spark Clusters.
    ss.parallel_start_spark_clusters(cluster_names_list)
    # Unbind Objects (Garbage Collector).
    del cp
    del ss
    del logger
    # End.
    exit(0)


if __name__ == "__main__":
    main()
