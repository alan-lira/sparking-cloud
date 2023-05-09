from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor, wait
from configparser import ConfigParser
from pathlib import Path
from typing import Any
from util.logging_util import load_logger, log_message
from util.os_util import check_if_file_exists, find_full_file_name_by_prefix
from util.process_util import execute_command, remotely_execute_command
from util.sparking_cloud_util import parse_sparking_cloud_config_file


class ClusterConfigurator:

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

    def store_instance_public_key_on_known_hosts(self,
                                                 instance_public_ipv4_address: str) -> None:
        configuration_rules_settings = self.get_attribute("configuration_rules_settings")
        store_remote_host_public_key_to_guest_known_hosts = \
            configuration_rules_settings["store_remote_host_public_key_to_guest_known_hosts"]
        if store_remote_host_public_key_to_guest_known_hosts:
            logger = self.get_attribute("logger")
            max_tries = configuration_rules_settings["max_tries"]
            time_between_retries_in_seconds = configuration_rules_settings["time_between_retries_in_seconds"]
            verbose_scripts = configuration_rules_settings["verbose_scripts"]
            store_remote_host_public_key_script_file = \
                configuration_rules_settings["store_remote_host_public_key_script_file"]
            key_types = configuration_rules_settings["key_types"]
            known_hosts_file = configuration_rules_settings["known_hosts_file"]
            commands_string = "bash {0} {1} {2} {3} {4}" \
                .format(store_remote_host_public_key_script_file,
                        str(instance_public_ipv4_address),
                        str(key_types),
                        Path(known_hosts_file),
                        bool(verbose_scripts))
            message = "Storing the remote host {0}'s public key ({1}) on {2} file..." \
                .format(instance_public_ipv4_address,
                        key_types,
                        known_hosts_file)
            log_message(logger, message, "DEBUG")
            execute_command(command=commands_string,
                            on_new_windows=False,
                            max_tries=max_tries,
                            time_between_retries_in_seconds=time_between_retries_in_seconds,
                            logger=logger,
                            logger_level="DEBUG")

    def setup_hadoop_on_master_instance(self,
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
        verbose_scripts = configuration_rules_settings["verbose_scripts"]
        hadoop_setup_on_master_script_file = configuration_rules_settings["hadoop_setup_on_master_script_file"]
        hadoop_version = configuration_rules_settings["hadoop_version"]
        message = "Setting up Hadoop on the remote host {0} ({1})..." \
            .format(instance_public_ipv4_address,
                    instance_name)
        log_message(logger, message, "DEBUG")
        # Remotely Create the Destination Folder.
        destination_folder = Path("script")
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
        # Send the Hadoop Setup Script to the Remote Host.
        local_command = "rsync -q -e 'ssh -i {0}' -r {1} {2}@{3}:~/{4}".format(instance_key_file,
                                                                               hadoop_setup_on_master_script_file,
                                                                               instance_username,
                                                                               instance_public_ipv4_address,
                                                                               destination_folder)
        execute_command(command=local_command,
                        on_new_windows=False,
                        max_tries=max_tries,
                        time_between_retries_in_seconds=time_between_retries_in_seconds,
                        logger=logger,
                        logger_level="DEBUG")
        # Remotely Execute the Hadoop Setup Script.
        remote_command = "bash {0} {1} {2}" \
            .format(hadoop_setup_on_master_script_file,
                    str(hadoop_version),
                    bool(verbose_scripts))
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
        # Remotely Delete the Hadoop Setup Script.
        remote_command = "rm -rf {0}".format(hadoop_setup_on_master_script_file)
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

    def setup_spark_on_master_instance(self,
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
        verbose_scripts = configuration_rules_settings["verbose_scripts"]
        spark_setup_on_master_script_file = configuration_rules_settings["spark_setup_on_master_script_file"]
        spark_version = configuration_rules_settings["spark_version"]
        message = "Setting up Spark on the remote host {0} ({1})..." \
            .format(instance_public_ipv4_address,
                    instance_name)
        log_message(logger, message, "DEBUG")
        # Remotely Create the Destination Folder.
        destination_folder = Path("script")
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
        # Send the Spark Setup Script to the Remote Host.
        local_command = "rsync -q -e 'ssh -i {0}' -r {1} {2}@{3}:~/{4}".format(instance_key_file,
                                                                               spark_setup_on_master_script_file,
                                                                               instance_username,
                                                                               instance_public_ipv4_address,
                                                                               destination_folder)
        execute_command(command=local_command,
                        on_new_windows=False,
                        max_tries=max_tries,
                        time_between_retries_in_seconds=time_between_retries_in_seconds,
                        logger=logger,
                        logger_level="DEBUG")
        # Remotely Execute the Spark Setup Script.
        remote_command = "bash {0} {1} {2}" \
            .format(spark_setup_on_master_script_file,
                    str(spark_version),
                    bool(verbose_scripts))
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
        # Remotely Delete the Spark Setup Script.
        remote_command = "rm -rf {0}".format(spark_setup_on_master_script_file)
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

    def setup_hadoop_on_worker_instance(self,
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
        verbose_scripts = configuration_rules_settings["verbose_scripts"]
        hadoop_setup_on_worker_script_file = configuration_rules_settings["hadoop_setup_on_worker_script_file"]
        hadoop_version = configuration_rules_settings["hadoop_version"]
        message = "Setting up Hadoop on the remote host {0} ({1})..." \
            .format(instance_public_ipv4_address,
                    instance_name)
        log_message(logger, message, "DEBUG")
        # Remotely Create the Destination Folder.
        destination_folder = Path("script")
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
        # Send the Hadoop Setup Script to the Remote Host.
        local_command = "rsync -q -e 'ssh -i {0}' -r {1} {2}@{3}:~/{4}".format(instance_key_file,
                                                                               hadoop_setup_on_worker_script_file,
                                                                               instance_username,
                                                                               instance_public_ipv4_address,
                                                                               destination_folder)
        execute_command(command=local_command,
                        on_new_windows=False,
                        max_tries=max_tries,
                        time_between_retries_in_seconds=time_between_retries_in_seconds,
                        logger=logger,
                        logger_level="DEBUG")
        # Remotely Execute the Hadoop Setup Script.
        remote_command = "bash {0} {1} {2}" \
            .format(hadoop_setup_on_worker_script_file,
                    str(hadoop_version),
                    bool(verbose_scripts))
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
        # Remotely Delete the Hadoop Setup Script.
        remote_command = "rm -rf {0}".format(hadoop_setup_on_worker_script_file)
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

    def setup_spark_on_worker_instance(self,
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
        verbose_scripts = configuration_rules_settings["verbose_scripts"]
        spark_setup_on_worker_script_file = configuration_rules_settings["spark_setup_on_worker_script_file"]
        spark_version = configuration_rules_settings["spark_version"]
        message = "Setting up Spark on the remote host {0} ({1})..." \
            .format(instance_public_ipv4_address,
                    instance_name)
        log_message(logger, message, "DEBUG")
        # Remotely Create the Destination Folder.
        destination_folder = Path("script")
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
        # Send the Spark Setup Script to the Remote Host.
        local_command = "rsync -q -e 'ssh -i {0}' -r {1} {2}@{3}:~/{4}".format(instance_key_file,
                                                                               spark_setup_on_worker_script_file,
                                                                               instance_username,
                                                                               instance_public_ipv4_address,
                                                                               destination_folder)
        execute_command(command=local_command,
                        on_new_windows=False,
                        max_tries=max_tries,
                        time_between_retries_in_seconds=time_between_retries_in_seconds,
                        logger=logger,
                        logger_level="DEBUG")
        # Remotely Execute the Spark Setup Script.
        remote_command = "bash {0} {1} {2}" \
            .format(spark_setup_on_worker_script_file,
                    str(spark_version),
                    bool(verbose_scripts))
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
        # Remotely Delete the Spark Setup Script.
        remote_command = "rm -rf {0}".format(spark_setup_on_worker_script_file)
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

    def configure_cluster_tasks(self,
                                cluster_name: str) -> None:
        # Get Configuration Rules Settings.
        configuration_rules_settings = self.get_attribute("configuration_rules_settings")
        install_hadoop = configuration_rules_settings["install_hadoop"]
        install_spark = configuration_rules_settings["install_spark"]
        # Read Cluster's Instances File.
        instances_list = self.read_instances_file(cluster_name)
        # Parallel Store Instances' Public Keys on 'known_hosts' File.
        with ThreadPoolExecutor() as thread_pool_executor:
            for instance_dict in instances_list:
                instance_public_ipv4_address = instance_dict["public_ipv4_address"]
                thread_pool_executor.submit(self.store_instance_public_key_on_known_hosts,
                                            instance_public_ipv4_address)
        # Parallel Remotely Setup Spark on Instances (Masters and Workers).
        with ThreadPoolExecutor() as thread_pool_executor:
            for instance_dict in instances_list:
                instance_name = instance_dict["name"]
                if "master" in instance_name.lower():
                    if install_hadoop:
                        thread_pool_executor.submit(self.setup_hadoop_on_master_instance,
                                                    instance_dict)
                    if install_spark:
                        thread_pool_executor.submit(self.setup_spark_on_master_instance,
                                                    instance_dict)
                elif "worker" in instance_name.lower():
                    if install_hadoop:
                        thread_pool_executor.submit(self.setup_hadoop_on_worker_instance,
                                                    instance_dict)
                    if install_spark:
                        thread_pool_executor.submit(self.setup_spark_on_worker_instance,
                                                    instance_dict)

    def parallel_configure_clusters(self,
                                    cluster_names: list) -> None:
        with ThreadPoolExecutor() as thread_pool_executor:
            for cluster_name in cluster_names:
                # Get Logger.
                logger = self.get_attribute("logger")
                message = "Configuring the Cluster '{0}'...".format(cluster_name)
                log_message(logger, message, "INFO")
                future = thread_pool_executor.submit(self.configure_cluster_tasks,
                                                     cluster_name)
                wait([future])
                message = "The Cluster '{0}' was configured successfully!".format(cluster_name)
                log_message(logger, message, "INFO")


def configure_cluster(arguments_dict: dict) -> None:
    # Get Arguments.
    sparking_cloud_config_file = arguments_dict["sparking_cloud_config_file"]
    cluster_names = arguments_dict["cluster_names"]
    # Get Cluster Names List.
    cluster_names_list = cluster_names.split(",")
    # Init Config Parser Object.
    cp = ConfigParser()
    cp.optionxform = str
    cp.read(filenames=sparking_cloud_config_file, encoding="utf-8")
    # Init Cluster Configurator Object.
    cc = ClusterConfigurator(sparking_cloud_config_file)
    # Parse Sparking Cloud Config File and Set Attributes.
    sparking_cloud_settings_dict = parse_sparking_cloud_config_file(cp)
    for k, v in sparking_cloud_settings_dict.items():
        cc.set_attribute(k, v)
    # Check if Logging is Enabled.
    enable_logging = cc.get_attribute("general_settings")["enable_logging"]
    # Get Logging Settings.
    logging_settings = cc.get_attribute("logging_settings")
    # Instantiate and Set Logger.
    logger = load_logger(enable_logging, logging_settings)
    cc.set_attribute("logger", logger)
    # Parallel Configure Clusters.
    cc.parallel_configure_clusters(cluster_names_list)
    # Unbind Objects (Garbage Collector).
    del cp
    del cc
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
    # Configure Cluster.
    configure_cluster(args_dict)
    # Unbind Objects (Garbage Collector).
    del ag
    # End.
    exit(0)
