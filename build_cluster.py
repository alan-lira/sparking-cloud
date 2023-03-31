from argparse import ArgumentParser
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser
from pathlib import Path
from typing import Any
from cloud_manager.ec2_manager import EC2Manager
from util.aws_config_util import parse_aws_config_file, parse_aws_credentials_file
from util.config_parser_util import parse_config_section
from util.logging_util import load_logger, log_message
from util.os_util import check_if_file_exists, remove_file
from util.sparking_cloud_util import parse_sparking_cloud_config_file, append_instance_dict_to_file, \
    read_instances_file, generate_cluster_instances_summary, print_cluster_instances_summary
from terminate_cluster import terminate_cluster


class ClusterBuilder:

    def __init__(self,
                 sparking_cloud_config_file: Path) -> None:
        self.sparking_cloud_config_file = sparking_cloud_config_file
        # Sparking Cloud's Config File Settings.
        self.general_settings = None
        self.logging_settings = None
        self.aws_settings = None
        self.configuration_rules = None
        # Other Attributes.
        self.logger = None

    def set_attribute(self,
                      attribute_name: str,
                      attribute_value: Any) -> None:
        setattr(self, attribute_name, attribute_value)

    def get_attribute(self,
                      attribute_name: str) -> Any:
        return getattr(self, attribute_name)

    def build_cluster_from_scratch(self,
                                   cluster_name: str,
                                   cluster_settings: dict,
                                   config_parser: ConfigParser,
                                   ec2m: EC2Manager) -> None:
        # Get Logger.
        logger = self.get_attribute("logger")
        with ThreadPoolExecutor(max_workers=1) as thread_pool_executor:
            message = "Building the Cluster '{0}'...".format(cluster_name)
            log_message(logger, message, "INFO")
            future = thread_pool_executor.submit(self.build_cluster_tasks,
                                                 cluster_settings,
                                                 config_parser,
                                                 ec2m)
            exception = future.exception()
            if exception:
                exit(exception)
            message = "The Cluster '{0}' was build successfully!".format(cluster_name)
            log_message(logger, message, "INFO")

    def show_cluster_build_options_input(self,
                                         cluster_name: str,
                                         cluster_settings: dict,
                                         cluster_instances_file: Path,
                                         config_parser: ConfigParser,
                                         ec2m: EC2Manager) -> None:
        valid_responses = ["1", "2", "3", "4"]
        response = None
        while response not in valid_responses:
            input_message = \
                "-------" \
                "\n1) Keep the previously built cluster and wake up the stopped instances " \
                "(Old Instances Only)." \
                "\n2) Keep the previously built cluster, wake up the stopped instances, " \
                "and add the new ones described in the configuration file (Old + New Instances)." \
                "\n3) Terminate the previously built cluster and start from scratch (New Instances Only)." \
                "\n4) Do nothing." \
                "\n-------" \
                "\nWhich action do you want to proceed with? "
            response = input(input_message)
        if response == "1":
            print("This action is still under development...")
            pass
        elif response == "2":
            print("This action is still under development...")
            pass
        elif response == "3":
            # Generate Arguments Dict.
            arguments_dict = {"sparking_cloud_config_file": self.sparking_cloud_config_file,
                              "cluster_names": cluster_name}
            # Terminate the previously built cluster (old instances).
            terminate_cluster(arguments_dict)
            # Remove the instances file of the previously built cluster.
            remove_file(cluster_instances_file)
            # Build the Cluster from Scratch (New Instances).
            self.build_cluster_from_scratch(cluster_name,
                                            cluster_settings,
                                            config_parser,
                                            ec2m)
            # Read the Cluster's Instances File.
            instances_list = read_instances_file(cluster_instances_file)
            # Generate the Cluster Instances Summary (All Providers).
            cluster_instances_summary = generate_cluster_instances_summary(instances_list, ec2m)
            # Print the Recently Created Cluster Instances Summary.
            print_cluster_instances_summary(cluster_name,
                                            cluster_instances_summary)
        elif response == "4":
            pass

    def create_spark_master_on_aws_tasks(self,
                                         cluster_name: str,
                                         master_id: int,
                                         master_instances_settings_dict: dict,
                                         ec2m: EC2Manager) -> None:
        # Get Logger.
        logger = self.get_attribute("logger")
        # Get Clusters Instances Root Folder.
        cluster_instances_root_folder = self.get_attribute("general_settings")["cluster_instances_root_folder"]
        # Get Cluster Instances File.
        cluster_instances_file = Path(cluster_instances_root_folder).joinpath(cluster_name)
        master_prefix_name = master_instances_settings_dict["prefix_name"]
        master_name = cluster_name + "-" + master_prefix_name + "-" + str(master_id)
        master_instance_options = ec2m.load_ec2_instance_options(master_name,
                                                                 master_instances_settings_dict)
        master_instance_id = None
        master_instance = None
        try:
            master_instance_id = ec2m.create_one_ec2_instance(master_instance_options)
            ec2m.wait_for_ec2_instance_to_be_alive(master_instance_id)
            master_instance = ec2m.get_ec2_instance(master_instance_id)
        except ClientError as ce:
            message = ce.args[0]
            log_message(logger, message, "INFO")
            if "MaxSpotInstanceCountExceeded" in message:
                raise ce
        if master_instance:
            master_type = master_instances_settings_dict["type"]
            master_market_type = master_instances_settings_dict["market_type"]
            master_keyname = master_instances_settings_dict["key_name"]
            master_username = master_instances_settings_dict["username"]
            master_public_ipv4_address = ec2m.get_ec2_instance_public_ipv4_address(master_instance)
            master_ssh_port = master_instances_settings_dict["ssh_port"]
            master_instance_dict = {"provider": "AWS",
                                    "name": master_name,
                                    "id": master_instance_id,
                                    "type": master_type,
                                    "market_type": master_market_type,
                                    "key_name": master_keyname,
                                    "username": master_username,
                                    "public_ipv4_address": master_public_ipv4_address,
                                    "ssh_port": master_ssh_port}
            append_instance_dict_to_file(master_instance_dict, cluster_instances_file)

    def parallel_create_spark_masters_on_aws(self,
                                             cluster_name: str,
                                             master_instances_settings_dict: dict,
                                             ec2m: EC2Manager) -> None:
        number_of_master_instances = master_instances_settings_dict["number_of_master_instances"]
        with ThreadPoolExecutor(max_workers=number_of_master_instances) as thread_pool_executor:
            for master_id in range(0, number_of_master_instances):
                future = thread_pool_executor.submit(self.create_spark_master_on_aws_tasks,
                                                     cluster_name,
                                                     master_id,
                                                     master_instances_settings_dict,
                                                     ec2m)
                exception = future.exception()
                if exception:
                    raise exception

    def create_spark_worker_on_aws_tasks(self,
                                         cluster_name: str,
                                         worker_id: int,
                                         worker_instances_settings_dict: dict,
                                         ec2m: EC2Manager) -> None:
        # Get Logger.
        logger = self.get_attribute("logger")
        # Get Clusters Instances Root Folder.
        cluster_instances_root_folder = self.get_attribute("general_settings")["cluster_instances_root_folder"]
        # Get Cluster Instances File.
        cluster_instances_file = Path(cluster_instances_root_folder).joinpath(cluster_name)
        worker_prefix_name = worker_instances_settings_dict["prefix_name"]
        worker_name = cluster_name + "-" + worker_prefix_name + "-" + str(worker_id)
        worker_instance_options = ec2m.load_ec2_instance_options(worker_name,
                                                                 worker_instances_settings_dict)
        worker_instance_id = None
        worker_instance = None
        try:
            worker_instance_id = ec2m.create_one_ec2_instance(worker_instance_options)
            ec2m.wait_for_ec2_instance_to_be_alive(worker_instance_id)
            worker_instance = ec2m.get_ec2_instance(worker_instance_id)
        except ClientError as ce:
            message = ce.args[0]
            log_message(logger, message, "INFO")
            if "MaxSpotInstanceCountExceeded" in message:
                raise ce
        if worker_instance:
            worker_type = worker_instances_settings_dict["type"]
            worker_market_type = worker_instances_settings_dict["market_type"]
            worker_keyname = worker_instances_settings_dict["key_name"]
            worker_username = worker_instances_settings_dict["username"]
            worker_public_ipv4_address = ec2m.get_ec2_instance_public_ipv4_address(worker_instance)
            worker_ssh_port = worker_instances_settings_dict["ssh_port"]
            worker_instance_dict = {"provider": "AWS",
                                    "name": worker_name,
                                    "id": worker_instance_id,
                                    "type": worker_type,
                                    "market_type": worker_market_type,
                                    "key_name": worker_keyname,
                                    "username": worker_username,
                                    "public_ipv4_address": worker_public_ipv4_address,
                                    "ssh_port": worker_ssh_port}
            append_instance_dict_to_file(worker_instance_dict, cluster_instances_file)

    def parallel_create_spark_workers_on_aws(self,
                                             cluster_name: str,
                                             worker_instances_settings_dict: dict,
                                             ec2m: EC2Manager) -> None:
        number_of_worker_instances = worker_instances_settings_dict["number_of_worker_instances"]
        with ThreadPoolExecutor(max_workers=number_of_worker_instances) as thread_pool_executor:
            for worker_id in range(0, number_of_worker_instances):
                future = thread_pool_executor.submit(self.create_spark_worker_on_aws_tasks,
                                                     cluster_name,
                                                     worker_id,
                                                     worker_instances_settings_dict,
                                                     ec2m)
                exception = future.exception()
                if exception:
                    raise exception

    def build_cluster_tasks(self,
                            cluster_settings: dict,
                            config_parser: ConfigParser,
                            ec2m: EC2Manager) -> None:
        cluster_name = cluster_settings["cluster_name"]
        master_instances_settings_list = cluster_settings["master_instances_settings"]
        worker_instances_settings_list = cluster_settings["worker_instances_settings"]
        # Get Number of Instances.
        number_of_instances = len(master_instances_settings_list) + len(worker_instances_settings_list)
        with ThreadPoolExecutor(max_workers=number_of_instances) as thread_pool_executor:
            # Parallel Launch Master Instances.
            for master_instances_settings in master_instances_settings_list:
                master_instances_settings_dict = parse_config_section(config_parser,
                                                                      master_instances_settings + " Settings")
                if "AWS" in master_instances_settings:
                    future = thread_pool_executor.submit(self.parallel_create_spark_masters_on_aws,
                                                         cluster_name,
                                                         master_instances_settings_dict,
                                                         ec2m)
                    exception = future.exception()
                    if exception:
                        raise exception
            # Parallel Launch Worker Instances.
            for worker_instances_settings in worker_instances_settings_list:
                worker_instances_settings_dict = parse_config_section(config_parser,
                                                                      worker_instances_settings + " Settings")
                if "AWS" in worker_instances_settings:
                    future = thread_pool_executor.submit(self.parallel_create_spark_workers_on_aws,
                                                         cluster_name,
                                                         worker_instances_settings_dict,
                                                         ec2m)
                    exception = future.exception()
                    if exception:
                        raise exception

    def parallel_build_clusters_tasks(self,
                                      cluster_name: str,
                                      cluster_settings: dict,
                                      cluster_instances_root_folder: Path,
                                      cluster_instances_file: Path,
                                      config_parser: ConfigParser,
                                      ec2m: EC2Manager) -> None:
        is_existing_cluster_name = check_if_file_exists(cluster_instances_file)
        if is_existing_cluster_name:
            # Print the Existing Cluster Message.
            message = "The instances file of a previously built cluster named '{0}' " \
                      "was found in the '{1}' folder, as follows:" \
                .format(cluster_name,
                        cluster_instances_root_folder)
            print(message)
            # Read the Cluster's Instances File.
            instances_list = read_instances_file(cluster_instances_file)
            # Generate the Cluster Instances Summary (All Providers).
            cluster_instances_summary = generate_cluster_instances_summary(instances_list, ec2m)
            # Print the Existing Cluster Instances Summary.
            print_cluster_instances_summary(cluster_name,
                                            cluster_instances_summary)
            # Show the Cluster Build Options Input.
            self.show_cluster_build_options_input(cluster_name,
                                                  cluster_settings,
                                                  cluster_instances_file,
                                                  config_parser,
                                                  ec2m)
        else:
            # Build the Cluster from Scratch (New Instances).
            self.build_cluster_from_scratch(cluster_name,
                                            cluster_settings,
                                            config_parser,
                                            ec2m)
            # Read the Cluster's Instances File.
            instances_list = read_instances_file(cluster_instances_file)
            # Generate the Cluster Instances Summary (All Providers).
            cluster_instances_summary = generate_cluster_instances_summary(instances_list, ec2m)
            # Print the Recently Created Cluster Instances Summary.
            print_cluster_instances_summary(cluster_name,
                                            cluster_instances_summary)

    def parallel_build_clusters(self,
                                config_parser: ConfigParser) -> None:
        # Get Clusters Settings.
        clusters_settings = self.get_attribute("clusters_settings")
        # Get Clusters Instances Root Folder.
        cluster_instances_root_folder = self.get_attribute("general_settings")["cluster_instances_root_folder"]
        # Get Cloud Provider Names.
        cloud_provider_names_list = self.get_attribute("general_settings")["cloud_provider_names"]
        # Get Number of Clusters.
        number_of_clusters = len(clusters_settings)
        ec2m = None
        if "AWS" in cloud_provider_names_list:
            # Parse AWS Config and Credentials Files.
            aws_config_file_path = self.get_attribute("aws_settings")["config_file_path"]
            aws_region, aws_output = parse_aws_config_file(aws_config_file_path)
            aws_credentials_file_path = self.get_attribute("aws_settings")["credentials_file_path"]
            aws_access_key_id, aws_secret_access_key = parse_aws_credentials_file(aws_credentials_file_path)
            # Get AWS Service Setting (EC2).
            aws_service = self.get_attribute("aws_settings")["service"]
            # Init AWS EC2Manager Object.
            ec2m = EC2Manager(service_name=aws_service, region_name=aws_region)
        with ThreadPoolExecutor(max_workers=number_of_clusters) as thread_pool_executor:
            for cluster_settings in clusters_settings:
                cluster_name = cluster_settings["cluster_name"]
                cluster_instances_file = Path(cluster_instances_root_folder).joinpath(cluster_name)
                future = thread_pool_executor.submit(self.parallel_build_clusters_tasks,
                                                     cluster_name,
                                                     cluster_settings,
                                                     cluster_instances_root_folder,
                                                     cluster_instances_file,
                                                     config_parser,
                                                     ec2m)
                exception = future.exception()
                if exception:
                    exit(exception)
        # Unbind Objects (Garbage Collector).
        del ec2m


def build_cluster(arguments_dict: dict) -> None:
    # Get Arguments.
    sparking_cloud_config_file = arguments_dict["sparking_cloud_config_file"]
    # Init Config Parser Object.
    cp = ConfigParser()
    cp.optionxform = str
    cp.read(filenames=sparking_cloud_config_file, encoding="utf-8")
    # Init Cluster Builder Object.
    cb = ClusterBuilder(sparking_cloud_config_file)
    # Parse Sparking Cloud Config File and Set Attributes.
    sparking_cloud_settings_dict = parse_sparking_cloud_config_file(cp)
    for k, v in sparking_cloud_settings_dict.items():
        cb.set_attribute(k, v)
    # Check if Logging is Enabled.
    enable_logging = cb.get_attribute("general_settings")["enable_logging"]
    # Get Logging Settings.
    logging_settings = cb.get_attribute("logging_settings")
    # Instantiate and Set Logger.
    logger = load_logger(enable_logging, logging_settings)
    cb.set_attribute("logger", logger)
    # Parallel Build Clusters.
    cb.parallel_build_clusters(cp)
    # Unbind Objects (Garbage Collector).
    del cp
    del cb


if __name__ == "__main__":
    # Begin.
    # Parse Cluster Builder Arguments.
    ag = ArgumentParser(description="Cluster Builder Arguments")
    ag.add_argument("--sparking_cloud_config_file",
                    type=Path,
                    required=False,
                    default=Path("config/sparking_cloud.cfg"),
                    help="Sparking Cloud Config File (default: config/sparking_cloud.cfg)")
    parsed_args = ag.parse_args()
    # Generate Arguments Dict.
    args_dict = {"sparking_cloud_config_file": Path(parsed_args.sparking_cloud_config_file)}
    # Build Cluster.
    build_cluster(args_dict)
    # Unbind Objects (Garbage Collector).
    del ag
    # End.
    exit(0)
