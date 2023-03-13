from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser
from pathlib import Path
from typing import Any
from cloud_manager.ec2_manager import EC2Manager
from util.aws_config_util import parse_aws_config_file
from util.logging_util import load_logger, log_message
from util.sparking_cloud_util import parse_sparking_cloud_config_file


class ClusterTerminator:

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

    def terminate_ec2_instances(self,
                                cluster_name: str,
                                instances_list: list,
                                ec2m: EC2Manager) -> None:
        ec2_instances_ids_list = []
        for instance_dict in instances_list:
            if instance_dict["provider"] == "AWS":
                ec2_instances_ids_list.append(instance_dict["id"])
        active_ec2_instances_list = ec2m.get_active_ec2_instances_list(ec2_instances_ids_list)
        number_of_active_ec2_instances = len(active_ec2_instances_list)
        active_ec2_instances_ids_list = [ec2_instance.id for ec2_instance in active_ec2_instances_list]
        if active_ec2_instances_ids_list:
            ec2m.terminate_ec2_instances_list(active_ec2_instances_ids_list)
            logger = self.get_attribute("logger")
            if number_of_active_ec2_instances == 1:
                message = "{0} EC2 Instance of '{1}' was terminated." \
                    .format(number_of_active_ec2_instances, cluster_name)
            else:
                message = "{0} EC2 Instances of '{1}' were terminated." \
                    .format(number_of_active_ec2_instances, cluster_name)
            log_message(logger, message, "INFO")

    def terminate_cluster_tasks(self,
                                cluster_name: str,
                                ec2m: EC2Manager) -> None:
        # Get Logger.
        logger = self.get_attribute("logger")
        # Read Cluster's Instances File.
        instances_list = self.read_instances_file(cluster_name)
        message = "Terminating the Cluster '{0}'...".format(cluster_name)
        log_message(logger, message, "INFO")
        # Terminate EC2 Instances (If Any Belongs to the Cluster).
        if ec2m:
            self.terminate_ec2_instances(cluster_name, instances_list, ec2m)
        message = "The Cluster '{0}' was terminated successfully!".format(cluster_name)
        log_message(logger, message, "INFO")

    def parallel_terminate_clusters(self,
                                    cluster_names: list) -> None:
        # Get Cloud Provider Names.
        cloud_provider_names_list = self.get_attribute("general_settings")["cloud_provider_names"]
        # Get Number of Clusters.
        number_of_clusters = len(cluster_names)
        ec2m = None
        # Load EC2 Manager (If Any EC2 Instance Belongs to the Cluster).
        if "AWS" in cloud_provider_names_list:
            # Parse AWS Config File.
            aws_config_file_path = self.get_attribute("aws_settings")["config_file_path"]
            aws_region, aws_output = parse_aws_config_file(aws_config_file_path)
            # Get AWS Service Setting (EC2).
            aws_service = self.get_attribute("aws_settings")["service"]
            # Init AWS EC2Manager Object.
            ec2m = EC2Manager(service_name=aws_service, region_name=aws_region)
        with ThreadPoolExecutor(max_workers=number_of_clusters) as thread_pool_executor:
            for cluster_name in cluster_names:
                thread_pool_executor.submit(self.terminate_cluster_tasks,
                                            cluster_name,
                                            ec2m)
        # Unbind Objects (Garbage Collector).
        del ec2m


def main() -> None:
    # Begin.
    # Parse Cluster Terminator Arguments.
    ag = ArgumentParser(description="Cluster Terminator Arguments")
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
    # Get Cluster Terminator Arguments.
    sparking_cloud_config_file = Path(parsed_args.sparking_cloud_config_file)
    cluster_names = str(parsed_args.cluster_names)
    cluster_names_list = cluster_names.split(",")
    # Init Config Parser Object.
    cp = ConfigParser()
    cp.optionxform = str
    cp.read(filenames=sparking_cloud_config_file, encoding="utf-8")
    # Init Cluster Terminator Object.
    ct = ClusterTerminator(sparking_cloud_config_file)
    # Parse Sparking Cloud Config File and Set Attributes.
    sparking_cloud_settings_dict = parse_sparking_cloud_config_file(cp)
    for k, v in sparking_cloud_settings_dict.items():
        ct.set_attribute(k, v)
    # Check if Logging is Enabled.
    enable_logging = ct.get_attribute("general_settings")["enable_logging"]
    # Get Logging Settings.
    logging_settings = ct.get_attribute("logging_settings")
    # Instantiate and Set Logger.
    logger = load_logger(enable_logging, logging_settings)
    ct.set_attribute("logger", logger)
    # Parallel Terminate Clusters.
    ct.parallel_terminate_clusters(cluster_names_list)
    # Unbind Objects (Garbage Collector).
    del cp
    del ct
    del logger
    # End.
    exit(0)


if __name__ == "__main__":
    main()
