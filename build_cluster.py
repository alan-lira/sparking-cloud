from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser
from pathlib import Path
from re import findall
from typing import Any
from cloud_manager.ec2_manager import EC2Manager
from util.aws_config_util import parse_aws_config_file, parse_aws_credentials_file
from util.config_parser_util import parse_config_section
from util.logging_util import load_logger, log_message
from util.sparking_cloud_util import parse_sparking_cloud_config_file


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

    @staticmethod
    def get_number_of_instances_appended_to_file(instances_file: Path) -> int:
        instances_list_parser = ConfigParser()
        instances_list_parser.optionxform = str
        instances_list_parser.read(filenames=instances_file,
                                   encoding="utf-8")
        number_of_instances_appended_to_file = 0
        for section in instances_list_parser.sections():
            if "Instance" in section:
                number_of_instances_appended_to_file += 1
        del instances_list_parser
        return number_of_instances_appended_to_file

    def append_instance_dict_to_file(self,
                                     cluster_name: str,
                                     instance_dict: dict) -> None:
        instances_file_root_folder = self.get_attribute("general_settings")["cluster_instances_root_folder"]
        instances_file = Path(instances_file_root_folder).joinpath(cluster_name)
        instances_file_parents_path = findall("(.*/)", str(instances_file))
        if instances_file_parents_path:
            Path(instances_file_parents_path[0]).mkdir(parents=True, exist_ok=True)
        instance_number = self.get_number_of_instances_appended_to_file(instances_file) + 1
        with open(file=instances_file, mode="a", encoding="utf-8") as instances_file:
            instances_file.write("[Instance {0}]\n".format(instance_number))
            instances_file.write("provider = {0}\n".format(instance_dict["provider"]))
            instances_file.write("name = {0}\n".format(instance_dict["name"]))
            instances_file.write("id = {0}\n".format(instance_dict["id"]))
            instances_file.write("key_name = {0}\n".format(instance_dict["key_name"]))
            instances_file.write("username = {0}\n".format(instance_dict["username"]))
            instances_file.write("public_ipv4_address = {0}\n".format(instance_dict["public_ipv4_address"]))
            instances_file.write("ssh_port = {0}\n".format(instance_dict["ssh_port"]))
            instances_file.write("\n")

    def create_spark_master_on_aws_tasks(self,
                                         cluster_name: str,
                                         master_id: int,
                                         master_instances_settings_dict: dict,
                                         ec2m: EC2Manager) -> None:
        master_prefix_name = master_instances_settings_dict["prefix_name"]
        master_name = cluster_name + "-" + master_prefix_name + "-" + str(master_id)
        master_instance_options = ec2m.load_ec2_instance_options(master_name,
                                                                 master_instances_settings_dict)
        try:
            master_instance_id = ec2m.create_one_ec2_instance(master_instance_options)
            ec2m.wait_for_ec2_instance_to_be_alive(master_instance_id)
            master_instance = ec2m.get_ec2_instance(master_instance_id)
            master_keyname = master_instances_settings_dict["key_name"]
            master_username = master_instances_settings_dict["username"]
            master_public_ipv4_address = ec2m.get_ec2_instance_public_ipv4_address(master_instance)
            master_ssh_port = master_instances_settings_dict["ssh_port"]
            master_instance_dict = {"provider": "AWS",
                                    "name": master_name,
                                    "id": master_instance_id,
                                    "key_name": master_keyname,
                                    "username": master_username,
                                    "public_ipv4_address": master_public_ipv4_address,
                                    "ssh_port": master_ssh_port}
            self.append_instance_dict_to_file(cluster_name, master_instance_dict)
        except:
            pass

    def parallel_create_spark_masters_on_aws(self,
                                             cluster_name: str,
                                             master_instances_settings_dict: dict,
                                             ec2m: EC2Manager) -> None:
        number_of_master_instances = master_instances_settings_dict["number_of_master_instances"]
        with ThreadPoolExecutor(max_workers=number_of_master_instances) as thread_pool_executor:
            for master_id in range(0, number_of_master_instances):
                thread_pool_executor.submit(self.create_spark_master_on_aws_tasks,
                                            cluster_name,
                                            master_id,
                                            master_instances_settings_dict,
                                            ec2m)

    def create_spark_worker_on_aws_tasks(self,
                                         cluster_name: str,
                                         worker_id: int,
                                         worker_instances_settings_dict: dict,
                                         ec2m: EC2Manager) -> None:
        worker_prefix_name = worker_instances_settings_dict["prefix_name"]
        worker_name = cluster_name + "-" + worker_prefix_name + "-" + str(worker_id)
        worker_instance_options = ec2m.load_ec2_instance_options(worker_name,
                                                                 worker_instances_settings_dict)
        try:
            worker_instance_id = ec2m.create_one_ec2_instance(worker_instance_options)
            ec2m.wait_for_ec2_instance_to_be_alive(worker_instance_id)
            worker_instance = ec2m.get_ec2_instance(worker_instance_id)
            worker_keyname = worker_instances_settings_dict["key_name"]
            worker_username = worker_instances_settings_dict["username"]
            worker_public_ipv4_address = ec2m.get_ec2_instance_public_ipv4_address(worker_instance)
            worker_ssh_port = worker_instances_settings_dict["ssh_port"]
            worker_instance_dict = {"provider": "AWS",
                                    "name": worker_name,
                                    "id": worker_instance_id,
                                    "key_name": worker_keyname,
                                    "username": worker_username,
                                    "public_ipv4_address": worker_public_ipv4_address,
                                    "ssh_port": worker_ssh_port}
            self.append_instance_dict_to_file(cluster_name, worker_instance_dict)
        except:
            pass

    def parallel_create_spark_workers_on_aws(self,
                                             cluster_name: str,
                                             worker_instances_settings_dict: dict,
                                             ec2m: EC2Manager) -> None:
        number_of_worker_instances = worker_instances_settings_dict["number_of_worker_instances"]
        with ThreadPoolExecutor(max_workers=number_of_worker_instances) as thread_pool_executor:
            for worker_id in range(0, number_of_worker_instances):
                thread_pool_executor.submit(self.create_spark_worker_on_aws_tasks,
                                            cluster_name,
                                            worker_id,
                                            worker_instances_settings_dict,
                                            ec2m)

    def build_cluster_tasks(self,
                            cluster_settings: dict,
                            config_parser: ConfigParser,
                            ec2m: EC2Manager) -> None:
        # Get Logger.
        logger = self.get_attribute("logger")
        cluster_name = cluster_settings["cluster_name"]
        message = "Building the Cluster '{0}'...".format(cluster_name)
        log_message(logger, message, "INFO")
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
                    thread_pool_executor.submit(self.parallel_create_spark_masters_on_aws,
                                                cluster_name,
                                                master_instances_settings_dict,
                                                ec2m)
            # Parallel Launch Worker Instances.
            for worker_instances_settings in worker_instances_settings_list:
                worker_instances_settings_dict = parse_config_section(config_parser,
                                                                      worker_instances_settings + " Settings")
                if "AWS" in worker_instances_settings:
                    thread_pool_executor.submit(self.parallel_create_spark_workers_on_aws,
                                                cluster_name,
                                                worker_instances_settings_dict,
                                                ec2m)
        message = "The Cluster '{0}' was build successfully!".format(cluster_name)
        log_message(logger, message, "INFO")

    def parallel_build_clusters(self,
                                config_parser: ConfigParser) -> None:
        # Get Clusters Settings.
        clusters_settings = self.get_attribute("clusters_settings")
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
                thread_pool_executor.submit(self.build_cluster_tasks,
                                            cluster_settings,
                                            config_parser,
                                            ec2m)
        # Unbind Objects (Garbage Collector).
        del ec2m


def main() -> None:
    # Begin.
    # Parse Cluster Builder Arguments.
    ag = ArgumentParser(description="Cluster Builder Arguments")
    ag.add_argument("--sparking_cloud_config_file",
                    type=Path,
                    required=False,
                    default=Path("config/sparking_cloud.cfg"),
                    help="Sparking Cloud Config File (default: config/sparking_cloud.cfg)")
    parsed_args = ag.parse_args()
    # Get Cluster Builder Arguments.
    sparking_cloud_config_file = Path(parsed_args.sparking_cloud_config_file)
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
    # End.
    exit(0)


if __name__ == "__main__":
    main()
