from configparser import ConfigParser
from itertools import groupby
from operator import itemgetter
from pathlib import Path
from re import findall

from cloud_manager.ec2_manager import EC2Manager
from util.config_parser_util import parse_config_section


def parse_sparking_cloud_config_file(config_parser: ConfigParser) -> dict:
    sparking_cloud_settings_dict = dict()
    # Parse 'General Settings'.
    general_settings = parse_config_section(config_parser, "General Settings")
    sparking_cloud_settings_dict.update({"general_settings": general_settings})
    # If Logging is Enabled...
    if general_settings["enable_logging"]:
        # Parse 'Logging Settings'.
        logging_settings = parse_config_section(config_parser, "Logging Settings")
        sparking_cloud_settings_dict.update({"logging_settings": logging_settings})
    # Parse 'Clusters Settings'.
    clusters_settings = []
    for cluster_name in general_settings["cluster_names"]:
        cluster_dict = {"cluster_name": cluster_name}
        cluster_name_settings = parse_config_section(config_parser, cluster_name + " Settings")
        for setting in cluster_name_settings:
            cluster_dict.update({setting: cluster_name_settings[setting]})
        clusters_settings.append(cluster_dict)
    sparking_cloud_settings_dict.update({"clusters_settings": clusters_settings})
    # Parse 'AWS Settings'.
    aws_settings = parse_config_section(config_parser, "AWS Settings")
    sparking_cloud_settings_dict.update({"aws_settings": aws_settings})
    # Parse 'Configuration Rules Settings'.
    configuration_rules_settings = parse_config_section(config_parser,
                                                        general_settings["configuration_rules"] + " Settings")
    sparking_cloud_settings_dict.update({"configuration_rules_settings": configuration_rules_settings})
    return sparking_cloud_settings_dict


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


def append_instance_dict_to_file(instance_dict: dict,
                                 instances_file: Path) -> None:
    instances_file_parents_path = findall("(.*/)", str(instances_file))
    if instances_file_parents_path:
        Path(instances_file_parents_path[0]).mkdir(parents=True, exist_ok=True)
    instance_number = get_number_of_instances_appended_to_file(instances_file) + 1
    with open(file=instances_file, mode="a", encoding="utf-8") as instances_file:
        instances_file.write("[Instance {0}]\n".format(instance_number))
        instances_file.write("provider = {0}\n".format(instance_dict["provider"]))
        instances_file.write("name = {0}\n".format(instance_dict["name"]))
        instances_file.write("id = {0}\n".format(instance_dict["id"]))
        instances_file.write("type = {0}\n".format(instance_dict["type"]))
        instances_file.write("market_type = {0}\n".format(instance_dict["market_type"]))
        instances_file.write("key_name = {0}\n".format(instance_dict["key_name"]))
        instances_file.write("username = {0}\n".format(instance_dict["username"]))
        instances_file.write("public_ipv4_address = {0}\n".format(instance_dict["public_ipv4_address"]))
        instances_file.write("ssh_port = {0}\n".format(instance_dict["ssh_port"]))
        instances_file.write("\n")


def read_instances_file(instances_file: Path) -> list:
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
            instance_type = instances_list_parser.get(section, "type")
            instance_market_type = instances_list_parser.get(section, "market_type")
            instance_key_name = instances_list_parser.get(section, "key_name")
            instance_username = instances_list_parser.get(section, "username")
            instance_public_ipv4_address = instances_list_parser.get(section, "public_ipv4_address")
            instance_ssh_port = instances_list_parser.get(section, "ssh_port")
            instance_dict = {"provider": instance_provider,
                             "name": instance_name,
                             "id": instance_id,
                             "type": instance_type,
                             "market_type": instance_market_type,
                             "key_name": instance_key_name,
                             "username": instance_username,
                             "public_ipv4_address": instance_public_ipv4_address,
                             "ssh_port": instance_ssh_port}
            instances_list.append(instance_dict)
    del instances_list_parser
    return instances_list


def generate_cluster_instances_summary(instances_list: list,
                                       ec2m: EC2Manager) -> list:
    ec2_instances_summary = []
    for instance_dict in instances_list:
        if instance_dict["provider"] == "AWS":
            instance_id = instance_dict["id"]
            instance_state_name = ec2m.get_ec2_instance_state_name(instance_id)
            instance_dict.update({"state": instance_state_name})
            ec2_instances_summary.append(instance_dict)
    return ec2_instances_summary


def print_provider_instances_summary(cluster_instances_summary: list,
                                     instance_provider: str,
                                     instance_function: str,
                                     preemptive_instances_alias: str,
                                     regular_instances_alias: str) -> None:
    print("   [{0}]".format(instance_provider))
    instances = [instance_dict for instance_dict in cluster_instances_summary
                 if instance_dict["provider"] == instance_provider]
    instances = [instance for instance in instances if instance_function in instance["name"]]
    instances_sorted_by_type = sorted(instances, key=itemgetter("type"))
    instances_grouped_by_type = groupby(instances_sorted_by_type, key=itemgetter("type"))
    for key, value in instances_grouped_by_type:
        preemptive_count = 0
        preemptive_status_dict = {}
        regular_count = 0
        regular_status_dict = {}
        for instance_dict in value:
            if instance_dict["market_type"] == preemptive_instances_alias:
                preemptive_count = preemptive_count + 1
                if "state" in preemptive_status_dict:
                    preemptive_status_dict["state"].append(instance_dict["state"])
                else:
                    preemptive_status_dict["state"] = [instance_dict["state"]]
            elif instance_dict["market_type"] == regular_instances_alias:
                regular_count = regular_count + 1
                if "state" in regular_status_dict:
                    regular_status_dict["state"].append(instance_dict["state"])
                else:
                    regular_status_dict["state"] = [instance_dict["state"]]
        if preemptive_count > 0:
            preemptive_status_counter = [{key: len(list(value))} for (key, value)
                                         in groupby(sorted(preemptive_status_dict["state"]))]
            preemptive_status_counter_separated = \
                "| ".join("{0}: {1}".format(k, v) for k, v in preemptive_status_counter[0].items())
            message = "\t{0}x {1} in the {2} market ({3})" \
                .format(preemptive_count,
                        key,
                        preemptive_instances_alias,
                        preemptive_status_counter_separated)
            print(message)
        if regular_count > 0:
            regular_status_counter = [{key: len(list(value))} for (key, value)
                                      in groupby(sorted(regular_status_dict["state"]))]
            regular_status_counter_separated = \
                "| ".join("{0}: {1}".format(k, v) for k, v in regular_status_counter[0].items())
            message = "\t{0}x {1} in the {2} market ({3})" \
                .format(regular_count,
                        key,
                        regular_instances_alias,
                        regular_status_counter_separated)
            print(message)


def print_cluster_instances_summary(cluster_name: str,
                                    cluster_instances_summary: list) -> None:
    message = "------- CLUSTER SUMMARY ({0}) -------".format(cluster_name)
    print(message)
    # Get and Print the Master Instances Count (All Providers).
    all_master_instances_count = len([instance_dict for instance_dict in cluster_instances_summary
                                      if "master" in instance_dict["name"]])
    message = "A) {0} Master Instance".format(all_master_instances_count) \
        if all_master_instances_count == 1 \
        else "A) {0} Master Instances".format(all_master_instances_count)
    print(message)
    # Get and Print the AWS EC2 Master Instances Summary.
    instance_provider = "AWS"
    instance_function = "master"
    preemptive_instances_alias = "spot"
    regular_instances_alias = "on-demand"
    print_provider_instances_summary(cluster_instances_summary,
                                     instance_provider,
                                     instance_function,
                                     preemptive_instances_alias,
                                     regular_instances_alias)
    # Get and Print the Worker Instances Count (All Providers).
    all_worker_instances_count = len([instance_dict for instance_dict in cluster_instances_summary
                                      if "worker" in instance_dict["name"]])
    message = "B) {0} Worker Instance".format(all_worker_instances_count) \
        if all_worker_instances_count == 1 \
        else "B) {0} Worker Instances".format(all_worker_instances_count)
    print(message)
    # Get and Print the AWS EC2 Worker Instances Summary.
    instance_provider = "AWS"
    instance_function = "worker"
    preemptive_instances_alias = "spot"
    regular_instances_alias = "on-demand"
    print_provider_instances_summary(cluster_instances_summary,
                                     instance_provider,
                                     instance_function,
                                     preemptive_instances_alias,
                                     regular_instances_alias)
