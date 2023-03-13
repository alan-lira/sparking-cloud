from configparser import ConfigParser
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
