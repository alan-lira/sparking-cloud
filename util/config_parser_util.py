from configparser import ConfigParser
from re import findall


def parse_config_section(config_parser: ConfigParser,
                         section_name: str) -> dict:
    parsed_section = {key: value for key, value in config_parser[section_name].items()}
    for key, value in parsed_section.items():
        if value == "None":
            parsed_section[key] = None
        elif value in ["True", "Yes"]:
            parsed_section[key] = True
        elif value in ["False", "No"]:
            parsed_section[key] = False
        elif value.isdigit():
            parsed_section[key] = int(value)
        elif value.replace(".", "", 1).isdigit():
            parsed_section[key] = float(value)
        elif not findall(r"%\(.*?\)s+", value) and findall(r"\[.*?]+", value):
            aux_list = value.replace("[", "").replace("]", "").replace(" ", "").split(",")
            for index, item in enumerate(aux_list):
                if item.isdigit():
                    aux_list[index] = int(item)
                elif item.replace(".", "", 1).isdigit():
                    aux_list[index] = float(item)
            parsed_section[key] = aux_list
        elif not findall(r"%\(.*?\)s+", value) and findall(r"\(.*?\)+", value):
            aux_list = value.replace("(", "").replace(")", "").replace(" ", "").split(",")
            for index, item in enumerate(aux_list):
                if item.isdigit():
                    aux_list[index] = int(item)
                elif item.replace(".", "", 1).isdigit():
                    aux_list[index] = float(item)
            parsed_section[key] = tuple(aux_list)
        elif not findall(r"%\(.*?\)s+", value) and findall(r"\{.*?}+", value):
            aux_dict = {}
            aux_list = value.replace("{", "").replace("}", "").replace(" ", "").split(",")
            for item in aux_list:
                pair_item = item.split(":")
                pair_key = pair_item[0]
                pair_value = pair_item[1]
                if pair_value == "None":
                    pair_value = None
                elif pair_value in ["True", "Yes"]:
                    pair_value = True
                elif pair_value in ["False", "No"]:
                    pair_value = False
                elif pair_value.isdigit():
                    pair_value = int(value)
                elif pair_value.replace(".", "", 1).isdigit():
                    pair_value = float(value)
                aux_dict.update({pair_key: pair_value})
            parsed_section[key] = aux_dict
    return parsed_section
