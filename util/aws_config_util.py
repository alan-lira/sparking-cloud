from pathlib import Path
from re import search


def parse_aws_config_file(aws_config_file_path: Path) -> tuple:
    aws_region = None
    aws_output = None
    with open(aws_config_file_path, mode="r") as aws_config_file:
        for line in iter(lambda: aws_config_file.readline(), ""):
            match_aws_region = search("region = (.*)$", line)
            match_aws_output = search("output = (.*)$", line)
            if match_aws_region:
                aws_region = match_aws_region.groups()[0]
            if match_aws_output:
                aws_output = match_aws_output.groups()[0]
    return aws_region, aws_output


def parse_aws_credentials_file(aws_credentials_file_path: Path) -> tuple:
    aws_access_key_id = None
    aws_secret_access_key = None
    with open(aws_credentials_file_path, mode="r") as aws_credentials_file:
        for line in iter(lambda: aws_credentials_file.readline(), ""):
            match_aws_access_key_id = search("aws_access_key_id = (.*)$", line)
            match_aws_secret_access_key = search("aws_secret_access_key = (.*)$", line)
            if match_aws_access_key_id:
                aws_access_key_id = match_aws_access_key_id.groups()[0]
            if match_aws_secret_access_key:
                aws_secret_access_key = match_aws_secret_access_key.groups()[0]
    return aws_access_key_id, aws_secret_access_key
