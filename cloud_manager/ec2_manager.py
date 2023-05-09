from botocore import exceptions
from boto3 import client, resource
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from socket import AF_INET, SOCK_STREAM, socket
from time import sleep
from typing import Any


class EC2Manager:

    def __init__(self,
                 service_name: str,
                 region_name: str) -> None:
        self.ec2_client = client(region_name=region_name, service_name=service_name)
        self.ec2_resource = resource(region_name=region_name, service_name=service_name)

    def fetch_current_ec2_spot_instance_price(self,
                                              availability_zone: str,
                                              instance_types: list,
                                              product_descriptions: list) -> float:
        current_ec2_spot_instance_price = 0
        query_result_json = self.ec2_client.describe_spot_price_history(AvailabilityZone=availability_zone,
                                                                        InstanceTypes=instance_types,
                                                                        ProductDescriptions=product_descriptions,
                                                                        StartTime=datetime.now())
        spot_price_history_json = query_result_json.get("SpotPriceHistory")
        if len(spot_price_history_json) > 0:
            current_ec2_spot_instance_price = spot_price_history_json[0].get("SpotPrice")
        return current_ec2_spot_instance_price

    def load_ec2_instance_options(self,
                                  instance_name: str,
                                  instances_settings_dict: dict) -> dict:
        instance_operating_system = instances_settings_dict["operating_system"]
        instance_type = instances_settings_dict["type"]
        instance_security_group_ids = instances_settings_dict["security_group_ids"]

        tag_specifications = [{"ResourceType": "instance",
                               "Tags": [{"Key": "Name", "Value": instance_name}]}]
        instance_market_type = instances_settings_dict["market_type"]
        instance_spot_max_price = instances_settings_dict["spot_max_price"]
        instance_availability_zone = instances_settings_dict["placement"]
        instance_market_options = {}
        if instance_market_type == "spot":
            if instance_spot_max_price == "Current_EC2_Spot_Instance_Price":
                instance_spot_max_price = \
                    self.fetch_current_ec2_spot_instance_price(availability_zone=instance_availability_zone,
                                                               instance_types=[instance_type],
                                                               product_descriptions=[instance_operating_system])
            instance_spot_type = instances_settings_dict["spot_type"]
            instance_spot_interruption_behavior = instances_settings_dict["spot_interruption_behavior"]
            instance_spot_options = {"MaxPrice": instance_spot_max_price,
                                     "SpotInstanceType": instance_spot_type,
                                     "InstanceInterruptionBehavior": instance_spot_interruption_behavior}
            instance_market_options = {"MarketType": instance_market_type,
                                       "SpotOptions": instance_spot_options}
        instance_placement = {"AvailabilityZone": instance_availability_zone}
        instance_subnet_id = instances_settings_dict["subnet_id"]
        aws_instance_options = {"ImageId": instances_settings_dict["ami_id"],
                                "InstanceType": instance_type,
                                "KeyName": instances_settings_dict["key_name"],
                                "SecurityGroupIds": instance_security_group_ids,
                                "TagSpecifications": tag_specifications,
                                "InstanceMarketOptions": instance_market_options,
                                "Placement": instance_placement,
                                "SubnetId": instance_subnet_id}
        return aws_instance_options

    def create_one_ec2_instance(self,
                                instance_options: dict) -> str:
        instance_request = \
            self.ec2_resource.create_instances(ImageId=instance_options["ImageId"],
                                               InstanceType=instance_options["InstanceType"],
                                               KeyName=instance_options["KeyName"],
                                               SecurityGroupIds=instance_options["SecurityGroupIds"],
                                               TagSpecifications=instance_options["TagSpecifications"],
                                               InstanceMarketOptions=instance_options["InstanceMarketOptions"],
                                               MinCount=1,
                                               MaxCount=1,
                                               Placement=instance_options["Placement"],
                                               SubnetId=instance_options["SubnetId"])
        instance_id = [instance.id for instance in instance_request][0]
        return instance_id

    def get_ec2_instance(self,
                         instance_id: str) -> any:
        return self.ec2_resource.Instance(instance_id)

    def _wait_for_ec2_instance_to_start_running(self,
                                                instance_id: str) -> any:
        instance = self.get_ec2_instance(instance_id)
        instance.wait_until_running()
        return instance

    @staticmethod
    def get_ec2_instance_public_ipv4_address(instance: any) -> str:
        return instance.public_ip_address

    @staticmethod
    def _wait_for_ec2_instance_ssh_port_availability(instance_public_ip_address: str) -> None:
        while True:
            sock = socket(family=AF_INET, type=SOCK_STREAM)
            result = sock.connect_ex((instance_public_ip_address, 22))
            if result == 0:
                break
            sleep(1)

    def wait_for_ec2_instance_to_be_alive(self,
                                          instance_id: str) -> None:
        while True:
            instance = self._wait_for_ec2_instance_to_start_running(instance_id)
            instance_public_ip_address = self.get_ec2_instance_public_ipv4_address(instance)
            if instance_public_ip_address:
                break
            sleep(1)
        self._wait_for_ec2_instance_ssh_port_availability(instance_public_ip_address)

    def wait_for_ec2_instances_to_be_alive(self,
                                           instances_list: list) -> None:
        with ThreadPoolExecutor() as thread_pool_executor:
            for instance_dict in instances_list:
                thread_pool_executor.submit(self.wait_for_ec2_instance_to_be_alive, str(instance_dict["id"]))

    def get_ec2_instance_from_id(self,
                                 instance_id: str) -> Any:
        return self.ec2_resource.Instance(instance_id)

    @staticmethod
    def is_ec2_instance_running(instance: Any) -> bool:
        ec2_instance_running = False
        instance_state_name = None
        try:
            instance_state_name = instance.state["Name"]
        except AttributeError:
            # The instance entry was deleted by AWS, as it has been terminated for a while already.
            pass
        if instance_state_name == "running":
            ec2_instance_running = True
        return ec2_instance_running

    def get_active_ec2_instances_list(self,
                                      instances_id_list: list) -> list:
        active_ec2_instances_list = []
        for instance_id in instances_id_list:
            try:
                instance = self.get_ec2_instance_from_id(instance_id)
                if self.is_ec2_instance_running(instance):
                    active_ec2_instances_list.append(instance)
            except Exception as ex:
                exception_type = type(ex).__name__
                if exception_type in ["ClientError", "AttributeError"]:
                    # The instance entry was deleted by AWS, as it has been terminated for a while already.
                    pass
        return active_ec2_instances_list

    def get_ec2_instance_state_name(self,
                                    instance_id: str) -> str:
        instance_state_name = None
        try:
            instance = self.get_ec2_instance_from_id(instance_id)
            instance_state_name = instance.state["Name"]
        except Exception as ex:
            exception_type = type(ex).__name__
            if exception_type in ["ClientError", "AttributeError"]:
                # The instance entry was deleted by AWS, as it has been terminated for a while already.
                instance_state_name = "deleted_entry"
        return instance_state_name

    def terminate_ec2_instances_list(self,
                                     instances_ids_list: list) -> None:
        if instances_ids_list:
            try:
                self.ec2_client.terminate_instances(InstanceIds=instances_ids_list)
                while True:
                    active_instances_list = self.get_active_ec2_instances_list(instances_ids_list)
                    if not active_instances_list:
                        break
                    sleep(1)
            except exceptions.ClientError as client_error:
                error_code = client_error.response["Error"]["Code"]
                if error_code == "InvalidInstanceID.NotFound":
                    pass
