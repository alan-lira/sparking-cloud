def create_one_instance(ec2_resource):
   instance_request = ec2_resource.create_instances(
ImageId='ami-09e67e426f25ce0d7',
InstanceType='t2.micro',
KeyName='alan-key',
SecurityGroupIds=[
   'sg-0337a115d0b4f1bb5',
],
TagSpecifications=[
   {
      'ResourceType': 'instance',
      'Tags': [
         {'Key': 'Name', 'Value': 'instance_name_here',},
      ],
   },
],
InstanceMarketOptions={
   'MarketType': 'spot',
   'SpotOptions': {
      'MaxPrice': '0.004',
      'SpotInstanceType': 'one-time',
      'InstanceInterruptionBehavior': 'terminate'
   }
},
MinCount=1,
MaxCount=1)
   instance_created = [ec2_instance.id for ec2_instance in instance_request]
   return instance_created


def terminate_instances(ec2_client, ec2_instance_id_list):
   ec2_client.terminate_instances(InstanceIds=ec2_instance_id_list)
