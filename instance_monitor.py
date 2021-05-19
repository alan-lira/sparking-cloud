def monitor_instances(ec2_resource, ec2_instance_id_list):
   for ec2_instance_id in ec2_instance_id_list:
      try:
         instance = ec2_resource.Instance(ec2_instance_id)
         print('Instance ' + instance.id + ' --> ' + instance.state['Name'])
      except:
         print('Terminated instance ' + instance.id + ' entry deleted by AWS')
