import boto3
import time

from apscheduler.schedulers.background import BackgroundScheduler
from instance_monitor import monitor_instances
from instance_factory import create_one_instance
from instance_factory import terminate_instances

ec2_resource = boto3.resource('ec2')
ec2_client = boto3.client('ec2')

ec2_instance_id_list = []

scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(lambda: monitor_instances(ec2_resource, ec2_instance_id_list), 'interval', seconds=5)
scheduler.start()

ec2_instance_created = create_one_instance(ec2_resource)
ec2_instance_id_list.append(ec2_instance_created.pop())

try:
   while True:
      time.sleep(1)
except (KeyboardInterrupt, SystemExit):
   scheduler.shutdown()
   terminate_instances(ec2_client, ec2_instance_id_list)
