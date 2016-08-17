# -*- encoding = utf-8 -*-

import boto3
import time


timer = 60

session = boto3.Session(profile_name='hr-stat-ec2')

# Get the service resource
sqs = session.resource('sqs')

# Get the queue
queue = sqs.get_queue_by_name(QueueName='hr-stat')

def sendmessage():

    now = int(time.time())
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)

    queue.send_message(MessageBody=otherStyleTime, MessageAttributes={

        'sqsposter': {
            'StringValue': 'sqsposter',
            'DataType': 'String'
        }
        ,

        'Timer': {
            'StringValue': str(timer),
            'DataType': 'String'
        }

    })

def main():

    while True:
        sendmessage()
        time.sleep(timer)


if __name__ == '__main__':
    main()