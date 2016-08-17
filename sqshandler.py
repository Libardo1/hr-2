# -*- coding = utf8 -*-

import boto3
import sys
import time

timer = 300

session = boto3.Session(profile_name='hr-stat-ec2')

# Get the service resource
sqs = session.resource('sqs')

# Get the queue
queue = sqs.get_queue_by_name(QueueName='hr-stat')



def sendmessage(Body= ""):

    now = int(time.time())

    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)

    body = "Delete sqsposter "+Body + " @ " + otherStyleTime

    queue.send_message(MessageBody=body, MessageAttributes={

        'sqshandler': {
            'StringValue': 'sqshandler',
            'DataType': 'String'
        }
        ,

        'Timer': {
            'StringValue': str(timer),
            'DataType': 'String'
        }

    })


def queuehandler():
    # Process messages by printing out body and optional author name
    for message in queue.receive_messages(MessageAttributeNames=['sqsposter']):

        sendmessage(message.body)
        # Let the queue know that the message is processed
        message.delete()


def main():
    reload(sys)
    sys.setdefaultencoding('utf8')

    while True:
        queuehandler()
        time.sleep(timer)

if __name__ == '__main__':
    main()