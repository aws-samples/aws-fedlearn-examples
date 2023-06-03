import json
import boto3
import CONSTANTS

def sns_publish(sns, sns_topic_arn, message):
    # Publish SNS Messages
    msg_body = json.dumps(message)
    sns.publish(
        TopicArn=sns_topic_arn, 
        Message=json.dumps({'default': msg_body}),
        MessageStructure='json')
    print("SNS published message: " + str(message))

def lambda_handler(event, context):
    if not ('Error' in event['Input'].keys()):
        
        metricDictEnd = {
            "Task_Name": CONSTANTS.NOT_APPLICABLE_STRING, 
            "Task_ID": CONSTANTS.NOT_APPLICABLE_STRING,
            "roundId": CONSTANTS.NOT_APPLICABLE_STRING,
            "member_ID": CONSTANTS.NOT_APPLICABLE_STRING,
            "source": CONSTANTS.ALL_STRING
        }
        
        sns_topic_arn = event['sns_topic_server']     
        sns_client = boto3.client('sns') 
        message = json.dumps(metricDictEnd)
        
        sns_publish(sns_client, sns_topic_arn, message)
        