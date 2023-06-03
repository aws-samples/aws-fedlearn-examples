import boto3
from time import gmtime, strftime
import CONSTANTS

def create_sns():
    sns_client = boto3.client('sns')
    return sns_client

def create_sns_topic(sns_client, sns_topic):
    topic_res = sns_client.create_topic(Name=sns_topic)
    sns_topic_arn = topic_res['TopicArn']
    return sns_topic_arn

def create_sqs_subscribe_sns_topic(sqs_region, sqs_name, sns, sns_topic_arn): 
    def allow_sns_to_write_to_sqs(topicarn, queuearn):
        policy_document = """{{
          "Version":"2012-10-17",
          "Statement":[
            {{
              "Sid":"MyPolicy",
              "Effect":"Allow",
              "Principal" : {{"AWS" : "*"}},
              "Action":"SQS:SendMessage",
              "Resource": "{}",
              "Condition":{{
                "ArnEquals":{{
                  "aws:SourceArn": "{}"
                }}
              }}
            }}
          ]
        }}""".format(queuearn, topicarn)
        return policy_document
    
    sqs_client = boto3.client('sqs', region_name=sqs_region)
    sqs_obj = boto3.resource('sqs', region_name=sqs_region)
    
    # Create/Get Queue
    sqs_client.create_queue(QueueName=sqs_name)
    sqs_queue = sqs_obj.get_queue_by_name(QueueName=sqs_name)
    queue_url = sqs_client.get_queue_url(QueueName=sqs_name)['QueueUrl']
    sqs_queue_attrs = sqs_client.get_queue_attributes(QueueUrl=queue_url,
                                                    AttributeNames=['All'])['Attributes']
    sqs_queue_arn = sqs_queue_attrs['QueueArn']
    if ':sqs.' in sqs_queue_arn:
        sqs_queue_arn = sqs_queue_arn.replace(':sqs.', ':')
        
    # Subscribe SQS queue to SNS
    sns.subscribe(
            TopicArn=sns_topic_arn,
            Protocol='sqs',
            Endpoint=sqs_queue_arn
    )
    
    policy_json = allow_sns_to_write_to_sqs(sns_topic_arn, sqs_queue_arn)
    response = sqs_client.set_queue_attributes(
        QueueUrl = queue_url,
        Attributes = {
            'Policy' : policy_json
        }
    )
    
    return sqs_obj
    
# delete all message in a sqs queue
def sqs_purge(sqs_region, sqs_name):
    sqs_obj = boto3.resource('sqs', region_name=sqs_region)
    sqs_client = boto3.client('sqs', region_name=sqs_region)
    sqs_queue = sqs_obj.get_queue_by_name(QueueName=sqs_name)
    queue_url = sqs_client.get_queue_url(QueueName=sqs_name)['QueueUrl']
    
    response = sqs_client.purge_queue(QueueUrl=queue_url)
    return response

def lambda_handler(event, context):
    # Create a SNS, a "global-model-ready" topic, and a sqs for each client 
    # create sns
    sns = create_sns()
    sns_topic_arn = event["sns_topic_server"]
    
    # for client 1
    sqs_region = event["sqs_region_1"]
    client_queue_name = event["client_queue_name_1"]
    client_sqs = create_sqs_subscribe_sns_topic(sqs_region, client_queue_name, sns, sns_topic_arn)
    # clean the sqs first
    sqs_purge(sqs_region, client_queue_name)
    # for client 2
    sqs_region = event["sqs_region_2"]
    client_queue_name = event["client_queue_name_2"]
    client_sqs = create_sqs_subscribe_sns_topic(sqs_region, client_queue_name, sns, sns_topic_arn)
    # clean the sqs first
    sqs_purge(sqs_region, client_queue_name)

    metricDictInit = {
            "Task_Name": "FL-Task-" + strftime("%Y-%m-%d-%H-%M-%S", gmtime()),  # Each FL task has a name 
            "Task_ID": event["member_ID"].zfill(4) + event["roundId"].zfill(8),  # and an id
            "roundId": event["roundId"],
            "member_ID": event["member_ID"],
            "numSamples": CONSTANTS.NOT_APPLICABLE_STRING,
            "numClientEpochs": event["numClientEpochs"],
            "trainAcc": CONSTANTS.NOT_APPLICABLE_STRING,
            "testAcc" : CONSTANTS.NOT_APPLICABLE_STRING,
            "trainLoss": CONSTANTS.NOT_APPLICABLE_STRING,
            "testLoss": CONSTANTS.NOT_APPLICABLE_STRING,
            "weightsFile": event["weightsFile"], 
            "numClientsRequired": event["numClientsRequired"], # required clients 
            "source": CONSTANTS.SERVER_NAME,
            }
            
    return metricDictInit