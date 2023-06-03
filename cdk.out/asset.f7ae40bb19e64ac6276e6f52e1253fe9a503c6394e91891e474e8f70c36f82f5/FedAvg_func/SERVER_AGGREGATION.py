import numpy as np
import json
import boto3
import os
from boto3.dynamodb.conditions import Key
import CONSTANTS

# read tasks from the task table
def readFromFLServerTaskTable(tasks_table_name, task_name):
        dynamodb = boto3.resource('dynamodb')
        task_table = dynamodb.Table(tasks_table_name) # environment varialble
        response = task_table.query(
            KeyConditionExpression=Key('Task_Name').eq(task_name)
        )
        print("read from tasks table = {}\n".format(response['Items']))
        return response['Items']

# find local models belonging to the given current round 
def receiveUpdatedModelsFromClients(transactions, task_name):

        # check if the local models from all required clients are received for the current round
        def hasReceivedFromClients(receivedNodes):
            required_num_clients = int(os.environ["REQUIRED_NUM_CLIENTS"])
            clientsReceivedSet = set()
            for node in receivedNodes.keys():
                   clientsReceivedSet.add(int(node))
            print('clientsReceived = {}'.format(clientsReceivedSet))
            return required_num_clients == len(clientsReceivedSet)
        
        # first needs to find the highest roundId among all tasks, which is the current roundId
        # since server's task info are also included, the roundId should be the latest
        roundId = -1
        for transaction in transactions:    
            if int(transaction["roundId"]) > roundId:
                roundId = int(transaction["roundId"])
        
        print("current roundId = " + str(roundId))
        # parse transactions received from the given task and round
        nodes = dict()
        tokens = []
        for transaction in transactions:
            if transaction["source"] != CONSTANTS.SERVER_NAME and roundId == int(transaction["roundId"]) and task_name == transaction['Task_Name']:
                metrics= {
                            "Task_Name": transaction['Task_Name'], 
                            "Task_ID": transaction['Task_ID'], 
                            "roundId":  transaction["roundId"],
                            "member_ID": transaction["member_ID"],
                            "numSamples": transaction["numSamples"],
                            "numClientEpochs": transaction["numClientEpochs"],
                            "trainAcc": transaction["trainAcc"],
                            "testAcc" : transaction["testAcc"],
                            "trainLoss": transaction["trainLoss"],
                            "testLoss": transaction["testLoss"],
                            "weightsFile": transaction["weightsFile"],
                            "numClientsRequired": transaction["numClientsRequired"],
                            "source": transaction["source"],
                }

                nodes[transaction["member_ID"]] = metrics
                tokens.append(transaction["TaskToken"])

        # check if required clients are satisfied
        if hasReceivedFromClients(nodes):      
            return nodes, roundId, tokens
        else:
            return None, None, None

# server aggregate algorithm: fedavg
def fedAvg(receivedNodes, roundId):
        # avg of matrix*weights
        def weightedMeanSequence(matrixSeq, weights):
            assert len(matrixSeq) == len(weights)
            total_weight = 0.0
            base = [0]*matrixSeq[0] #initialize
            for w in range(len(matrixSeq)):  # w is the number of local samples
                total_weight += weights[w]
                base = base + matrixSeq[w]*weights[w] 
            weighted_matrix = [v / total_weight for v in base]
            return weighted_matrix
           
        model_params_w = []
        numSamples = []
        testAcc = []
        trainAcc = []
        testLoss = []
        trainLoss = []

        # collect all weight metrics from clients that received local models from 
        # can be improved to save memory for large models or a large number of clients
        for key in receivedNodes:
            update = receivedNodes[key]        
            if update != None:
                # retrieve weights file from s3
                s3 = boto3.resource('s3')  
                server_s3_address = os.environ['SERVER_S3_ADDRESS'] #"flserver0databucket"  # make it an environment variable for lambda
                key = update["weightsFile"] # the file name at S3
                lambda_temp_store = '/tmp/' + key # the defined /tmp/ path in lambda to store files
                s3.Bucket(server_s3_address).download_file(key, lambda_temp_store)
                
                model_params_w0 = np.load(lambda_temp_store, allow_pickle=True)
                model_params_w.append(model_params_w0)
                numSamples.append(np.array(int(update["numSamples"])))

                testAcc.append(np.array(float(update["testAcc"])))  
                trainAcc.append(np.array(float(update["trainAcc"])))
                testLoss.append(np.array(float(update["testLoss"])))  
                trainLoss.append(np.array(float(update["trainLoss"])))

        print(model_params_w)
        print(numSamples)

        avg_model_params_w = weightedMeanSequence(model_params_w, numSamples)
        avg_TestAcc = weightedMeanSequence(testAcc, numSamples)
        avg_TrainAcc = weightedMeanSequence(trainAcc, numSamples)
        avg_TestLoss = weightedMeanSequence(testLoss, numSamples)
        avg_TrainLoss = weightedMeanSequence(trainLoss, numSamples)

        print(avg_model_params_w)
        
        # save model weights to sever's S3
        savedModelFileName = 'train_weight_round_{}.npy'.format(roundId)
        lambda_temp_store = '/tmp/' + savedModelFileName # the defined /tmp/ path in lambda to store files
        np.save(lambda_temp_store, avg_model_params_w) # notice the order of the parameters    
        # upload local model to the FL server S3
        s3 = boto3.resource('s3')           
        server_s3_address = os.environ['SERVER_S3_ADDRESS'] 
        s3.Bucket(server_s3_address).upload_file(lambda_temp_store, savedModelFileName)
        return avg_TrainAcc[0], avg_TestAcc[0], avg_TrainLoss[0], avg_TestLoss[0], savedModelFileName
    
def lambda_handler(event, context):
    # tasks_table_name, task_name
    task_name = event['Records'][0]['dynamodb']['Keys']['Task_Name']['S'] 
    task_id = event['Records'][0]['dynamodb']['Keys']['Task_ID']['S'] 
    
    # read transactions from DynamoDB
    transactions = readFromFLServerTaskTable(os.environ['TASKS_TABLE_NAME'], task_name)

    # receive local models from required clients
    receivedNodes, roundId, tokens = receiveUpdatedModelsFromClients(transactions, task_name)

    print(receivedNodes)

    output = None
    if (receivedNodes != None):
        # aggregation updates
        avg_TrainAcc, avg_TestAcc, avg_TrainLoss, avg_TestLoss, savedModelFileName = fedAvg(receivedNodes, roundId)

        numClientsRequired = CONSTANTS.NOT_APPLICABLE_STRING
        numClientEpochs = CONSTANTS.NOT_APPLICABLE_STRING

        for member in receivedNodes.values():
            if numClientEpochs  == CONSTANTS.NOT_APPLICABLE_STRING:
                numClientEpochs = member['numClientEpochs']
            else: 
                assert numClientEpochs == member['numClientEpochs']

            if numClientsRequired  == CONSTANTS.NOT_APPLICABLE_STRING:
                numClientsRequired = member['numClientsRequired']
            else:
                assert numClientsRequired == member['numClientsRequired']

        output = {'Task_Name': task_name, 
                'Task_ID': task_id, 
                'numClientsRequired': numClientsRequired, 
                'roundId': str(roundId), 
                'numClientEpochs': numClientEpochs, 
                'trainAcc': str(avg_TrainAcc), 
                'testAcc': str(avg_TestAcc), 
                'trainLoss': str(avg_TrainLoss), 
                'testLoss': str(avg_TestLoss), 
                'weightsFile': str(savedModelFileName),
                }

        step_client = boto3.client('stepfunctions')
        out_str = json.dumps(output)

        # assert all tokens should be same
        token = None 
        for atoken in tokens:
            if token  == None:
                token = atoken
            else:
                assert token == atoken
        
        step_client.send_task_success(
                taskToken=token,
                output=out_str
        )

        return out_str, token
