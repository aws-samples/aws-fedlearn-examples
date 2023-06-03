import CONSTANTS

def lambda_handler(event, context):
    # reorg input to the next FL round
    member_ID = 0 # server ID
    roundId = event['Input']['iterator']['index_round']

    metricDict = {
            "Task_Name": event['Input']['iterator']['taskresult']['Task_Name'],
            "Task_ID": str(member_ID).zfill(4) + roundId.zfill(8),  # and an id
            "roundId": roundId,
            "member_ID": str(member_ID),
            "numSamples": CONSTANTS.NOT_APPLICABLE_STRING,
            "numClientEpochs": event['Input']['iterator']['taskresult']['numClientEpochs'],
            "trainAcc": event['Input']['iterator']['taskresult']['trainAcc'],
            "testAcc" : event['Input']['iterator']['taskresult']['testAcc'],
            "trainLoss": event['Input']['iterator']['taskresult']['trainLoss'],
            "testLoss": event['Input']['iterator']['taskresult']['testLoss'],
            "weightsFile": event['Input']['iterator']['taskresult']['weightsFile'],
            "numClientsRequired": event['Input']['iterator']['taskresult']['numClientsRequired'],
            "source": CONSTANTS.SERVER_NAME,
    }
 
    return metricDict