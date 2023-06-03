import CONSTANTS

def lambda_handler(event, context):
    print(event)

    index_round = int(event['Input']['taskresult']['roundId'])
    index_round = index_round + int(event['step_round'])
    
    continued = CONSTANTS.FALSE_STRING
    if index_round < int(event['count_round']):
        continued = CONSTANTS.TRUE_STRING
    
    return {
        'index_round': str(index_round),
        'step_round': event['step_round'],
        'taskresult': event['Input']["taskresult"],
        'continue': continued
    }

    