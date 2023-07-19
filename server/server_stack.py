from constructs import Construct
import json
import os
from aws_cdk import (
    Duration,
    Stack,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as _aws_stepfunctions_tasks,
    aws_sns as sns, 
    aws_dynamodb as dynamodb,
    RemovalPolicy,
    aws_s3 as s3,
)
from aws_cdk.aws_sns import CfnTopic
from aws_cdk.aws_lambda_event_sources import DynamoEventSource
from cdk_nag import NagSuppressions 
import aws_cdk.aws_logs as logs

class ServerStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # Server info from server_config.json
        # Load configuration file
        directory = os.getcwd()
        config_server = os.path.join(directory, "server", "server_config.json")
        server_config = None
        with open(config_server, 'r') as config_file:
            server_config = json.load(config_file)
        s3_models = server_config["s3_fl_model_registry"]
        dynamodb_model_info = server_config["dynamodb_table_model_info"]
        num_clients_required = server_config["num_clients_required"]
        timeout_seconds_per_fl_round = server_config["timeout_seconds_per_fl_round"]
        lambda_timeout = 900 # the default lambda timeout is 3 seconds

        server_member_ID = server_config["server_member_ID"]
        starting_round_id = server_config["starting_round_id"]
        num_clients_required = server_config["num_clients_required"]
        num_client_epochs = server_config["num_client_epochs"]
        model_file_name = server_config["model_file_name"]
        aggregation_alg = server_config["aggregation_alg"]
        fl_rounds = server_config["fl_rounds"]

        clients = server_config["clients"]

        # The local scope 'this' is the Stack. 
        NagSuppressions.add_stack_suppressions(self, [
        {
            "id": 'AwsSolutions-IAM5',
            "reason": 'A stack level suppression.'
        },
        ])

        # Create a S3 read and write only policy 
        s3_read_write_policy = iam.Policy(
                            self, "s3_read_write_policy",
                            statements=[
                                iam.PolicyStatement(
                                    actions=["s3:GetObject",
                                             "s3:PutObject",
                                    ],
                                    resources=["*"]
                            )]
        )
        # Remediate AwsSolutions-IAM5 
        NagSuppressions.add_resource_suppressions(s3_read_write_policy, [
            {
                "id": 'AwsSolutions-IAM5',
                "reason": 'Apply to multiple services.'
            },
        ]) 
                
        # Create a DynamoDB write only policy  
        dynamoDB_write_policy = iam.Policy(
                            self, "dynamoDB_write_policy",
                            statements=[
                                iam.PolicyStatement(
                                    actions=[ "dynamodb:PutItem"],
                                    resources=["*"]
                            )]
        )
        # Remediate AwsSolutions-IAM5 
        NagSuppressions.add_resource_suppressions(dynamoDB_write_policy, [
            {
                "id": 'AwsSolutions-IAM5',
                "reason": 'Apply to multiple services.'
            },
        ]) 
        
        # Create a SQS read and delete policy
        sqs_read_policy = iam.Policy( 
                            self, "sqs_read_policy",
                            statements=[
                                iam.PolicyStatement(
                                    actions=["sqs:ReceiveMessage",
                                            "sqs:DeleteMessage",
                                            "sqs:GetQueueAttributes",                    
                                            "sqs:GetQueueUrl",
                                            ],
                                    resources=["*"]
                            )]
        )
        # Remediate AwsSolutions-IAM5 
        NagSuppressions.add_resource_suppressions(sqs_read_policy, [
            {
                "id": 'AwsSolutions-IAM5',
                "reason": 'Apply to multiple services.'
            },
        ]) 
        
        # Configure cross account client role to access 
        for client in clients:
            if "across_account_id" in client.keys():
                S3_across_account_role_name = "cross-account-" + client['across_account_id'] + "-s3-role"
                db_across_account_role_name = "cross-account-" + client['across_account_id'] + "-dynamodb-role"
                sqs_across_account_role_name = "cross-account-" + client['across_account_id'] + "-sqs-role"

                cross_account_s3_role = iam.Role(self, S3_across_account_role_name, 
                                            assumed_by=iam.AccountPrincipal(client['across_account_id']),
                                            role_name=S3_across_account_role_name,
                                        )
                cross_account_s3_role.apply_removal_policy(RemovalPolicy.DESTROY)   
                cross_account_s3_role.attach_inline_policy(s3_read_write_policy)                                          

                cross_account_dynamodb_role = iam.Role(self, db_across_account_role_name, 
                                            assumed_by=iam.AccountPrincipal(client['across_account_id']),
                                            role_name=db_across_account_role_name,
                                        )
                cross_account_dynamodb_role.apply_removal_policy(RemovalPolicy.DESTROY)  
                cross_account_dynamodb_role.attach_inline_policy(dynamoDB_write_policy)                                        

                cross_account_sqs_role = iam.Role(self, sqs_across_account_role_name, 
                                            assumed_by=iam.AccountPrincipal(client['across_account_id']),
                                            role_name=sqs_across_account_role_name,
                                        )
                cross_account_sqs_role.apply_removal_policy(RemovalPolicy.DESTROY) 
                cross_account_sqs_role.attach_inline_policy(sqs_read_policy)

        # Create a lambda function to be triggered by DynamoDB stream
        lambda_multiple_role = iam.Role(self, "lambda_multiple_role",
          assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")) 
        # # Create a S3 read/write, Lambda execute, and Step Functions SendTaskSuccess policy 
        lambda_multiple_role_policy = iam.Policy(self, "lambda_multiple_role_policy",
                        statements=[iam.PolicyStatement(
                            actions=[
                                     "s3:GetObject",
                                     "s3:PutObject",
                                     "states:SendTaskSuccess",
                                     "logs:CreateLogGroup",
                                     "logs:CreateLogStream",
                                     "logs:PutLogEvents",
                                     "dynamodb:Query",
                                     ],
                            resources=["*"]
                        )]         
        )   
        lambda_multiple_role.attach_inline_policy(lambda_multiple_role_policy)
        # Create a lambda function to be triggered by DynamoDB stream
        image_asset_path = "lambdas/" + aggregation_alg + "_func"
        serveraggregation_lambda = _lambda.DockerImageFunction(self, 'ServerFedAvgLambda',
            code=_lambda.DockerImageCode.from_image_asset(image_asset_path),
                timeout=Duration.seconds(lambda_timeout), # Default is only 3 seconds
                memory_size=1000, # If your docker code is pretty complex
                environment={
                    "SERVER_S3_ADDRESS": s3_models,
                    "REQUIRED_NUM_CLIENTS": num_clients_required,
                    "TASKS_TABLE_NAME": dynamodb_model_info,
                },
                role=lambda_multiple_role,
        )                
        # Remediate AwsSolutions-IAM5 
        NagSuppressions.add_resource_suppressions(lambda_multiple_role, [
            {
                "id": 'AwsSolutions-IAM5',
                "reason": 'Apply to multiple services.'
            },
        ]) 

        # Create S3 as FL registry to store models
        bucket = s3.Bucket(self, s3_models,
            bucket_name=s3_models, 
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )
        # Remediate AwsSolutions-S1
        NagSuppressions.add_resource_suppressions(bucket, [
            {
                "id": 'AwsSolutions-S1',
                "reason": 'Track local model info in DynamoDB.'
            },
        ])

        # Create a DynamoDB with Stream enabled to trigger serveraggregation_lambda
        fl_registry_dynamodb = dynamodb.Table(self, dynamodb_model_info,
            partition_key=dynamodb.Attribute(
                name="Task_Name", 
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="Task_ID", 
                type=dynamodb.AttributeType.STRING
            ),
            stream=dynamodb.StreamViewType.NEW_IMAGE,
            table_name=dynamodb_model_info,
            point_in_time_recovery=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        serveraggregation_lambda.add_event_source(DynamoEventSource(fl_registry_dynamodb,
            starting_position=_lambda.StartingPosition.TRIM_HORIZON,
            batch_size=5,
            bisect_batch_on_error=True,
            retry_attempts=0
        ))

        # Create a role for serverinitsns_lambda to access sns
        lambda_sns_role = iam.Role(self, "lambda_sns_role",
          assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")) 
        # Create a lambda policy statement to execute lambda, and send message to SQS and SNS
        lambda_execute_sqs_sns_policy = iam.Policy(self, "lambda_execute_sqs_sns_policy",
                            statements=[iam.PolicyStatement(
                                            actions=["logs:CreateLogGroup",
                                                    "logs:CreateLogStream",
                                                    "logs:PutLogEvents",
                                                    "sqs:CreateQueue",
                                                    "sqs:DeleteMessage",
                                                    "sqs:GetQueueAttributes",
                                                    "sqs:SetQueueAttributes",                     
                                                    "sqs:GetQueueUrl",
                                                    "sqs:PurgeQueue",
                                                    "sns:Publish",
                                                    "sns:Subscribe",
                                                    "sns:AddPermission",
                                                    "sns:CreateTopic"
                                                    ],
                                            resources=["*"]
                                        )]
        )
        lambda_sns_role.attach_inline_policy(lambda_execute_sqs_sns_policy)
        # Remediate AwsSolutions-IAM5 
        NagSuppressions.add_resource_suppressions(lambda_execute_sqs_sns_policy, [
            {
                "id": 'AwsSolutions-IAM5',
                "reason": 'Apply to multiple services.'
            },
        ])
        # Create a serverinitsns Lambda function for the StepFunctions
        serverinitsns_lambda = _lambda.Function(
            self, 'serverinitsns_lambda',
            runtime=_lambda.Runtime.PYTHON_3_10,
            code=_lambda.Code.from_asset('lambdas'),
            handler='serverinitsns.lambda_handler',
            role=lambda_sns_role,
            timeout=Duration.seconds(lambda_timeout),
        )

        # StepFunctions steps definition
        # Create a topic for sns to publish and sqs to subscribe
        topic_global = CfnTopic(self, 
                                "global-model-ready")
        topic_arn = topic_global.ref
        # Remediate AwsSolutions-SNS2
        NagSuppressions.add_resource_suppressions(topic_global, [
            {
                "id": 'AwsSolutions-SNS2',
                "reason": 'not required.'
            },
        ])
        # Remediate AwsSolutions-SNS3
        NagSuppressions.add_resource_suppressions(topic_global, [
            {
                "id": 'AwsSolutions-SNS3',
                "reason": 'not required.'
            },
        ])

        end_state = sfn.Fail(
            self, "Execution Failed",
        )

        serverinitsns_step = _aws_stepfunctions_tasks.LambdaInvoke(
            self, "Initalize server",
            lambda_function=serverinitsns_lambda,           
            payload=sfn.TaskInput.from_object({
                "sns_topic_server": topic_arn, 
                "sqs_region_1": server_config["clients"][0]["sqs_region_1"], #"us-east-1",
                "client_queue_name_1": server_config["clients"][0]["client_queue_name_1"], #"clientsqs-us-east-1",
                "sqs_region_2": server_config["clients"][1]["sqs_region_2"], #"us-west-2",
                "client_queue_name_2": server_config["clients"][1]["client_queue_name_2"], # "clientsqs-us-west-2",
                "roundId": starting_round_id,
                "member_ID": server_member_ID,
                "numClientsRequired": num_clients_required,
                "numClientEpochs": num_client_epochs,
                "weightsFile": model_file_name,
            }),
            result_selector={
                "taskresult": sfn.JsonPath.string_at("$.Payload"),
            },
            task_timeout=sfn.Timeout.duration(Duration.minutes(int(lambda_timeout))),
        )
        serverinitsns_step.add_catch(end_state)

        sns_step = _aws_stepfunctions_tasks.SnsPublish(
            self, "Notify clients",
            topic=sns.Topic.from_topic_arn(self, 'sns_topic', topic_arn), 
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            message=sfn.TaskInput.from_object({
                    "TaskToken": sfn.JsonPath.task_token,
                    "Input": sfn.JsonPath.string_at("$.taskresult")
                    }),
            result_path="$.taskresult",
            task_timeout=sfn.Timeout.duration(Duration.minutes(int(timeout_seconds_per_fl_round))), 
        )
        sns_step.add_catch(end_state)

        # Create a Lambda execution policy  
        afterAgg_lambda_role = iam.Role(self, "afterAgg_lambda_role",
          assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")) 
        afterAgg_lambda_policy = iam.Policy(self, "afterAgg_lambda_policy",
                statements=[
                    iam.PolicyStatement(
                                actions=["logs:CreateLogGroup",
                                        "logs:CreateLogStream",
                                        "logs:PutLogEvents",
                                        ],
                                resources=['*']
                            )    
                ])    
        afterAgg_lambda_role.attach_inline_policy(afterAgg_lambda_policy)
        # Remediate AwsSolutions-IAM5 
        NagSuppressions.add_resource_suppressions(afterAgg_lambda_policy, [
            {
                "id": 'AwsSolutions-IAM5',
                "reason": 'Apply to multiple services.'
            },
        ])
        # Create a lambda function for the StepFunctions
        afterAgg_lambda = _lambda.Function(
            self, 'afterAgg_lambda',
            runtime=_lambda.Runtime.PYTHON_3_10,
            code=_lambda.Code.from_asset('lambdas'),
            handler='afterAgg.lambda_handler',
            role=afterAgg_lambda_role,
        )
        afterAgg_step = _aws_stepfunctions_tasks.LambdaInvoke(
            self, "Next FL iteration",
            lambda_function=afterAgg_lambda, 
            payload= sfn.TaskInput.from_object({
                "Input": sfn.JsonPath.string_at("$"),
                "count_round": fl_rounds,
                "step_round": "1",
            }), 
            result_selector={
                "iterator": sfn.JsonPath.string_at("$.Payload"),
            },
        )
        afterAgg_step.add_catch(end_state)

        # Create a Lambda execution policy  
        prepareNextRound_lambda_role = iam.Role(self, "prepareNextRound_lambda_role",
          assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")) 
        prepareNextRound_lambda_policy = iam.Policy(self, "prepareNextRound_lambda_policy",
                statements=[
                    iam.PolicyStatement(
                                actions=["logs:CreateLogGroup",
                                        "logs:CreateLogStream",
                                        "logs:PutLogEvents",
                                        ],
                                resources=["*"]
                            )    
                ])    
        prepareNextRound_lambda_role.attach_inline_policy(prepareNextRound_lambda_policy)
        # Remediate AwsSolutions-IAM5 
        NagSuppressions.add_resource_suppressions(prepareNextRound_lambda_policy, [
            {
                "id": 'AwsSolutions-IAM5',
                "reason": 'Apply to multiple services.'
            },
        ])
        # Create a lambda function for the StepFunctions
        prepareNextRound_lambda = _lambda.Function(
            self, 'prepareNextRound_lambda',
            runtime=_lambda.Runtime.PYTHON_3_10,
            code=_lambda.Code.from_asset('lambdas'),
            handler='prepareNextRound.lambda_handler',
            role=prepareNextRound_lambda_role,
        )
        prepareNextRound_step = _aws_stepfunctions_tasks.LambdaInvoke(
            self, "No: next FL iteration",
            lambda_function=prepareNextRound_lambda,           
            payload= sfn.TaskInput.from_object({
                "Input": sfn.JsonPath.string_at("$"),
            }),
            result_selector={
                "taskresult": sfn.JsonPath.string_at("$.Payload"),
            },
            task_timeout=sfn.Timeout.duration(Duration.minutes(int(lambda_timeout))), 
        )

        # Create a role for endTraining_lambda to send sns        
        endTraining_lambda_role = iam.Role(self, "endTraining_lambda_role",
          assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")) 
        # Create a lambda execute policy, and send SNS to FL clients
        lambda_execute_sns_policy = iam.Policy(self, "lambda_execute_sns_policy",
                                        statements=[iam.PolicyStatement(
                                            actions=["logs:CreateLogGroup",
                                                    "logs:CreateLogStream",
                                                    "logs:PutLogEvents",
                                                    "sns:Publish",],
                                            resources=["*"]
                                        )]
        )
        endTraining_lambda_role.attach_inline_policy(lambda_execute_sns_policy)
        # Remediate AwsSolutions-IAM5 
        NagSuppressions.add_resource_suppressions(lambda_execute_sns_policy, [
            {
                "id": 'AwsSolutions-IAM5',
                "reason": 'Apply to multiple services.'
            },
        ])
        # Create an endTraining_lambda function for the StepFunctions
        endTraining_lambda = _lambda.Function(
            self, 'endTraining_lambda',
            runtime=_lambda.Runtime.PYTHON_3_10,
            code=_lambda.Code.from_asset('lambdas'),
            handler='endTraining.lambda_handler',
            role=endTraining_lambda_role,
        )
        endTraining_step = _aws_stepfunctions_tasks.LambdaInvoke(
            self, "Yes: end training",
            lambda_function=endTraining_lambda,           
            payload=sfn.TaskInput.from_object({
                "Input": sfn.JsonPath.string_at("$"),
                "sns_topic_server": topic_arn, 
            }),
            task_timeout=sfn.Timeout.duration(Duration.minutes(int(lambda_timeout))),
        )

        # Create Chain
        states_chain = serverinitsns_step.next(sns_step).next(afterAgg_step).next(sfn.Choice(self, "Satisfied?")
                            .when(
                                    sfn.Condition.string_equals("$.iterator.continue", "True"),
                                    prepareNextRound_step.next(sns_step)
                            )
                            .otherwise(endTraining_step)
                        )

        # Create state machine in the Stepfunctions
        log_group = logs.LogGroup(self, "stepfunctions_loggroup")
        sm = sfn.StateMachine(
            self, "FLServerStateMachine",
            definition=states_chain,
            logs=sfn.LogOptions(
                    destination=log_group,
                    level=sfn.LogLevel.ALL
            ),
            tracing_enabled=True 
        )
