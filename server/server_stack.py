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

class ServerStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Server info from server_config.json
        # load configuration file
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
                cross_account_s3_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name(
                        'AmazonS3FullAccess',
                        ))

                cross_account_dynamodb_role = iam.Role(self, db_across_account_role_name, 
                                            assumed_by=iam.AccountPrincipal(client['across_account_id']),
                                            role_name=db_across_account_role_name,
                                        )
                cross_account_dynamodb_role.apply_removal_policy(RemovalPolicy.DESTROY)  
                cross_account_dynamodb_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name(
                        'AmazonDynamoDBFullAccess',
                        ))

                cross_account_sqs_role = iam.Role(self, sqs_across_account_role_name, 
                                            assumed_by=iam.AccountPrincipal(client['across_account_id']),
                                            role_name=sqs_across_account_role_name,
                                        )
                cross_account_sqs_role.apply_removal_policy(RemovalPolicy.DESTROY) 
                cross_account_sqs_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name(
                        'AmazonSQSFullAccess'
                        ))

        # Create a lambda function to be triggered by DynamoDB stream
        lambda_multiple_role = iam.Role(self, "lambda_multiple_role",
          assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")) 
        lambda_multiple_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                        'AmazonSQSFullAccess',
            )
        )
        lambda_multiple_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                        'AmazonSNSFullAccess',
            )
        )
        lambda_multiple_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                        'AWSLambdaExecute',
            )
        )
        lambda_multiple_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                        'AmazonDynamoDBFullAccess',
            )
        )
        lambda_multiple_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                        'AmazonS3FullAccess',
            )
        )
        lambda_multiple_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                        'AWSStepFunctionsFullAccess',
            )
        )
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

        # Create S3 as FL registry to store models
        fl_registry_s3 = s3.Bucket(self, s3_models,
            bucket_name=s3_models, 
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )
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
        lambda_sns_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                        'AmazonSQSFullAccess',
            )
        )
        lambda_sns_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                        'AmazonSNSFullAccess',
            )
        )
        lambda_sns_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                        'AWSLambdaExecute',
            )
        )
        # Create lambda functions for the StepFunctions
        serverinitsns_lambda = _lambda.Function(
            self, 'serverinitsns_lambda',
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset('lambdas'),
            handler='serverinitsns.lambda_handler',
            role=lambda_sns_role,
            timeout=Duration.seconds(lambda_timeout),
        )

        #StepFunctions steps definition
        #Create a topic for sns to publish and sqs to subscribe
        topic_global = CfnTopic(self, "global-model-ready")
        topic_arn = topic_global.ref

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
            timeout=Duration.seconds(lambda_timeout),
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
            timeout=Duration.seconds(int(timeout_seconds_per_fl_round)),
        )
        sns_step.add_catch(end_state)

        afterAgg_lambda = _lambda.Function(
            self, 'afterAgg_lambda',
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset('lambdas'),
            handler='afterAgg.lambda_handler',
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

        prepareNextRound_lambda = _lambda.Function(
            self, 'prepareNextRound_lambda',
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset('lambdas'),
            handler='prepareNextRound.lambda_handler',
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
            timeout=Duration.seconds(lambda_timeout),
        )

        # Create a role for endTraining_lambda to send sns
        endTraining_lambda_role = iam.Role(self, "endTraining_lambda_role",
          assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")) 
        endTraining_lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                        'AmazonSNSFullAccess',
            )
        )
        endTraining_lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                        'AWSLambdaExecute',
            )
        )
        endTraining_lambda = _lambda.Function(
            self, 'endTraining_lambda',
            runtime=_lambda.Runtime.PYTHON_3_9,
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
            timeout=Duration.seconds(lambda_timeout),
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
        sm = sfn.StateMachine(
            self, "FLServerStateMachine",
            definition=states_chain,
        )
