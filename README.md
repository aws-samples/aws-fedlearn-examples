# Federated Learning Examples
## Deploying the FL server using AWS CDK:
* Set up a Python [Cloud Development Kit](https://aws.amazon.com/cdk/)(CDK) development environment following https://cdkworkshop.com/
* Run cdk from your PC to deploy the services at the FL server
    * Under the directory `fedlearn-cdk-hcls` (containing the file `app.py`), run commands:
        1. `cdk bootstrap aws://your-aws-account_id/your-aws-region` (replace `your-aws-account_id` by "AWS_account_A ID" and `your-aws-region` with your AWS account region)
        2. `python -m pip install -r requirements.txt`
        3. Install and start docker desktop on your PC 
        4. `cdk synth`
        5. `cdk deploy`
    * Wait until the deployment is finished
        

## Setting up Client 1 and Client 2 at Amazon SageMaker for Local Training
* zip the two folders `Client1_cdk` and `Client2_cdk` to `Client1_cdk.zip` and `Client2_cdk.zip`, respectively
* Login to "AWS_account_A"
* Go to **Amazon SageMaker** service and launch **SageMaker Studio**
* In SageMaker Studio, upload `Client1_cdk.zip` and `Client2_cdk.zip`
* Open a Terminal in SageMaker Studio
* Run commands below in the terminal to unzip both zip files:
    1. `sudo yum install -y unzip`
    2. `unzip Client1_cdk.zip`
    3. `unzip Client2_cdk.zip`
* In SageMaker Studio, double click  `Client1_cdk/client1-auto.ipynb`, to open a notebook
* In SageMaker Studio, double click  `Client2_cdk/client2-auto.ipynb`, to open a second notebook

## Launching the FL server
* Login "AWS_account_A"
* Go to **Amazon Step Functions** (https://us-west-2.console.aws.amazon.com/states/home?region=us-west-2) service
* Click the state machine created by CDK and click “Start execution”
* Wait until the state machine reaches *Step “1: Notify“*

## Launch Client 1
* In the `client1-auto.ipynb` notebook, click the “run the select cells and advance”

## Launch Client 2
* In the `client2-auto.ipynb` notebook, click the “run the select cells and advance”

## Destroy FL server resources after finished
* Delete all files in the S3 bucket created by CDK
* Under the directory `fedlearn-cdk-workshop` (containing the file `app.py`), run commands:
    * `cdk destroy`

## Useful commands
| Command | Description |
| ------- | ------- |
| `cdk ls` | list all stacks in the app |
| `cdk synth` | emits the synthesized CloudFormation template |
| `cdk deploy` | deploy this stack to your default AWS account/region |
| `cdk diff` | compare deployed stack with current state |
| `cdk docs` | open CDK documentation |

## Enjoy!
