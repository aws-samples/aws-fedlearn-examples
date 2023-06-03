FROM public.ecr.aws/lambda/python:3.8

COPY requirements.txt ./
RUN pip3 install -r requirements.txt
RUN pip install numpy==1.21.6
COPY SERVER_AGGREGATION.py ./
COPY CONSTANTS.py ./

CMD ["SERVER_AGGREGATION.lambda_handler"]
