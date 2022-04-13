import json
import boto3
import time
import botocore

def lambda_handler(event, context):
    
    # Opening parameters file
    f = open('params.json')
     
    # returns JSON object as a dictionary
    data = json.load(f)
     
    # Closing file
    f.close()
    
    path=data['path']
    source_parameters = data['Parameters']
    destination_account = data['destination_account']
    destination_region = data['destination_region']
    cross_account_role_arn = data['cross_account_role_arn']
    destination_kms_key_id = data['destination_kms_key_id']
    
    #get parameters
    destination_parameters = get_parameters(source_parameters,path,destination_kms_key_id)
    
    #put parameters
    store_parameters_cross_account(destination_parameters,destination_account,destination_region,cross_account_role_arn) if destination_account else store_parameters_cross_region(destination_parameters,destination_region)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
    
    
#Function to get parameter values by passing list of parameter names
def get_parameters(source_parameters,parameters_path,destination_kms_key_id):
    ssm_client = boto3.client('ssm')
    get_parameter_response_list = []
    describe_parameter_response_list = []
    
    if source_parameters:
        #Splitting source parameters list in to smaller chunks (10 per list)
        source_parameters_chunks = [source_parameters[x:x+10] for x in range(0, len(source_parameters), 10)]
        for parameters_list in source_parameters_chunks:
            #get parameter api call to retrieve parameter value, parameter type and data type
            get_parameter_response = ssm_client.get_parameters(
                Names=parameters_list,
                WithDecryption=True
            )
            get_parameter_response_list.extend(get_parameter_response['Parameters'])
        
        #describe parameter api call to retrieve parameter tier, policies, kmskeyId etc
        describe_parameter_response = ssm_client.describe_parameters(
                ParameterFilters=[
                    {
                    'Key': 'Name',
                    'Values': source_parameters
                    },
                ],
            )
        describe_parameter_response_list.extend(describe_parameter_response['Parameters'])
        
        while "NextToken" in describe_parameter_response:
                nextToken=describe_parameter_response["NextToken"]
                describe_parameter_response = ssm_client.describe_parameters(
                    ParameterFilters=[
                        {
                        'Key': 'Name',
                        'Values': source_parameters
                        },
                    ],
                    NextToken=nextToken
                )
                describe_parameter_response_list.extend(describe_parameter_response['Parameters'])
            
    
    if parameters_path:
        #get parameter api call to retrieve parameter value, parameter type and data type
        for path in parameters_path:
            get_parameter_response = ssm_client.get_parameters_by_path(
                Path=path,
                Recursive=True,
                WithDecryption=True
            )
            get_parameter_response_list.extend(get_parameter_response['Parameters'])
            
            while "NextToken" in get_parameter_response:
                nextToken=get_parameter_response["NextToken"]
                get_parameter_response = ssm_client.get_parameters_by_path(
                    Path=path,
                    Recursive=True,
                    WithDecryption=True,
                    NextToken=nextToken
                )
                get_parameter_response_list.extend(get_parameter_response['Parameters'])
            
            #describe parameter api call to retrieve parameter tier, policies, kmskeyId etc
            describe_parameter_response = ssm_client.describe_parameters(
                ParameterFilters=[
                    {
                    'Key': 'Path',
                    'Values': [path]
                    },
                ],
            )
            describe_parameter_response_list.extend(describe_parameter_response['Parameters'])
            
            while "NextToken" in describe_parameter_response:
                nextToken=describe_parameter_response["NextToken"]
                describe_parameter_response = ssm_client.describe_parameters(
                    ParameterFilters=[
                        {
                        'Key': 'Path',
                        'Values': [path]
                        },
                    ],
                    NextToken=nextToken
                )
                describe_parameter_response_list.extend(describe_parameter_response['Parameters'])
    
    destination_parameters = get_parameter_details(get_parameter_response_list,describe_parameter_response_list,destination_kms_key_id)
    return destination_parameters

    
#Function to parse through get and describe parameter calls
def get_parameter_details(get_parameter_response_list, describe_parameter_response_list,destination_kms_key_id):
    ssm_client = boto3.client('ssm')
    destination_parameters = {}
    
    for parameter in get_parameter_response_list:
        parameter_data = {}
        name = parameter['Name']
        parameter_data.update({'Name':parameter['Name'],'Type':parameter['Type'],'Value':parameter['Value'],'DataType':parameter['DataType']})
        destination_parameters[name]=parameter_data
    
    
    for parameter in describe_parameter_response_list:
        parameter_data = {}
        name = parameter['Name']
        parameter_data.update({'Tier':parameter['Tier']})
        if 'Policies' in parameter:
            if parameter['Policies']:
                policies_list = []
                for policy in parameter['Policies']:
                    policies_list.append(json.loads(policy['PolicyText']))
                parameter_data.update({'Policies':json.dumps(policies_list)})
         
        if 'Description' in parameter:
            parameter_data.update({'Description':parameter['Description']})

        if (parameter['Type']=="SecureString"):
            if 'alias/aws/ssm' not in parameter["KeyId"]:
                if destination_kms_key_id:
                    parameter_data.update({'KeyId':destination_kms_key_id})
        
        destination_parameters[name].update(parameter_data)
    return destination_parameters
    
    
#function to create parameters cross account
def store_parameters_cross_account(destination_parameters,destination_account,destination_region,cross_account_role_arn):
    #Cross account assume role
    sts_client = boto3.client('sts')
    cross_account_creds = sts_client.assume_role(
        RoleArn=cross_account_role_arn,
        RoleSessionName="cross_acct_lambda"
    )
    
    access_key = cross_account_creds['Credentials']['AccessKeyId']
    secret_key = cross_account_creds['Credentials']['SecretAccessKey']
    session_token = cross_account_creds['Credentials']['SessionToken']

    ssm_client = boto3.client(
        'ssm',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
        region_name=destination_region
    )
    store_parameters(ssm_client,destination_parameters)

#function to create parameters cross region
def store_parameters_cross_region(destination_parameters,destination_region):
    ssm_client = boto3.client(
        'ssm',
        region_name=destination_region
    )
    store_parameters(ssm_client,destination_parameters)

    
#Function to store parameters in destination account/region
def store_parameters(ssm_client,destination_parameters):
    for parameter in destination_parameters.values():
        parameter['Overwrite']=True
        status="Fail"
        retry=0
        while (retry<5) and (status=="Fail"):
            try:
                print("Copying parameter " + parameter["Name"])
                response = ssm_client.put_parameter(
                    **parameter
                )
                if 'Tier' in response:
                    status="Success"
            except botocore.exceptions.ClientError as error:
                retry = retry+1
                backoff = pow(2,retry)
                time.sleep(0.1*backoff)
                print("Retry attempt:", retry)
                if (retry==5):
                    print("Failed to copy parameter " + parameter["Name"] + " after " + str(retry) + " attempts." )
                    print(error)
                continue