import json
import boto3

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('csv')


def lambda_handler(event, context):
    try:
        params = json.loads(event['body'])
        bucket_name = params.get('bucket_name')
        object_key = params.get('object_key')
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        csv = response['Body'].read().decode('utf-8')
        csv = csv.split('\n')
        for idx, value in enumerate(csv):
            value = value.split(';')
            if len(value) == 2:
                cpf_cnpj = value[0].replace('.', '').replace('-', '')
                date = value[1].split('/')
                date = date[0] + '-' + date[1] + '-' + date[2]
                table.put_item(TableName='csv', Item={'id': str(idx), 'cpf_cnpj': cpf_cnpj, 'date': date})
        return {
            'statusCode': 200
        }
    except Exception as error:
        return {
            'error': error
        }


