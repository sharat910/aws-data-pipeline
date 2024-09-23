import json
import os
import boto3
import base64
import urllib3

def get_geo_info(ip):
    # TODO: Can use dynamo DB / prefix lookup to cache the results
    http = urllib3.PoolManager()
    try:
        response = http.request('GET', f'http://ip-api.com/json/{ip}')
        if response.status == 200:
            data = json.loads(response.data.decode('utf-8'))
            return {
                'country': data.get('country'),
                'regionName': data.get('regionName'),
                'city': data.get('city'),
                'lat': data.get('lat'),
                'lon': data.get('lon')
            }
    except Exception as e:
        print(f'Error fetching geo info for IP {ip}: {e}')
    return {}


## Lambda Handler

kinesis_client = boto3.client('kinesis', region_name='ap-southeast-2')

def lambda_handler(event, context):
    processed_records = 0
    failed_records = 0
    
    for record in event['Records']:
        try:
            decoded_data = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            payload = json.loads(decoded_data)
            print(f"Payload before enrichment: {payload}")
            if 'source_ip' not in payload:
                raise ValueError("Source IP address not found in payload")
            
            geo_info = get_geo_info(payload['source_ip'])
            if len(geo_info) == 0:
                failed_records += 1
            print(f"Geo info: {geo_info}")
            payload.update(geo_info)
            print(f"Payload after enrichment: {payload}")
            kinesis_client.put_record(
                StreamName='nullify-enriched-stream',
                Data=json.dumps(payload),
                PartitionKey=payload['source_ip']
            )
            processed_records += 1
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            failed_records += 1
        except ValueError as e:
            print(f"Value error: {e}")
            failed_records += 1
        except Exception as e:
            print(f"Unexpected error processing record: {e}")
            failed_records += 1

    return {
        'statusCode': 200,
        'body': f'Processed {processed_records} records, failed {failed_records} records'
    }