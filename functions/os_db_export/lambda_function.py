import base64
import json
import os
from opensearchpy import OpenSearch, RequestsHttpConnection

service = 'aoss'
region = 'ap-southeast-2'
service = 'es'
username = 'nullify'
password = os.getenv('OPENSEARCH_PASSWORD')

host = 'search-nullify-test-senb2mnquk4zcaqdlnubnv5g2y.aos.ap-southeast-2.on.aws'

# create an opensearch client and use the username and password for authentication
client = OpenSearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=(username, password),
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    pool_maxsize=20,
)

index = 'nullify-events'

# Check if the index exists, if not, create it
if not client.indices.exists(index=index):
    create_response = client.indices.create(
        index=index,
        body={
            'settings': {
                'number_of_shards': 1,
                'number_of_replicas': 0
            },
            'mappings': {
                'properties': {
                    'id': {'type': 'keyword'},
                    'timestamp': {'type': 'date'},
                    'enriched_record': {
                        'type': 'object',
                        'properties': {
                            'timestamp': {'type': 'date'},
                            'source_ip': {'type': 'ip'},
                            'destination_ip': {'type': 'ip'},
                            'event_type': {'type': 'keyword'},
                            'severity': {'type': 'keyword'},
                            'description': {'type': 'text'},
                            'country': {'type': 'keyword'},
                            'regionName': {'type': 'keyword'},
                            'city': {'type': 'keyword'},
                            'lat': {'type': 'float'},
                            'lon': {'type': 'float'}
                        }
                    }
                },
                'dynamic': True  # Allow schema flexibility for future changes
            }
        }
    )
    print('Creating index:')
    print(create_response)

def lambda_handler(event, context):
    success_count = 0
    failed_count = 0
    for record in event['Records']:
        try:
            id = record['eventID']
            timestamp = record['kinesis']['approximateArrivalTimestamp']

            # Kinesis data is base64-encoded, so decode here
            message = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            # Parse the JSON document
            enriched_record = json.loads(message)
            # Create the JSON document
            document = { "id": id, "timestamp": timestamp, "enriched_record": enriched_record }
            # Index the document
            response = client.index(
                index=index,
                body=document,
                id=id
            )
            if response['result'] in ['created', 'updated']:
                success_count += 1
            else:
                print(f"Error processing record: {response}")
                failed_count += 1
        except Exception as e:
            print(f"Error processing record: {e}")
            failed_count += 1
    return 'Processed ' + str(success_count) + ' items, failed ' + str(failed_count) + ' items.'