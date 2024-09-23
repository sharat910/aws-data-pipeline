import json
import os
import boto3
import base64

MALICIOUS_IPS = ['192.0.2.1', '198.51.100.2', '203.0.113.3']  # Example malicious IPs

def threat_detection(event):
    severity = event.get('severity', None)
    if severity is None:
        raise ValueError('severity is required: malformed event')
    severity_flag = severity == 'high'
    
    source_ip = event.get('source_ip', None)
    if source_ip is None:
        raise ValueError('source_ip is required: malformed event')
    
    malicious_ip = source_ip in MALICIOUS_IPS
    if severity_flag and malicious_ip:
        return True, "High severity & malicious IP detected"
    elif severity_flag:
        return True, "High severity detected"
    elif malicious_ip:
        return True, "Malicious IP detected"
    else:
        return False, "No threats detected"

sns_client = boto3.client('sns')

def lambda_handler(event, context):    
    for record in event['Records']:
        # Decode and parse the Kinesis record
        decoded_data = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        payload = json.loads(decoded_data)
        print("received:", payload)
        # Check if the event represents a threat
        is_threat, message = threat_detection(payload)
        if is_threat:
            # If it's a threat, publish to SNS topic
            payload['threat_info'] = message
            print("threat:", payload)

            sns_client.publish(
                TopicArn='arn:aws:sns:ap-southeast-2:593793046736:nullify-threat-intel',
                Message=json.dumps(payload),
                Subject=message,
            )
    
    return {
        'statusCode': 200,
        'body': 'Processed events from Kinesis and sent threats to SNS'
    }