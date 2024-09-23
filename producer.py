import json
import random
import time
import requests
import boto3
import base64
import dotenv

dotenv.load_dotenv()

EVENT_TYPES = ['login_attempt', 'port_scan', 'malware_detected', 'access_granted', 'access_denied']
SEVERITY_LEVELS = ['low', 'medium', 'high']
DESCRIPTIONS = {
    'login_attempt': 'User attempted to login',
    'port_scan': 'Multiple ports scanned on host',
    'malware_detected': 'Malware detected on system',
    'access_granted': 'User access granted',
    'access_denied': 'User access denied'
}
MALICIOUS_IPS = ['192.0.2.1', '198.51.100.2', '203.0.113.3']  # Example malicious IPs

def generate_ip():
    return '.'.join(str(random.randint(1, 254)) for _ in range(4))

def generate_event():
    event_type = random.choice(EVENT_TYPES)
    severity = random.choice(SEVERITY_LEVELS)
    source_ip = random.choice(MALICIOUS_IPS) if random.random() < 0.1 else generate_ip()
    event = {
        'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'source_ip': source_ip,
        'destination_ip': generate_ip(),
        'event_type': event_type,
        'severity': severity,
        'description': DESCRIPTIONS[event_type]
    }
    return event

kinesis_client = boto3.client('kinesis', region_name='ap-southeast-2')

def send_event(event):
    kinesis_client.put_record(
        StreamName='nullify-source-stream',
        Data=json.dumps(event),
        PartitionKey=event['source_ip']
    )

def main():
    for _ in range(10):
        event = generate_event()
        send_event(event)
        print(f'Sent event: {event}')
        time.sleep(random.uniform(0.1, 0.5))  # Sleep for a short time between events

if __name__ == '__main__':
    main()