import json

def lambda_handler(event, context):
    # Iterate through each record in the SNS event
    for record in event['Records']:
        # Extract the SNS message
        sns_subject = record['Sns']['Subject']
        sns_message = record['Sns']['Message']
        try:
            # Log the notification
            print(f"Received notification: Subject: {sns_subject}, Message: {sns_message}")
        except Exception as e:
            print(f"Unexpected error processing message: {e}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Successfully processed SNS notifications')
    }
