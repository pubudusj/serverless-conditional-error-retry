import json

def handler(event, context):
    print("Enrichment")
    return format(event)


def format(event):
    records = []
    for record in event:
        print(record)
        data = json.loads(record['body'])
        original_payload = json.loads(data['requestPayload']['Records'][0]['Sns']['Message'])
        topic_arn = data['requestPayload']['Records'][0]['Sns']['TopicArn']
        response_payload = data['responsePayload']

        if 'retry_count' in original_payload:
            original_payload['retry_count'] += 1
        else:
            original_payload['retry_count'] = 1

        records.append({
            'payload': original_payload,
            'meta': {
                'source_topic': topic_arn,
                'error_type': response_payload['errorType'] if 'errorType' in response_payload else '',
                'retry_count': original_payload['retry_count'],
                'response_payload': response_payload, 
            }
        })
    print(records)
    return records