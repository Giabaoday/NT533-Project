import json
import boto3
from datetime import datetime
from decimal import Decimal

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    posts_table = dynamodb.Table('fb_comments_analysis_table')
    history_table = dynamodb.Table('post_history')
    
    try:
        response = posts_table.scan()
        
        for post in response['Items']:
            history_table.put_item(
                Item={
                    'post_id': post['post_id'],
                    'last_updated': post['last_updated'],
                    'average_sentiment': Decimal(post['average_sentiment']),
                    'total_comments': int(post['total_comments'])
                }
            )
            
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'History saved successfully',
                'timestamp': datetime.now().isoformat()
            })
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }