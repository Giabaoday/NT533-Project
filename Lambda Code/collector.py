import json
import os
import logging
from datetime import datetime, timedelta
import facebook
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal
import time

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class FacebookCollector:
    def __init__(self):
        """Initialize with AWS services and credentials"""
        self.access_token = os.environ['FACEBOOK_ACCESS_TOKEN']
        self.page_id = os.environ['FACEBOOK_PAGE_ID']
        self.queue_url = os.environ['SQS_RAW_QUEUE_URL']
        
        # Initialize AWS clients
        self.graph = facebook.GraphAPI(access_token=self.access_token, version='3.1')
        self.sqs_client = boto3.client('sqs')
        self.dynamodb = boto3.resource('dynamodb')
        self.processed_table = self.dynamodb.Table(os.environ['PROCESSED_COMMENTS_TABLE'])
        self.posts_table = self.dynamodb.Table(os.environ['POSTS_TABLE'])
        
    def is_comment_processed(self, comment_id):
        """Check if comment has been processed before"""
        try:
            response = self.processed_table.get_item(
                Key={
                    'comment_id': comment_id
                }
            )
            return 'Item' in response
        except Exception as e:
            logger.error(f"Error checking comment status: {str(e)}")
            return False

    def mark_comment_processed(self, comment_id):
        """Mark comment as processed in DynamoDB"""
        try:
            self.processed_table.put_item(
                Item={
                    'comment_id': comment_id,
                    'processed_at': datetime.now().isoformat(),
                }
            )
            logger.info(f"Marked comment {comment_id} as processed")
            return True
        except Exception as e:
            logger.error(f"Error marking comment as processed: {str(e)}")
            return False

    def save_post_data(self, post, post_type='status', media_url=''):
        """Save or update post data in DynamoDB"""
        try:
            # Kiểm tra post đã tồn tại chưa
            existing_post = self.posts_table.get_item(
                Key={'post_id': post['id']}
            ).get('Item')

            if not existing_post:
                # Nếu là post mới thì tạo mới
                post_data = {
                    'post_id': post['id'],
                    'content': post.get('message', ''),
                    'post_type': post_type,
                    'created_time': post['created_time'],
                    'media_url': media_url,
                    'average_sentiment': Decimal('5.0'),
                    'average_toxic': Decimal('0'),
                    'sentiment_sum': Decimal('0'),
                    'toxic_sum': Decimal('0'),
                    'total_comments': 0,
                    'last_updated': datetime.now().isoformat()
                }
                self.posts_table.put_item(Item=post_data)
                logger.info(f"Created new post {post['id']}")
            else:
                # Nếu post đã tồn tại thì chỉ update các thông tin cơ bản
                self.posts_table.update_item(
                    Key={'post_id': post['id']},
                    UpdateExpression='SET content = :content, post_type = :type, media_url = :media, last_updated = :ts',
                    ExpressionAttributeValues={
                        ':content': post.get('message', ''),
                        ':type': post_type,
                        ':media': media_url,
                        ':ts': datetime.now().isoformat()
                    }
                )
                logger.info(f"Updated existing post {post['id']}")
            return True
        except Exception as e:
            logger.error(f"Error saving post data: {str(e)}")
            return False

    def get_page_posts(self, limit=5):
        """Get posts from Facebook page with more details"""
        try:
            posts = self.graph.get_connections(
                id=self.page_id,
                connection_name='posts',
                fields='id,message,created_time,attachments{type,media,url},comments.limit(50){id,message,created_time}',
                limit=limit
            )
            
            # Lưu thông tin post vào DynamoDB
            for post in posts.get('data', []):
                # Xác định post type và media url từ attachments
                post_type = 'status'
                media_url = ''
                
                if 'attachments' in post and 'data' in post['attachments']:
                    attachment = post['attachments']['data'][0]
                    post_type = attachment.get('type', 'status')
                    if 'media' in attachment:
                        media_url = attachment['media'].get('image', {}).get('src', '')
                    elif 'url' in attachment:
                        media_url = attachment['url']
                
                # Lưu hoặc update post
                self.save_post_data(post, post_type, media_url)
                
            logger.info(f"Retrieved and processed {len(posts.get('data', []))} posts")
            return posts.get('data', [])
        except Exception as e:
            logger.error(f"Error getting posts: {str(e)}")
            raise

    def extract_comments(self, posts):
        """Extract and filter unprocessed comments from posts"""
        new_comments = []
        processed_comments = 0
        skipped_comments = 0
        
        for post in posts:
            if 'comments' in post and 'data' in post['comments']:
                post_comments = post['comments']['data']
                for comment in post_comments:
                    try:
                        comment_id = comment['id']
                        
                        # Skip if already processed
                        if self.is_comment_processed(comment_id):
                            skipped_comments += 1
                            continue
                            
                        comment_data = {
                            'comment_id': comment_id,
                            'post_id': post['id'],
                            'comment_text': comment.get('message', ''),
                            'timestamp': int(datetime.strptime(comment['created_time'], 
                                                            '%Y-%m-%dT%H:%M:%S+0000').timestamp()),
                            'metadata': {
                                'platform': 'Facebook',
                                'page_id': self.page_id,
                                'post_type': post.get('type', 'unknown')
                            }
                        }
                        
                        new_comments.append(comment_data)
                        
                        # Mark as processed
                        if self.mark_comment_processed(comment_id):
                            processed_comments += 1
                            
                    except Exception as e:
                        logger.error(f"Error processing comment: {str(e)}")
                        continue

        logger.info(f"Processed: {processed_comments}, Skipped: {skipped_comments}, New: {len(new_comments)}")
        return new_comments

    def send_to_sqs(self, comments):
        """Send comments to SQS queue"""
        if not comments:
            logger.info("No new comments to send")
            return 0

        messages_sent = 0
        for comment in comments:
            try:
                response = self.sqs_client.send_message(
                    QueueUrl=self.queue_url,
                    MessageBody=json.dumps(comment, ensure_ascii=False),
                    MessageAttributes={
                        'DataType': {
                            'StringValue': 'FacebookComment',
                            'DataType': 'String'
                        }
                    }
                )
                messages_sent += 1
                logger.debug(f"Sent message {response['MessageId']} to SQS")
            except Exception as e:
                logger.error(f"Error sending message to SQS: {str(e)}")
                continue

        logger.info(f"Successfully sent {messages_sent} messages to SQS")
        return messages_sent

def lambda_handler(event, context):
    """Main Lambda handler"""
    try:
        collector = FacebookCollector()
        
        # Get posts
        posts = collector.get_page_posts()
        if not posts:
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'No posts found'})
            }
        
        # Extract new comments
        comments = collector.extract_comments(posts)
        if not comments:
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'No new comments found'})
            }
        
        # Send to SQS
        messages_sent = collector.send_to_sqs(comments)

        time.sleep(10)
        eventbridge = boto3.client('events')
        eventbridge.put_events(
            Entries=[{
                'Source': 'facebook.collector',
                'DetailType': 'SaveHistory',
                'Detail': json.dumps({
                    'timestamp': datetime.now().isoformat()
                }),
                'Time': datetime.now()
            }]
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Success',
                'new_comments': len(comments),
                'messages_sent': messages_sent
            }, ensure_ascii=False)
        }
        
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }