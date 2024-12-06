import json
import os
import boto3
from botocore.exceptions import ClientError
import logging
from decimal import Decimal

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class CommentProcessor:
    def __init__(self):
        """Initialize processor with AWS services"""
        self.sqs_client = boto3.client('sqs')
        self.dynamodb = boto3.resource('dynamodb')
        self.comprehend = boto3.client('comprehend')
        self.result_queue_url = os.environ['SQS_RESULT_QUEUE_URL']
        self.table = self.dynamodb.Table(os.environ['DYNAMODB_TABLE'])

    def detect_language(self, text):
        """Detect language using Amazon Comprehend"""
        try:
            response = self.comprehend.detect_dominant_language(Text=text)
            languages = response['Languages']
            if languages:
                # Lấy ngôn ngữ có score cao nhất
                return languages[0]['LanguageCode']
            return 'unknown'
        except Exception as e:
            logger.error(f"Error detecting language: {str(e)}")
            return 'unknown'

    def analyze_sentiment(self, text, language_code):
        """Analyze sentiment using Amazon Comprehend"""
        try:
            response = self.comprehend.detect_sentiment(
                Text=text,
                LanguageCode=language_code
            )
            # Chuyển đổi sentiment score sang thang -1 đến 1
            sentiment_map = {
            'POSITIVE': 8.5,    # Tích cực
            'NEGATIVE': 1.5,    # Tiêu cực
            'NEUTRAL': 5.0,     # Trung tính
            'MIXED': 5.0        # Hỗn hợp
            }
            return sentiment_map.get(response['Sentiment'], 0.0)
        except Exception as e:
            logger.error(f"Error analyzing sentiment: {str(e)}")
            return 5.0

    def detect_toxic(self, text, language_code):
        """Basic toxic detection using keyword matching"""
        try:
            # Danh sách từ độc hại cơ bản
            toxic_words = ['fuck', 'shit', 'damn', 'hate', 'stupid', 'idiot']
            text = text.lower()
            word_count = len(text.split())
            if word_count == 0:
                return 0.0
            
            toxic_count = sum(1 for word in toxic_words if word in text)
            toxic_score = (toxic_count / word_count) * 10
            return min(10.0, toxic_score)
        except Exception as e:
            logger.error(f"Error detecting toxic content: {str(e)}")
            return 0.0

    def process_comment(self, comment):
        """Process a single comment"""
        try:
            text = comment.get('comment_text', '')
            
            # Detect language first
            language = self.detect_language(text)
            default_lang = language if language != 'unknown' else 'en'
            
            # Calculate scores (gọi trực tiếp detect_toxic thay vì qua API)
            sentiment_score = self.analyze_sentiment(text, default_lang) 
            toxic_score = self.detect_toxic(text, default_lang)  # Thay đổi ở đây
            
            processed_data = {
                'comment_id': comment['comment_id'],
                'post_id': comment['post_id'],
                'comment_text': text,
                'timestamp': comment['timestamp'],
                'language': language,
                'sentiment_score': float(sentiment_score),
                'toxic_score': float(toxic_score),
                'processed_status': 'COMPLETED',
                'metadata': comment.get('metadata', {})
            }
            
            # Save to DynamoDB
            #self.table.put_item(Item=processed_data)
            
            # Send to result queue
            self.sqs_client.send_message(
                QueueUrl=self.result_queue_url,
                MessageBody=json.dumps(processed_data)
            )
            
            return True
        
        except Exception as e:
            logger.error(f"Error processing comment {comment.get('comment_id')}: {str(e)}")
            return False

    def process_batch(self, records):
        """Process a batch of records"""
        failed_records = []
        
        for record in records:
            try:
                comment = json.loads(record['body'])
                success = self.process_comment(comment)
                if not success:
                    failed_records.append(record['messageId'])
            except Exception as e:
                logger.error(f"Error processing record: {str(e)}")
                failed_records.append(record['messageId'])
                
        return failed_records

def lambda_handler(event, context):
    """Lambda handler for processing comments"""
    logger.info(f"Input event: {json.dumps(event)}")
    
    try:
        # Kiểm tra và chuẩn bị event data
        if isinstance(event, str):
            event = json.loads(event)
            
        if isinstance(event, dict):
            records = event.get('Records', [])
        else:
            records = []
            
        logger.info(f"Processing {len(records)} records")
        
        processor = CommentProcessor()
        failed_records = processor.process_batch(records)
        
        if failed_records:
            return {
                'batchItemFailures': [
                    {'itemIdentifier': record_id} for record_id in failed_records
                ]
            }
        
        return {'batchItemFailures': []}
        
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")
        logger.error(f"Event: {event}")
        raise