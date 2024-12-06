import json
import os
import boto3
from botocore.exceptions import ClientError
import logging
from decimal import Decimal
from datetime import datetime

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class CommentAggregator:
    def __init__(self):
        """Initialize aggregator with AWS services"""
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(os.environ['DYNAMODB_TABLE'])

    def initialize_post_metrics(self, post_id, comments):
        """Initialize metrics for a new post"""
        try:
            total_comments = len(comments)
            sentiment_sum = sum(float(c.get('sentiment_score', 0)) for c in comments)
            toxic_sum = sum(float(c.get('toxic_score', 0)) for c in comments)
            
            # Count languages
            language_counts = {}
            for comment in comments:
                lang = comment.get('language', 'unknown')
                language_counts[lang] = language_counts.get(lang, 0) + 1

            self.table.put_item(
                Item={
                    'post_id': post_id,
                    'total_comments': total_comments,
                    'sentiment_sum': Decimal(str(sentiment_sum)),
                    'toxic_sum': Decimal(str(toxic_sum)),
                    'average_sentiment': Decimal(str(sentiment_sum / total_comments)),
                    'average_toxic': Decimal(str(toxic_sum / total_comments)),
                    'language_distribution': language_counts,
                    'last_updated': datetime.now().isoformat()
                }
            )
            logger.info(f"Initialized metrics for post {post_id}")
            return True
        except Exception as e:
            logger.error(f"Error initializing metrics: {str(e)}")
            return False

    def store_aggregation(self, post_id, new_comments):
        """Update aggregated data in DynamoDB using atomic updates"""
        try:
            # Calculate metrics for new comments
            total_new = len(new_comments)
            sentiment_sum = sum(float(c.get('sentiment_score', 0)) for c in new_comments)
            toxic_sum = sum(float(c.get('toxic_score', 0)) for c in new_comments)
            
            # Count languages
            new_lang_counts = {}
            for comment in new_comments:
                lang = comment.get('language', 'unknown')
                new_lang_counts[lang] = new_lang_counts.get(lang, 0) + 1

            try:
                # Try to update existing item
                response = self.table.update_item(
                    Key={'post_id': post_id},
                    UpdateExpression='ADD total_comments :inc, sentiment_sum :sent, toxic_sum :tox',
                    ExpressionAttributeValues={
                        ':inc': total_new,
                        ':sent': Decimal(str(sentiment_sum)),
                        ':tox': Decimal(str(toxic_sum))
                    },
                    ReturnValues='UPDATED_NEW'
                )
                
                # Calculate and update averages
                updated_total = response['Attributes']['total_comments']
                updated_sentiment = Decimal(str(response['Attributes']['sentiment_sum'])) / updated_total
                updated_toxic = Decimal(str(response['Attributes']['toxic_sum'])) / updated_total

                self.table.update_item(
                    Key={'post_id': post_id},
                    UpdateExpression='SET average_sentiment = :avg_sent, average_toxic = :avg_tox, last_updated = :ts',
                    ExpressionAttributeValues={
                        ':avg_sent': updated_sentiment,
                        ':avg_tox': updated_toxic,
                        ':ts': datetime.now().isoformat()
                    }
                )

            except ClientError as e:
                if e.response['Error']['Code'] == 'ValidationException':
                    # Item doesn't exist, create new
                    return self.initialize_post_metrics(post_id, new_comments)
                else:
                    raise

            logger.info(f"Successfully updated aggregation for post {post_id}")
            return True

        except Exception as e:
            logger.error(f"Error storing aggregation: {str(e)}")
            return False

    def aggregate_by_post(self, comments):
        """Aggregate comments by post_id"""
        try:
            # Group comments by post_id
            post_groups = {}
            for comment in comments:
                post_id = comment['post_id']
                if post_id not in post_groups:
                    post_groups[post_id] = []
                post_groups[post_id].append(comment)

            # Process each post's comments
            for post_id, post_comments in post_groups.items():
                success = self.store_aggregation(post_id, post_comments)
                if not success:
                    logger.error(f"Failed to store aggregation for post {post_id}")

            return True
        except Exception as e:
            logger.error(f"Error aggregating comments by post: {str(e)}")
            return False

    def process_batch(self, records):
        """Process a batch of records from Result Queue"""
        failed_records = []
        processed_comments = []

        for record in records:
            try:
                # Parse comment data
                comment_data = record['body']
                if isinstance(comment_data, str):
                    comment_data = json.loads(comment_data)
                
                logger.info(f"Processing comment: {json.dumps(comment_data)}")
                processed_comments.append(comment_data)
                
            except Exception as e:
                logger.error(f"Error parsing record: {str(e)}")
                logger.error(f"Record body: {record.get('body')}")
                failed_records.append(record['messageId'])

        if processed_comments:
            success = self.aggregate_by_post(processed_comments)
            if not success:
                failed_records.extend(record['messageId'] for record in records)

        return failed_records

def lambda_handler(event, context):
    """Lambda handler for processing results"""
    logger.info(f"Input event: {json.dumps(event)}")
    
    try:
        records = event.get('Records', [])
        logger.info(f"Processing {len(records)} records")
        
        if not records:
            logger.info("No records to process")
            return {'batchItemFailures': []}

        aggregator = CommentAggregator()
        failed_records = aggregator.process_batch(records)
        
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