import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

const dynamodb = DynamoDBDocument.from(new DynamoDB({}));
const TABLE_NAME = 'fb_comments_analysis_table';
const HISTORY_TABLE = 'post_history';

export const handler = async (event) => {
    console.log('Event:', JSON.stringify(event, null, 2));
    
    try {
        const httpMethod = event.httpMethod || event.requestContext.http.method;
        const path = event.path || event.requestContext.http.path;
        
        console.log('Method:', httpMethod);
        console.log('Path:', path);
        
        // GET /posts - Lấy danh sách posts
        if (path.match(/^\/posts$/) && httpMethod === 'GET') {
            const params = {
                TableName: TABLE_NAME,
                ProjectionExpression: "post_id, content, created_time, last_updated, media_url, post_type, average_sentiment, average_toxic, total_comments, sentiment_sum, toxic_sum"
            };
            
            const result = await dynamodb.scan(params);
            
            const formattedPosts = result.Items.map(post => ({
                post_id: post.post_id,
                content: post.content,
                created_time: post.created_time,
                last_updated: post.last_updated,
                media_url: post.media_url || '',
                post_type: post.post_type,
                average_sentiment: post.average_sentiment || 0,
                average_toxic: post.average_toxic || 0,
                total_comments: post.total_comments || 0,
                sentiment_sum: post.sentiment_sum || 0,
                toxic_sum: post.toxic_sum || 0
            }));

            return {
                statusCode: 200,
                headers: {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify(formattedPosts)
            };
        }
        
        // GET /posts/{postId}/analytics
        else if (path.match(/^\/posts\/[^/]+\/analytics$/) && httpMethod === 'GET') {
            const postId = path.split('/')[2];
            
            const params = {
                TableName: TABLE_NAME,
                Key: {
                    'post_id': postId
                }
            };
            
            const result = await dynamodb.get(params);
            
            if (!result.Item) {
                return {
                    statusCode: 404,
                    headers: {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    body: JSON.stringify({ message: 'Post not found' })
                };
            }

            const analytics = {
                post_id: result.Item.post_id,
                content: result.Item.content,
                created_time: result.Item.created_time,
                last_updated: result.Item.last_updated,
                media_url: result.Item.media_url || '',
                post_type: result.Item.post_type,
                analytics: {
                    average_sentiment: result.Item.average_sentiment || 0,
                    average_toxic: result.Item.average_toxic || 0,
                    total_comments: result.Item.total_comments || 0,
                    sentiment_sum: result.Item.sentiment_sum || 0,
                    toxic_sum: result.Item.toxic_sum || 0
                }
            };

            return {
                statusCode: 200,
                headers: {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify(analytics)
            };
        }

        else if (path.match(/^\/posts\/[^/]+\/history$/) && httpMethod === 'GET') {
            const postId = path.split('/')[2];
            
            const params = {
                TableName: HISTORY_TABLE,
                KeyConditionExpression: 'post_id = :pid',
                ExpressionAttributeValues: {
                    ':pid': postId
                },
                ScanIndexForward: true
            };
            
            const result = await dynamodb.query(params);
            
            return {
                statusCode: 200,
                headers: {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify(result.Items)
            };
        }
        
        // Handle OPTIONS for CORS
        else if (httpMethod === 'OPTIONS') {
            return {
                statusCode: 200,
                headers: {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                    'Access-Control-Allow-Methods': 'GET,OPTIONS'
                },
                body: ''
            };
        }
        
        return {
            statusCode: 404,
            headers: {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            body: JSON.stringify({ message: 'Route not found' })
        };
        
    } catch (error) {
        console.error('Detailed error:', {
            message: error.message,
            stack: error.stack,
            event: event
        });
        
        return {
            statusCode: 500,
            headers: {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            body: JSON.stringify({ 
                message: 'Internal Server Error',
                details: error.message
            })
        };
    }
};