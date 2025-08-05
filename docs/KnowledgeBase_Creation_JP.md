# Knowledge Base Creation Guide

This document explains the steps to create a Knowledge Base using AWS Bedrock. The following procedures show how to create a Knowledge Base with S3 bucket, OpenSearch collection, and automatic synchronization functionality.

## Prerequisites

- AWS CLI installed and configured
- Logged in as an IAM user or role with appropriate AWS permissions
- Target region determined (e.g., us-east-1)

## Step 1: 必要なポリシーファイルの準備

まず、IAMロール作成に必要なポリシーファイルを作成します。

```shell
# Bedrock Knowledge Base用のトラストポリシーファイルを作成
cat > bedrock-kb-trust-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "bedrock.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Lambda関数用のトラストポリシーファイルを作成
cat > lambda-trust-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
```

## Step 2: IAM Roles and Policies の作成

### 2.1 Bedrock Knowledge Base用のIAMロール作成

```shell
# 環境変数の設定（適宜変更してください）
export KB_ROLE_NAME="BedrockKnowledgeBaseRole"
export S3_BUCKET_NAME="your-kb-documents-bucket"
export OPENSEARCH_COLLECTION_NAME="your-opensearch-collection"
export AWS_REGION="us-east-1"
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Bedrock Knowledge Base用のIAMロールを作成
aws iam create-role \
  --role-name $KB_ROLE_NAME \
  --assume-role-policy-document file://bedrock-kb-trust-policy.json

# 必要な権限ポリシーをアタッチ
aws iam attach-role-policy \
  --role-name $KB_ROLE_NAME \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess

aws iam attach-role-policy \
  --role-name $KB_ROLE_NAME \
  --policy-arn arn:aws:iam::aws:policy/AmazonOpenSearchServiceFullAccess

aws iam attach-role-policy \
  --role-name $KB_ROLE_NAME \
  --policy-arn arn:aws:iam::aws:policy/AmazonBedrockFullAccess
```

### 2.2 OpenSearch Serverlessの追加設定

OpenSearch Serverlessを使用する場合、追加のポリシーが必要です。

```shell
# OpenSearch Serverless用のカスタムポリシーを作成
cat > opensearch-serverless-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "aoss:*"
      ],
      "Resource": "*"
    }
  ]
}
EOF

# カスタムポリシーを作成してアタッチ
aws iam put-role-policy \
  --role-name $KB_ROLE_NAME \
  --policy-name OpenSearchServerlessPolicy \
  --policy-document file://opensearch-serverless-policy.json
```

## Step 3: OpenSearch Serverless Collection の作成

### 3.1 セキュリティポリシーの作成

```shell
# 暗号化ポリシーを作成
aws opensearchserverless create-security-policy \
  --name "${OPENSEARCH_COLLECTION_NAME}-encryption-policy" \
  --type encryption \
  --policy '[{
    "Rules": [
      {
        "Resource": ["collection/'${OPENSEARCH_COLLECTION_NAME}'"],
        "ResourceType": "collection"
      }
    ],
    "AWSOwnedKey": true
  }]'

# ネットワークポリシーを作成
aws opensearchserverless create-security-policy \
  --name "${OPENSEARCH_COLLECTION_NAME}-network-policy" \
  --type network \
  --policy '[{
    "Rules": [
      {
        "Resource": ["collection/'${OPENSEARCH_COLLECTION_NAME}'"],
        "ResourceType": "collection"
      }
    ],
    "AllowFromPublic": true
  }]'
```

### 3.2 アクセスポリシーの作成

```shell
# アクセスポリシーを作成
aws opensearchserverless create-access-policy \
  --name "${OPENSEARCH_COLLECTION_NAME}-access-policy" \
  --type data \
  --policy '[{
    "Rules": [
      {
        "Resource": ["collection/'${OPENSEARCH_COLLECTION_NAME}'"],
        "Permission": ["aoss:*"],
        "ResourceType": "collection"
      },
      {
        "Resource": ["index/'${OPENSEARCH_COLLECTION_NAME}'/*"],
        "Permission": ["aoss:*"],
        "ResourceType": "index"
      }
    ],
    "Principal": [
      "arn:aws:iam::'${AWS_ACCOUNT_ID}':role/'${KB_ROLE_NAME}'"
    ]
  }]'
```

### 3.3 OpenSearch Serverless Collection の作成

```shell
# OpenSearch Serverless Collectionを作成
aws opensearchserverless create-collection \
  --name $OPENSEARCH_COLLECTION_NAME \
  --type VECTORSEARCH \
  --description "Vector search collection for Bedrock Knowledge Base"

# Collection作成の完了を待つ
aws opensearchserverless batch-get-collection \
  --names $OPENSEARCH_COLLECTION_NAME
```

## Step 4: S3 Bucket の作成と設定

```shell
# S3バケットを作成
aws s3 mb s3://$S3_BUCKET_NAME --region $AWS_REGION

# バージョニングを有効化（推奨）
aws s3api put-bucket-versioning \
  --bucket $S3_BUCKET_NAME \
  --versioning-configuration Status=Enabled
```

## Step 5: Bedrock Knowledge Base の作成

```shell
# Knowledge Baseを作成（OpenSearch Serverless使用）
aws bedrock-agent create-knowledge-base \
  --name "MyKnowledgeBase" \
  --description "Auto-syncing Knowledge Base with S3 integration" \
  --role-arn "arn:aws:iam::${AWS_ACCOUNT_ID}:role/${KB_ROLE_NAME}" \
  --knowledge-base-configuration '{
    "type": "VECTOR",
    "vectorKnowledgeBaseConfiguration": {
      "embeddingModelArn": "arn:aws:bedrock:'${AWS_REGION}'::foundation-model/amazon.titan-embed-text-v1",
      "embeddingModelConfiguration": {
        "bedrockEmbeddingModelConfiguration": {
          "dimensions": 1536
        }
      }
    }
  }' \
  --storage-configuration '{
    "type": "OPENSEARCH_SERVERLESS",
    "opensearchServerlessConfiguration": {
      "collectionArn": "arn:aws:aoss:'${AWS_REGION}':'${AWS_ACCOUNT_ID}':collection/'${OPENSEARCH_COLLECTION_NAME}'",
      "vectorIndexName": "bedrock-knowledge-base-index",
      "fieldMapping": {
        "vectorField": "bedrock-knowledge-base-default-vector",
        "textField": "AMAZON_BEDROCK_TEXT_CHUNK",
        "metadataField": "AMAZON_BEDROCK_METADATA"
      }
    }
  }'

# 作成されたKnowledge BaseのIDを取得
export KNOWLEDGE_BASE_ID=$(aws bedrock-agent list-knowledge-bases \
  --query 'knowledgeBaseSummaries[?name==`MyKnowledgeBase`].knowledgeBaseId' \
  --output text)

echo "Created Knowledge Base ID: $KNOWLEDGE_BASE_ID"
```

## Step 6: S3 Data Source の作成

```shell
# S3データソースを作成
aws bedrock-agent create-data-source \
  --knowledge-base-id $KNOWLEDGE_BASE_ID \
  --name "S3DataSourceAutoSync" \
  --description "S3 data source with automatic sync on S3 events" \
  --data-source-configuration '{
    "type": "S3",
    "s3Configuration": {
      "bucketArn": "arn:aws:s3:::'${S3_BUCKET_NAME}'"
    }
  }' \
  --vector-ingestion-configuration '{
    "chunkingConfiguration": {
      "chunkingStrategy": "FIXED_SIZE",
      "fixedSizeChunkingConfiguration": {
        "maxTokens": 300,
        "overlapPercentage": 20
      }
    }
  }'

# 作成されたData SourceのIDを取得
export DATA_SOURCE_ID=$(aws bedrock-agent list-data-sources \
  --knowledge-base-id $KNOWLEDGE_BASE_ID \
  --query 'dataSourceSummaries[?name==`S3DataSourceAutoSync`].dataSourceId' \
  --output text)

echo "Created Data Source ID: $DATA_SOURCE_ID"
```

## Step 7: 自動同期のためのEventBridge設定

### 7.1 S3バケットのEventBridge統合設定

S3バケットでEventBridge通知を有効化します。

```shell
# EventBridge設定ファイルを作成
cat > s3-notification-config.json << EOF
{
  "EventBridgeConfiguration": {}
}
EOF

# S3バケットに通知設定を適用
aws s3api put-bucket-notification-configuration \
  --bucket $S3_BUCKET_NAME \
  --notification-configuration file://s3-notification-config.json
```

### 7.2 EventBridgeルールの作成

S3オブジェクト作成イベントをキャッチするEventBridgeルールを作成します。

```shell
# EventBridgeルールを作成
aws events put-rule \
  --name "BedrockKBAutoSync" \
  --description "Auto sync Bedrock Knowledge Base on S3 events" \
  --event-pattern '{
    "source": ["aws.s3"],
    "detail-type": ["Object Created"],
    "detail": {
      "bucket": {
        "name": ["'${S3_BUCKET_NAME}'"]
      }
    }
  }'
```

### 7.3 Lambda関数の作成

インジェストジョブを自動開始するLambda関数を作成します。

```python
# lambda_function.py
import json
import boto3
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    EventBridgeからのS3イベントを受信してBedrock Knowledge Baseのインジェストジョブを開始する
    """
    
    try:
        # イベントの検証
        if 'source' not in event or event['source'] != 'aws.s3':
            logger.warning("Invalid event source")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Invalid event source'})
            }
        
        # 環境変数から設定を読み込み
        knowledge_base_id = os.environ.get('KNOWLEDGE_BASE_ID')
        data_source_id = os.environ.get('DATA_SOURCE_ID')
        
        if not knowledge_base_id or not data_source_id:
            logger.error("Missing required environment variables")
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Configuration error'})
            }
        
        # Bedrock Agentクライアントを初期化
        bedrock_agent = boto3.client('bedrock-agent')
        
        logger.info(f"Starting ingestion job for KB: {knowledge_base_id}, DS: {data_source_id}")
        
        # S3イベントの詳細をログに記録
        if 'detail' in event and 'object' in event['detail']:
            object_key = event['detail']['object']['key']
            logger.info(f"Triggered by S3 object: {object_key}")
        
        # インジェストジョブを開始
        response = bedrock_agent.start_ingestion_job(
            knowledgeBaseId=knowledge_base_id,
            dataSourceId=data_source_id,
            description="Auto-triggered ingestion job from S3 event"
        )
        
        ingestion_job_id = response['ingestionJob']['ingestionJobId']
        logger.info(f"Ingestion job started successfully: {ingestion_job_id}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Ingestion job started successfully',
                'ingestionJobId': ingestion_job_id
            })
        }
        
    except Exception as e:
        logger.error(f"Error starting ingestion job: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }
```

### 7.4 Lambda実行ロールの作成

```shell
# Lambda関数用のロール名を設定
export LAMBDA_ROLE_NAME="BedrockKBLambdaRole"

# 権限ポリシーファイルを作成
cat > lambda-bedrock-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "bedrock-agent:StartIngestionJob",
        "bedrock-agent:GetIngestionJob",
        "bedrock-agent:ListIngestionJobs"
      ],
      "Resource": "*"
    }
  ]
}
EOF

# IAMロールを作成
aws iam create-role \
  --role-name $LAMBDA_ROLE_NAME \
  --assume-role-policy-document file://lambda-trust-policy.json

# ポリシーをロールに適用
aws iam put-role-policy \
  --role-name $LAMBDA_ROLE_NAME \
  --policy-name BedrockKBLambdaPolicy \
  --policy-document file://lambda-bedrock-policy.json

# AWSマネージドポリシーをアタッチ（Lambda基本実行権限）
aws iam attach-role-policy \
  --role-name $LAMBDA_ROLE_NAME \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
```

### 7.5 Lambda関数のデプロイ

```shell
# Lambda関数名を設定
export LAMBDA_FUNCTION_NAME="bedrock-kb-auto-sync"

# Lambda関数をパッケージ化
zip lambda_function.zip lambda_function.py

# Lambda関数を作成
aws lambda create-function \
  --function-name $LAMBDA_FUNCTION_NAME \
  --runtime python3.11 \
  --role "arn:aws:iam::${AWS_ACCOUNT_ID}:role/${LAMBDA_ROLE_NAME}" \
  --handler lambda_function.lambda_handler \
  --zip-file fileb://lambda_function.zip \
  --description "Auto-sync Bedrock Knowledge Base on S3 events" \
  --timeout 300 \
  --environment Variables="{KNOWLEDGE_BASE_ID=${KNOWLEDGE_BASE_ID},DATA_SOURCE_ID=${DATA_SOURCE_ID}}"

# Lambda関数のARNを取得
export LAMBDA_FUNCTION_ARN=$(aws lambda get-function \
  --function-name $LAMBDA_FUNCTION_NAME \
  --query 'Configuration.FunctionArn' \
  --output text)

echo "Lambda Function ARN: $LAMBDA_FUNCTION_ARN"
```

### 7.6 EventBridgeとLambda関数の連携

```shell
# EventBridgeルールにLambda関数をターゲットとして追加
aws events put-targets \
  --rule BedrockKBAutoSync \
  --targets "Id"="1","Arn"="${LAMBDA_FUNCTION_ARN}"

# EventBridgeがLambda関数を呼び出せるように権限を追加
aws lambda add-permission \
  --function-name $LAMBDA_FUNCTION_NAME \
  --statement-id allow-eventbridge \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn "arn:aws:events:${AWS_REGION}:${AWS_ACCOUNT_ID}:rule/BedrockKBAutoSync"
```

## Step 8: 自動化の動作確認

### 8.1 テストファイルのアップロード

```shell
# テストファイルを作成
echo "これは自動同期のテストドキュメントです。Knowledge Baseが正常に動作していることを確認します。" > test-auto-sync.txt

# S3にアップロード
aws s3 cp test-auto-sync.txt s3://$S3_BUCKET_NAME/

# インジェストジョブの状況を確認
aws bedrock-agent list-ingestion-jobs \
  --knowledge-base-id $KNOWLEDGE_BASE_ID \
  --data-source-id $DATA_SOURCE_ID \
  --max-results 3
```

### 8.2 ログの確認

```shell
# Lambda関数のログを確認
aws logs describe-log-groups \
  --log-group-name-prefix "/aws/lambda/$LAMBDA_FUNCTION_NAME"

# 最新のログストリームを取得
LOG_STREAM_NAME=$(aws logs describe-log-streams \
  --log-group-name "/aws/lambda/$LAMBDA_FUNCTION_NAME" \
  --order-by LastEventTime \
  --descending \
  --max-items 1 \
  --query 'logStreams[0].logStreamName' \
  --output text)

# ログイベントを表示
aws logs get-log-events \
  --log-group-name "/aws/lambda/$LAMBDA_FUNCTION_NAME" \
  --log-stream-name "$LOG_STREAM_NAME"
```

## Step 9: 自動化フローの概要

設定完了後、以下の自動化フローが動作します：

```
1. S3アップロード
   ↓
2. EventBridge通知（S3 → EventBridge）
   ↓
3. ルール適用（EventBridgeルールがイベントをキャッチ）
   ↓
4. Lambda呼び出し（Lambda関数が自動実行）
   ↓
5. インジェスト開始（Bedrock Knowledge Baseのインジェストジョブが自動開始）
   ↓
6. インデックス更新（新しいドキュメントが自動的にKnowledge Baseに追加）
```

## Step 10: Knowledge Baseの確認と運用

### 10.1 ステータス確認

```shell
# Knowledge Baseの状態確認
aws bedrock-agent get-knowledge-base --knowledge-base-id $KNOWLEDGE_BASE_ID

# データソースの状態確認
aws bedrock-agent get-data-source \
  --knowledge-base-id $KNOWLEDGE_BASE_ID \
  --data-source-id $DATA_SOURCE_ID

# 最新のインジェストジョブ確認
aws bedrock-agent list-ingestion-jobs \
  --knowledge-base-id $KNOWLEDGE_BASE_ID \
  --data-source-id $DATA_SOURCE_ID \
  --max-results 5
```

### 10.2 Knowledge Baseのクエリテスト

```shell
# Knowledge Baseに対してクエリを実行（例）
aws bedrock-agent-runtime retrieve-and-generate \
  --knowledge-base-id $KNOWLEDGE_BASE_ID \
  --input '{
    "text": "テストドキュメントの内容について教えてください"
  }' \
  --retrieve-and-generate-configuration '{
    "type": "KNOWLEDGE_BASE",
    "knowledgeBaseConfiguration": {
      "knowledgeBaseId": "'$KNOWLEDGE_BASE_ID'",
      "modelArn": "arn:aws:bedrock:'$AWS_REGION'::foundation-model/anthropic.claude-3-sonnet-20240229-v1:0"
    }
  }'
```

## トラブルシューティング

### よくある問題と解決方法

1. **インジェストジョブが開始されない**
   - EventBridgeルールの設定を確認
   - Lambda関数の権限を確認
   - CloudWatch Logsでエラーを確認

2. **OpenSearch Serverlessへの接続エラー**
   - アクセスポリシーの設定を確認
   - セキュリティポリシーの設定を確認

3. **権限エラー**
   - IAMロールの権限を確認
   - リソースベースのポリシーを確認

---

## まとめ

以上でAWS Bedrock Knowledge Baseの完全な設定が完了します。この設定により、S3にファイルをアップロードするだけで自動的にKnowledge Baseが更新される仕組みが構築されます。

各手順で作成されるリソースは環境変数で管理されているため、複数の環境での展開や、異なる設定での再利用が容易になっています。

運用時は定期的にインジェストジョブのステータスを確認し、必要に応じてLambda関数のログを監視することをお勧めします。
