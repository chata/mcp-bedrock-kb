# Amazon Bedrock Knowledge Base MCP Server

An MCP (Model Context Protocol) server for Amazon Bedrock Knowledge Base with full CRUD operations support. This server enables AI assistants to interact with Bedrock Knowledge Bases for searching, querying, and managing documents.

## Features

### Search & Query Operations
- **Search Knowledge Base**: Semantic and hybrid search capabilities
- **RAG Query**: Generate answers using Retrieval-Augmented Generation
- **List Knowledge Bases**: Browse available knowledge bases

### Document Management (CRUD)
- **Upload Documents**: Add new text documents or files to knowledge base
- **Update Documents**: Modify existing documents
- **Delete Documents**: Remove documents from knowledge base
- **List Documents**: Browse documents in S3
- **Sync Data Sources**: Trigger ingestion jobs for data synchronization

## Installation

### Prerequisites
- Python 3.10 or higher (3.13 recommended)
- AWS Account with Bedrock Knowledge Base configured
- Appropriate IAM permissions (see below)

### Quick Install with uvx (Recommended)

```bash
# Install and run directly from GitHub
uvx --from git+https://github.com/chata/mcp-bedrock-kb.git bedrock-kb-mcp

# Or install to use repeatedly
uvx --from git+https://github.com/chata/mcp-bedrock-kb.git --spec bedrock-kb-mcp bedrock-kb-mcp
```

### Install from source

```bash
# Clone the repository
git clone https://github.com/chata/mcp-bedrock-kb.git
cd mcp-bedrock-kb

# Install dependencies (Python 3.10+ required)
python3.13 -m pip install -e .
# or with your preferred Python version
pip install -e .
```

### Install via pip

```bash
pip install git+https://github.com/chata/mcp-bedrock-kb.git
```

## Configuration

### AWS Credentials

The server supports multiple authentication methods:

1. **AWS SSO Profile** (Recommended)
```yaml
aws:
  profile: your-sso-profile
  region: us-east-1
```

2. **Environment Variables**
```bash
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_REGION=us-east-1
```

3. **IAM Role** (for EC2/Lambda)
```yaml
aws:
  use_iam_role: true
  region: us-east-1
```

### Configuration File

Create a `config.yaml` file:

```yaml
aws:
  region: us-east-1
  profile: null  # AWS SSO profile name
  use_iam_role: true

bedrock:
  default_model: "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-sonnet-20240229-v1:0"
  default_kb_id: null
  
s3:
  default_bucket: null  # Will auto-detect from Knowledge Base
  upload_prefix: "documents/"
  
document_processing:
  supported_formats: ["txt", "md", "html", "pdf", "docx"]
  max_file_size_mb: 50
  encoding: "utf-8"
  
logging:
  level: INFO
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  file: null
```

### Environment Variables

You can override configuration via environment variables. Environment variables take precedence over configuration files but are overridden by explicit configuration settings.

#### AWS Configuration Variables

```bash
# AWS Authentication
export AWS_PROFILE=your-sso-profile              # AWS SSO profile to use
export AWS_REGION=us-east-1                      # Primary AWS region setting
export AWS_DEFAULT_REGION=us-west-2              # Fallback region if AWS_REGION not set
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE    # Access key for programmatic access
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI...    # Secret key for programmatic access
export AWS_SESSION_TOKEN=AQoEXAMPLEH4aoAH0g...   # Session token for temporary credentials
export AWS_USE_IAM_ROLE=true                     # Use IAM role instead of credentials
```

#### Bedrock Configuration Variables

```bash
export BEDROCK_DEFAULT_MODEL="arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-sonnet-20240229-v1:0"
export BEDROCK_DEFAULT_KB_ID=KB123456789         # Default Knowledge Base ID
```

#### S3 Configuration Variables

```bash
export S3_DEFAULT_BUCKET=my-kb-bucket            # Default S3 bucket for documents
export S3_UPLOAD_PREFIX=documents/               # Prefix for uploaded documents
```

#### Document Processing Variables

```bash
export DOC_MAX_FILE_SIZE_MB=50                   # Maximum file size in MB
export DOC_ENCODING=utf-8                        # Default text encoding
```

#### Logging Configuration Variables

```bash
export LOG_LEVEL=INFO                            # Logging level (DEBUG, INFO, WARNING, ERROR)
export LOG_FILE=/path/to/logfile.log             # Log to file instead of console
```

#### Variable Priority

The configuration system uses the following priority order (highest to lowest):

1. **Explicit configuration values** (set via config file or direct API calls)
2. **Environment variables** (listed above)  
3. **Default values** (built-in defaults)

For AWS regions specifically: `AWS_REGION` > `AWS_DEFAULT_REGION` > default (`us-east-1`)

## Required IAM Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "bedrock:InvokeModel",
        "bedrock:Retrieve",
        "bedrock:ListKnowledgeBases",
        "bedrock:GetKnowledgeBase"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow", 
      "Action": [
        "bedrock:ListDataSources",
        "bedrock:GetDataSource",
        "bedrock:StartIngestionJob",
        "bedrock:GetIngestionJob",
        "bedrock:ListIngestionJobs"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject", 
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-knowledge-base-bucket",
        "arn:aws:s3:::your-knowledge-base-bucket/*"
      ]
    }
  ]
}
```

## Usage with MCP Clients

### Claude Desktop Configuration

#### Option 1: Using uvx (Recommended)

Add to your Claude Desktop configuration (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "bedrock-kb": {
      "command": "uvx",
      "args": [
        "--from", 
        "git+https://github.com/chata/mcp-bedrock-kb.git",
        "bedrock-kb-mcp"
      ],
      "env": {
        "AWS_REGION": "us-east-1",
        "BEDROCK_DEFAULT_KB_ID": "your-kb-id"
      }
    }
  }
}
```

#### Option 2: Using installed package

If you've already installed the package:

```json
{
  "mcpServers": {
    "bedrock-kb": {
      "command": "bedrock-kb-mcp",
      "env": {
        "AWS_REGION": "us-east-1",
        "BEDROCK_DEFAULT_KB_ID": "your-kb-id"
      }
    }
  }
}
```

#### Option 3: Using Python module

```json
{
  "mcpServers": {
    "bedrock-kb": {
      "command": "python",
      "args": ["-m", "bedrock_kb_mcp.server"],
      "env": {
        "AWS_PROFILE": "your-profile",
        "AWS_REGION": "us-east-1"
      }
    }
  }
}
```

### Usage Examples

Once configured, you can interact with the Knowledge Base through your MCP client:

```python
# Search for information
await mcp_client.call_tool("bedrock_kb_search", {
    "knowledge_base_id": "KB123456789",
    "query": "AWS Lambda pricing",
    "num_results": 10,
    "search_type": "HYBRID"
})

# Generate answer with RAG
await mcp_client.call_tool("bedrock_kb_query", {
    "knowledge_base_id": "KB123456789",
    "question": "How does AWS Lambda pricing work?",
    "temperature": 0.1,
    "max_tokens": 2000
})

# Upload a document
await mcp_client.call_tool("bedrock_kb_upload_document", {
    "knowledge_base_id": "KB123456789",
    "document_content": "AWS Lambda charges you based on...",
    "document_name": "lambda-pricing-guide.md",
    "document_format": "md",
    "metadata": {
        "category": "pricing",
        "product": "lambda",
        "last_updated": "2024-01-01"
    }
})

# Upload a file
await mcp_client.call_tool("bedrock_kb_upload_file", {
    "knowledge_base_id": "KB123456789",
    "file_path": "/path/to/document.pdf",
    "s3_key": "guides/user-manual.pdf",
    "metadata": {"type": "manual"}
})

# Update existing document
await mcp_client.call_tool("bedrock_kb_update_document", {
    "knowledge_base_id": "KB123456789",
    "document_s3_key": "documents/lambda-guide.md",
    "new_content": "Updated content here...",
    "metadata": {"version": "2.0"}
})

# Delete document
await mcp_client.call_tool("bedrock_kb_delete_document", {
    "knowledge_base_id": "KB123456789",
    "document_s3_key": "documents/old-guide.txt"
})

# List documents
await mcp_client.call_tool("bedrock_kb_list_documents", {
    "knowledge_base_id": "KB123456789",
    "prefix": "guides/",
    "max_items": 50
})

# Sync data source
await mcp_client.call_tool("bedrock_kb_sync_datasource", {
    "knowledge_base_id": "KB123456789",
    "data_source_id": "DS123456789",
    "description": "Daily sync"
})

# Check sync status
await mcp_client.call_tool("bedrock_kb_get_sync_status", {
    "knowledge_base_id": "KB123456789",
    "data_source_id": "DS123456789"
})
```

## Development

### Running Tests

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run with coverage
pytest --cov=src/bedrock_kb_mcp

# Run specific test file
pytest tests/test_utils.py
```

### Code Quality

```bash
# Format code
black src/ tests/

# Lint code
ruff check src/ tests/

# Type checking
mypy src/
```

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Verify AWS credentials are configured
   - Check IAM permissions
   - Ensure correct region is set

2. **Knowledge Base Not Found**
   - Verify Knowledge Base ID is correct
   - Check region matches where KB was created
   - Ensure you have access permissions

3. **S3 Access Denied**
   - Check S3 bucket permissions
   - Verify bucket exists
   - Ensure IAM role has S3 access

4. **File Upload Failures**
   - Check file size limits
   - Verify file format is supported
   - Ensure S3 bucket is writable

### Debug Mode

Enable debug logging:

```yaml
logging:
  level: DEBUG
  file: /path/to/debug.log
```

Or via environment:

```bash
export LOG_LEVEL=DEBUG
```

## Architecture

```
bedrock-kb-mcp/
├── src/bedrock_kb_mcp/
│   ├── server.py          # MCP server implementation
│   ├── bedrock_client.py  # Bedrock API operations
│   ├── s3_manager.py      # S3 file operations
│   ├── auth_manager.py    # AWS authentication
│   ├── config_manager.py  # Configuration management
│   └── utils.py           # Utility functions
├── tests/                 # Unit tests
├── examples/              # Usage examples
└── config.yaml           # Configuration file
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
- Open an issue on GitHub
- Check existing issues for solutions
- Review the examples directory

## Acknowledgments

- Built on the [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)
- Uses AWS Bedrock Knowledge Base service
- Inspired by the MCP ecosystem
