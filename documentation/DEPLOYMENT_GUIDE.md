# Licence IQ Research Platform - Deployment Guide

## Deployment Overview
This guide covers the complete deployment process for the Licence IQ Research Platform from development to production.

## Environment Setup

### Development Environment (Current)
- **Platform:** Replit
- **Database:** Neon PostgreSQL
- **Runtime:** Node.js with npm
- **Build:** Vite development server
- **Storage:** Local filesystem

### Production Environment Requirements
- **Infrastructure:** Cloud platform (AWS, GCP, Azure)
- **Container:** Docker containerization
- **Database:** Managed PostgreSQL service
- **Storage:** Cloud object storage
- **CDN:** Global content delivery network

## Pre-Deployment Checklist

### ✅ Code Readiness
- [ ] All features tested and working
- [ ] No console errors or warnings
- [ ] TypeScript compilation successful
- [ ] Security vulnerabilities addressed
- [ ] Performance optimization completed

### ✅ Database Readiness
- [ ] Database schema finalized
- [ ] Migrations tested
- [ ] Indexes optimized
- [ ] Backup strategy implemented
- [ ] Connection pooling configured

### ✅ Security Hardening
- [ ] Environment variables secured
- [ ] API keys rotated for production
- [ ] Session security configured
- [ ] Input validation comprehensive
- [ ] File upload restrictions in place

### ✅ Monitoring Setup
- [ ] Application logging configured
- [ ] Error tracking implemented
- [ ] Performance monitoring active
- [ ] Health checks defined
- [ ] Alerting rules established

## Docker Configuration

### Dockerfile
```dockerfile
# Production Dockerfile
FROM node:18-alpine AS base

# Install dependencies only when needed
FROM base AS deps
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production

# Rebuild the source code only when needed
FROM base AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .

# Build client
RUN npm run build:client

# Production image
FROM base AS runner
WORKDIR /app

ENV NODE_ENV=production

RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 nextjs

# Copy built application
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/client/dist ./client/dist
COPY --from=deps /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./package.json

USER nextjs

EXPOSE 5000

ENV PORT 5000
ENV HOSTNAME "0.0.0.0"

CMD ["npm", "start"]
```

### Docker Compose (Development)
```yaml
version: '3.8'

services:
  app:
    build: .
    ports:
      - "5000:5000"
    environment:
      - NODE_ENV=development
      - DATABASE_URL=${DATABASE_URL}
      - GROQ_API_KEY=${GROQ_API_KEY}
      - SESSION_SECRET=${SESSION_SECRET}
    depends_on:
      - postgres
    volumes:
      - ./uploads:/app/uploads

  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: licenceiq
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

volumes:
  postgres_data:
```

## Cloud Deployment Options

### AWS Deployment

#### Infrastructure Components
```bash
# AWS Services Required
- EC2 or ECS for application hosting
- RDS PostgreSQL for database
- S3 for file storage
- CloudFront for CDN
- Application Load Balancer
- Route 53 for DNS
- CloudWatch for monitoring
```

#### Terraform Configuration
```hcl
# terraform/main.tf
provider "aws" {
  region = var.aws_region
}

# Application Load Balancer
resource "aws_lb" "app_lb" {
  name               = "licenceiq-lb"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.lb_sg.id]
  subnets           = var.public_subnets

  enable_deletion_protection = false
}

# ECS Cluster
resource "aws_ecs_cluster" "main" {
  name = "licenceiq-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

# RDS PostgreSQL
resource "aws_db_instance" "postgres" {
  identifier             = "licenceiq-postgres"
  engine                 = "postgres"
  engine_version         = "15.3"
  instance_class         = "db.t3.micro"
  allocated_storage      = 20
  storage_encrypted      = true
  
  db_name  = var.db_name
  username = var.db_username
  password = var.db_password
  
  vpc_security_group_ids = [aws_security_group.rds_sg.id]
  db_subnet_group_name   = aws_db_subnet_group.postgres.name
  
  backup_retention_period = 7
  backup_window          = "03:00-04:00"
  maintenance_window     = "sun:04:00-sun:05:00"
  
  skip_final_snapshot = true
}

# S3 Bucket for file storage
resource "aws_s3_bucket" "files" {
  bucket = "licenceiq-files-${random_string.bucket_suffix.result}"
}

resource "aws_s3_bucket_versioning" "files" {
  bucket = aws_s3_bucket.files.id
  versioning_configuration {
    status = "Enabled"
  }
}
```

### Google Cloud Platform Deployment

#### Infrastructure Components
```bash
# GCP Services Required
- Cloud Run for containerized application
- Cloud SQL for PostgreSQL
- Cloud Storage for file storage
- Cloud CDN for global delivery
- Cloud Load Balancing
- Cloud DNS for domain management
- Cloud Monitoring and Logging
```

#### GCP Configuration
```yaml
# cloudbuild.yaml
steps:
  # Build the container image
  - name: 'gcr.io/cloud-builders/docker'
    args: ['build', '-t', 'gcr.io/$PROJECT_ID/licenceiq:$COMMIT_SHA', '.']
  
  # Push the container image to Container Registry
  - name: 'gcr.io/cloud-builders/docker'
    args: ['push', 'gcr.io/$PROJECT_ID/licenceiq:$COMMIT_SHA']
  
  # Deploy container image to Cloud Run
  - name: 'gcr.io/cloud-builders/gcloud'
    args:
    - 'run'
    - 'deploy'
    - 'licenceiq'
    - '--image'
    - 'gcr.io/$PROJECT_ID/licenceiq:$COMMIT_SHA'
    - '--region'
    - 'us-central1'
    - '--platform'
    - 'managed'
    - '--allow-unauthenticated'
```

### Azure Deployment

#### Infrastructure Components
```bash
# Azure Services Required
- Azure Container Instances or App Service
- Azure Database for PostgreSQL
- Azure Blob Storage for file storage
- Azure CDN
- Azure Load Balancer
- Azure DNS
- Azure Monitor
```

## Environment Variables Configuration

### Production Environment Variables
```bash
# Application Configuration
NODE_ENV=production
PORT=5000
HOSTNAME=0.0.0.0

# Database Configuration
DATABASE_URL=postgresql://user:password@host:5432/database
PGHOST=your-postgres-host
PGPORT=5432
PGUSER=your-postgres-user
PGPASSWORD=your-postgres-password
PGDATABASE=your-database-name

# AI Service Configuration
GROQ_API_KEY=your-groq-api-key

# Security Configuration
SESSION_SECRET=your-super-secure-session-secret-minimum-32-chars

# Storage Configuration (Cloud)
AWS_ACCESS_KEY_ID=your-aws-access-key
AWS_SECRET_ACCESS_KEY=your-aws-secret-key
AWS_S3_BUCKET=your-s3-bucket-name
AWS_REGION=us-east-1

# Monitoring Configuration
SENTRY_DSN=your-sentry-dsn
LOG_LEVEL=info
```

### Environment Variable Security
```bash
# Use AWS Secrets Manager
aws secretsmanager create-secret \
  --name licenceiq/production/database \
  --description "Database credentials for Licence IQ" \
  --secret-string '{"username":"dbuser","password":"securepassword"}'

# Use Azure Key Vault
az keyvault secret set \
  --vault-name licenceiq-keyvault \
  --name database-password \
  --value "securepassword"

# Use Google Secret Manager
gcloud secrets create database-password --data-file=password.txt
```

## Database Migration Strategy

### Production Migration Process
```bash
# 1. Create database backup
pg_dump $DATABASE_URL > backup_$(date +%Y%m%d_%H%M%S).sql

# 2. Run migrations in transaction
npm run db:push

# 3. Verify migration success
npm run db:verify

# 4. Update application version
git tag v1.0.0-production
```

### Migration Rollback Plan
```sql
-- Rollback script template
BEGIN;

-- Drop new columns/tables if needed
-- ALTER TABLE contracts DROP COLUMN IF EXISTS new_column;

-- Restore from backup if necessary
-- \i backup_20240101_120000.sql

COMMIT;
```

## Monitoring and Observability

### Application Monitoring
```javascript
// monitoring/setup.js
const Sentry = require('@sentry/node');
const prometheus = require('prom-client');

// Initialize Sentry for error tracking
Sentry.init({
  dsn: process.env.SENTRY_DSN,
  environment: process.env.NODE_ENV,
});

// Prometheus metrics
const httpDuration = new prometheus.Histogram({
  name: 'http_request_duration_seconds',
  help: 'Duration of HTTP requests in seconds',
  labelNames: ['method', 'route', 'status_code'],
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'healthy',
    timestamp: new Date().toISOString(),
    version: process.env.npm_package_version,
  });
});
```

### Log Configuration
```javascript
// logging/winston.js
const winston = require('winston');

const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: 'error.log', level: 'error' }),
    new winston.transports.File({ filename: 'combined.log' }),
    new winston.transports.Console({
      format: winston.format.simple()
    })
  ],
});
```

## SSL/TLS Configuration

### Let's Encrypt Setup
```bash
# Install Certbot
sudo apt-get install certbot python3-certbot-nginx

# Obtain SSL certificate
sudo certbot --nginx -d yourdomain.com -d www.yourdomain.com

# Auto-renewal
sudo crontab -e
# Add line: 0 12 * * * /usr/bin/certbot renew --quiet
```

### Nginx Configuration
```nginx
server {
    listen 80;
    server_name yourdomain.com www.yourdomain.com;
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    server_name yourdomain.com www.yourdomain.com;

    ssl_certificate /etc/letsencrypt/live/yourdomain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/yourdomain.com/privkey.pem;
    
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers ECDHE-RSA-AES256-GCM-SHA512:DHE-RSA-AES256-GCM-SHA512;
    ssl_prefer_server_ciphers off;
    ssl_session_cache shared:SSL:10m;

    location / {
        proxy_pass http://localhost:5000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_cache_bypass $http_upgrade;
    }
}
```

## Backup and Disaster Recovery

### Database Backup Strategy
```bash
#!/bin/bash
# backup_script.sh

DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/backups/postgresql"
RETENTION_DAYS=30

# Create backup
pg_dump $DATABASE_URL | gzip > $BACKUP_DIR/backup_$DATE.sql.gz

# Upload to cloud storage
aws s3 cp $BACKUP_DIR/backup_$DATE.sql.gz s3://licenceiq-backups/

# Clean old backups
find $BACKUP_DIR -name "backup_*.sql.gz" -mtime +$RETENTION_DAYS -delete

echo "Backup completed: backup_$DATE.sql.gz"
```

### Disaster Recovery Plan
1. **Database Recovery:** Restore from latest backup
2. **Application Recovery:** Deploy from version control
3. **File Recovery:** Restore from cloud storage backup
4. **DNS Failover:** Switch to backup infrastructure
5. **Communication:** Notify stakeholders of status

## Performance Optimization

### Production Optimizations
```javascript
// production optimizations
const compression = require('compression');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');

// Enable compression
app.use(compression());

// Security headers
app.use(helmet());

// Rate limiting
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100 // limit each IP to 100 requests per windowMs
});
app.use('/api', limiter);

// Database connection pooling
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  min: 2,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});
```

## Deployment Checklist

### Pre-Deployment
- [ ] Code review completed
- [ ] All tests passing
- [ ] Security scan completed
- [ ] Performance testing done
- [ ] Database migration prepared
- [ ] Backup verified
- [ ] Rollback plan ready

### During Deployment
- [ ] Maintenance mode enabled
- [ ] Database migration executed
- [ ] Application deployed
- [ ] Health checks passing
- [ ] SSL certificates valid
- [ ] CDN cache cleared

### Post-Deployment
- [ ] Functionality testing completed
- [ ] Performance monitoring active
- [ ] Error rates within normal range
- [ ] User acceptance testing passed
- [ ] Documentation updated
- [ ] Team notified of deployment