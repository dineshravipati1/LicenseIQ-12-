#!/bin/bash

# LicenseIQ Azure Deployment Script
# This script automates the Azure deployment process

set -e  # Exit on any error

# Configuration - MODIFY THESE VALUES
RESOURCE_GROUP="licenseiq-prod"
LOCATION="East US"
APP_NAME="licenseiq-app"
DB_SERVER_NAME="licenseiq-db-server"
DB_ADMIN_USER="licenseiqadmin"
DB_ADMIN_PASSWORD="YourSecurePassword123!"  # CHANGE THIS!
SESSION_SECRET="your-super-secure-session-secret-here"  # CHANGE THIS!
GROQ_API_KEY=""  # SET YOUR GROQ API KEY HERE

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if Azure CLI is installed
    if ! command -v az &> /dev/null; then
        log_error "Azure CLI is not installed. Please install it first."
        exit 1
    fi
    
    # Check if user is logged in
    if ! az account show &> /dev/null; then
        log_error "Not logged in to Azure. Please run 'az login' first."
        exit 1
    fi
    
    # Check if required variables are set
    if [ -z "$GROQ_API_KEY" ]; then
        log_error "GROQ_API_KEY is not set. Please update the script with your API key."
        exit 1
    fi
    
    log_success "Prerequisites check passed!"
}

# Create Azure resources
create_resources() {
    log_info "Creating Azure resources..."
    
    # Create resource group
    log_info "Creating resource group: $RESOURCE_GROUP"
    az group create --name "$RESOURCE_GROUP" --location "$LOCATION" --output table
    
    # Create App Service Plan
    log_info "Creating App Service Plan..."
    az appservice plan create \
        --name "${APP_NAME}-plan" \
        --resource-group "$RESOURCE_GROUP" \
        --location "$LOCATION" \
        --sku P1V3 \
        --is-linux \
        --output table
    
    log_success "Azure resources created successfully!"
}

# Setup PostgreSQL database
setup_database() {
    log_info "Setting up PostgreSQL Flexible Server..."
    
    # Create PostgreSQL server
    log_info "Creating PostgreSQL server: $DB_SERVER_NAME"
    az postgres flexible-server create \
        --name "$DB_SERVER_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --location "$LOCATION" \
        --admin-user "$DB_ADMIN_USER" \
        --admin-password "$DB_ADMIN_PASSWORD" \
        --sku-name Standard_B2s \
        --tier Burstable \
        --storage-size 128 \
        --version 14 \
        --public-access 0.0.0.0 \
        --output table
    
    # Create database
    log_info "Creating application database..."
    az postgres flexible-server db create \
        --resource-group "$RESOURCE_GROUP" \
        --server-name "$DB_SERVER_NAME" \
        --database-name licenseiq \
        --output table
    
    # Configure firewall
    log_info "Configuring firewall rules..."
    az postgres flexible-server firewall-rule create \
        --resource-group "$RESOURCE_GROUP" \
        --name "$DB_SERVER_NAME" \
        --rule-name AllowAzureServices \
        --start-ip-address 0.0.0.0 \
        --end-ip-address 0.0.0.0 \
        --output table
    
    log_success "PostgreSQL setup completed!"
}

# Create and configure web app
setup_webapp() {
    log_info "Creating and configuring web app..."
    
    # Create web app
    log_info "Creating web app: $APP_NAME"
    az webapp create \
        --resource-group "$RESOURCE_GROUP" \
        --plan "${APP_NAME}-plan" \
        --name "$APP_NAME" \
        --runtime "NODE:20-lts" \
        --deployment-local-git \
        --output table
    
    # Configure Node.js version
    log_info "Configuring Node.js version..."
    az webapp config appsettings set \
        --resource-group "$RESOURCE_GROUP" \
        --name "$APP_NAME" \
        --settings WEBSITE_NODE_DEFAULT_VERSION=20.11.0 \
        --output table
    
    # Set startup command
    log_info "Setting startup command..."
    az webapp config set \
        --resource-group "$RESOURCE_GROUP" \
        --name "$APP_NAME" \
        --startup-file "npm start" \
        --output table
    
    log_success "Web app setup completed!"
}

# Configure environment variables
configure_environment() {
    log_info "Configuring environment variables..."
    
    # Construct database URL
    DATABASE_URL="postgresql://${DB_ADMIN_USER}:${DB_ADMIN_PASSWORD}@${DB_SERVER_NAME}.postgres.database.azure.com:5432/licenseiq?sslmode=require"
    
    # Set environment variables
    az webapp config appsettings set \
        --resource-group "$RESOURCE_GROUP" \
        --name "$APP_NAME" \
        --settings \
            NODE_ENV=production \
            PORT=8080 \
            DATABASE_URL="$DATABASE_URL" \
            SESSION_SECRET="$SESSION_SECRET" \
            GROQ_API_KEY="$GROQ_API_KEY" \
        --output table
    
    log_success "Environment variables configured!"
}

# Enable HTTPS and security features
configure_security() {
    log_info "Configuring security features..."
    
    # Enable HTTPS only
    az webapp update \
        --resource-group "$RESOURCE_GROUP" \
        --name "$APP_NAME" \
        --https-only true \
        --output table
    
    # Enable system-assigned managed identity
    az webapp identity assign \
        --resource-group "$RESOURCE_GROUP" \
        --name "$APP_NAME" \
        --output table
    
    log_success "Security features configured!"
}

# Setup monitoring
setup_monitoring() {
    log_info "Setting up Application Insights..."
    
    # Create Application Insights
    az monitor app-insights component create \
        --app licenseiq-insights \
        --location "$LOCATION" \
        --resource-group "$RESOURCE_GROUP" \
        --kind web \
        --output table
    
    # Get instrumentation key
    INSIGHTS_KEY=$(az monitor app-insights component show \
        --app licenseiq-insights \
        --resource-group "$RESOURCE_GROUP" \
        --query "instrumentationKey" \
        --output tsv)
    
    # Configure App Service to use Application Insights
    az webapp config appsettings set \
        --resource-group "$RESOURCE_GROUP" \
        --name "$APP_NAME" \
        --settings APPINSIGHTS_INSTRUMENTATIONKEY="$INSIGHTS_KEY" \
        --output table
    
    # Enable application logging
    az webapp log config \
        --resource-group "$RESOURCE_GROUP" \
        --name "$APP_NAME" \
        --application-logging true \
        --level information \
        --output table
    
    log_success "Monitoring setup completed!"
}

# Deploy application
deploy_application() {
    log_info "Preparing application for deployment..."
    
    # Build the application
    log_info "Building application..."
    npm run build
    
    # Get Git deployment URL
    log_info "Getting Git deployment URL..."
    GITURL=$(az webapp deployment source config-local-git \
        --name "$APP_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --query url --output tsv)
    
    log_info "Git deployment URL: $GITURL"
    log_warning "To deploy your code, run the following commands:"
    echo ""
    echo "git remote add azure $GITURL"
    echo "git push azure main"
    echo ""
    
    log_success "Application deployment configuration completed!"
}

# Post-deployment verification
verify_deployment() {
    log_info "Performing post-deployment verification..."
    
    # Get app URL
    APP_URL="https://${APP_NAME}.azurewebsites.net"
    
    # Check app status
    APP_STATE=$(az webapp show \
        --resource-group "$RESOURCE_GROUP" \
        --name "$APP_NAME" \
        --query "state" \
        --output tsv)
    
    log_info "App state: $APP_STATE"
    
    if [ "$APP_STATE" = "Running" ]; then
        log_success "Application is running!"
        log_info "Application URL: $APP_URL"
        log_info "Kudu URL: https://${APP_NAME}.scm.azurewebsites.net"
    else
        log_warning "Application is not running. Check the logs."
    fi
    
    # Show next steps
    echo ""
    log_info "=== NEXT STEPS ==="
    echo "1. Deploy your code using Git:"
    echo "   git remote add azure $GITURL"
    echo "   git push azure main"
    echo ""
    echo "2. Run database migrations:"
    echo "   az webapp ssh --resource-group $RESOURCE_GROUP --name $APP_NAME"
    echo "   cd /home/site/wwwroot && npm run db:push --force"
    echo ""
    echo "3. Test your application:"
    echo "   Open: $APP_URL"
    echo ""
    echo "4. Monitor logs:"
    echo "   az webapp log tail --resource-group $RESOURCE_GROUP --name $APP_NAME"
    echo ""
}

# Cleanup function
cleanup() {
    log_warning "To delete all created resources, run:"
    echo "az group delete --name $RESOURCE_GROUP --yes --no-wait"
}

# Main execution
main() {
    echo "=================================="
    echo "LicenseIQ Azure Deployment Script"
    echo "=================================="
    echo ""
    
    log_info "Starting deployment to Azure..."
    echo "Resource Group: $RESOURCE_GROUP"
    echo "Location: $LOCATION"
    echo "App Name: $APP_NAME"
    echo "Database Server: $DB_SERVER_NAME"
    echo ""
    
    # Confirm before proceeding
    read -p "Do you want to proceed with the deployment? (y/N): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Deployment cancelled by user."
        exit 0
    fi
    
    # Execute deployment steps
    check_prerequisites
    create_resources
    setup_database
    setup_webapp
    configure_environment
    configure_security
    setup_monitoring
    deploy_application
    verify_deployment
    
    log_success "Deployment script completed successfully!"
    log_info "Your LicenseIQ application is ready for code deployment."
    
    # Show cleanup info
    cleanup
}

# Run main function
main "$@"