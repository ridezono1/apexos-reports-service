#!/bin/bash

# Reports Service Microservice Management Script
# This script manages the reports-service microservice within the monorepo

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Service configuration
SERVICE_NAME="reports-service"
SERVICE_PORT="8001"
SERVICE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MONOREPO_ROOT="$(cd "$SERVICE_DIR/.." && pwd)"

# Functions
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

# Check if we're in the right directory
check_directory() {
    if [[ ! -f "$SERVICE_DIR/app/main.py" ]]; then
        log_error "Not in reports-service directory or main.py not found"
        exit 1
    fi
}

# Install dependencies
install_deps() {
    log_info "Installing Python dependencies..."
    cd "$SERVICE_DIR"
    pip install -r requirements.txt
    log_success "Dependencies installed"
}

# Run tests
run_tests() {
    log_info "Running tests..."
    cd "$SERVICE_DIR"
    python -m pytest tests/ -v
    log_success "Tests completed"
}

# Start development server
start_dev() {
    log_info "Starting development server..."
    cd "$SERVICE_DIR"
    export ENVIRONMENT=development
    export DEBUG=true
    python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
}

# Start with Docker
start_docker() {
    log_info "Starting with Docker..."
    cd "$SERVICE_DIR"
    docker-compose -f docker-compose.microservice.yml up --build
}

# Stop Docker services
stop_docker() {
    log_info "Stopping Docker services..."
    cd "$SERVICE_DIR"
    docker-compose -f docker-compose.microservice.yml down
    log_success "Docker services stopped"
}

# Build Docker image
build_docker() {
    log_info "Building Docker image..."
    cd "$SERVICE_DIR"
    docker build -t "$SERVICE_NAME:latest" .
    log_success "Docker image built"
}

# Deploy to Heroku
deploy_heroku() {
    log_info "Deploying to Heroku..."
    cd "$MONOREPO_ROOT"
    
    # Check if Heroku remote exists
    if ! git remote | grep -q "heroku-reports"; then
        log_warning "Heroku remote not found. Please add it first:"
        log_warning "git remote add heroku-reports https://git.heroku.com/apexos-reports-service.git"
        exit 1
    fi
    
    # Deploy only the reports-service directory
    git subtree push --prefix=reports-service heroku-reports main
    log_success "Deployed to Heroku"
}

# Health check
health_check() {
    log_info "Checking service health..."
    if curl -f "http://localhost:$SERVICE_PORT/health" > /dev/null 2>&1; then
        log_success "Service is healthy"
    else
        log_error "Service is not responding"
        exit 1
    fi
}

# Show service status
status() {
    log_info "Service Status:"
    echo "  Name: $SERVICE_NAME"
    echo "  Port: $SERVICE_PORT"
    echo "  Directory: $SERVICE_DIR"
    echo "  Monorepo: $MONOREPO_ROOT"
    
    if pgrep -f "uvicorn.*reports-service" > /dev/null; then
        log_success "Development server is running"
    else
        log_warning "Development server is not running"
    fi
    
    if docker ps | grep -q "$SERVICE_NAME"; then
        log_success "Docker container is running"
    else
        log_warning "Docker container is not running"
    fi
}

# Show help
show_help() {
    echo "Reports Service Microservice Management"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  install     Install Python dependencies"
    echo "  test        Run tests"
    echo "  dev         Start development server"
    echo "  docker      Start with Docker"
    echo "  stop        Stop Docker services"
    echo "  build       Build Docker image"
    echo "  deploy      Deploy to Heroku"
    echo "  health      Check service health"
    echo "  status      Show service status"
    echo "  help        Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 dev      # Start development server"
    echo "  $0 docker   # Start with Docker"
    echo "  $0 deploy   # Deploy to Heroku"
}

# Main script logic
main() {
    check_directory
    
    case "${1:-help}" in
        install)
            install_deps
            ;;
        test)
            run_tests
            ;;
        dev)
            start_dev
            ;;
        docker)
            start_docker
            ;;
        stop)
            stop_docker
            ;;
        build)
            build_docker
            ;;
        deploy)
            deploy_heroku
            ;;
        health)
            health_check
            ;;
        status)
            status
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            log_error "Unknown command: $1"
            show_help
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"
