#!/bin/bash

# Update the package repository
sudo apt-get update

# Install Docker
sudo apt-get install -y docker.io

# Start and automate Docker service
sudo systemctl start docker
sudo systemctl enable docker

# Install Docker Compose
# Replace [COMPOSE_VERSION] with the desired version
sudo curl -L "https://github.com/docker/compose/releases/download/v2.23.3/docker-compose-Linux-x86_64" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Verify installation
docker --version
docker-compose --version