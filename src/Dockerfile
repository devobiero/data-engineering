# Base image
FROM python:3.10

# Setting the working directory to /app
WORKDIR /app

# Copy the current directory  into the image
COPY . ./

# Installing the required libraries
RUN pip install --no-cache-dir -r requirements.txt
