# Use Python 3.9 as the base image
FROM ubuntu:latest

# Install required dependencies
RUN apt-get update --fix-missing && \
    apt-get install -y software-properties-common &&\
    add-apt-repository ppa:openjdk-r/ppa && \
    apt-get update && \
    apt-get install wget -y && \
    apt-get install -y python3 && \
    apt-get install python3-pip -y && \
    apt install openjdk-11-jdk -y && \
    wget https://dlcdn.apache.org/spark/spark-3.4.3/spark-3.4.3-bin-hadoop3.tgz && \
    tar xvf spark-3.4.3-bin-hadoop3.tgz && \
    mv spark-3.4.3-bin-hadoop3 /opt/spark && \
    export SPARK_HOME=/opt/spark && \
    export PATH=$PATH:$SPARK_HOME/bin

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install the required packages
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt 

# Copy the Python script into the container
COPY main.py pipeline.py ./

# Copy the data directory into the container (uncomment this if you are using local data)
# COPY data ./data

# Set the command to run the script
ENTRYPOINT  ["python3", "main.py"]
