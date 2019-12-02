# Use an official Python runtime as a parent image
# FROM python:2.7-slim
FROM ubuntu 

# Set the working directory to /project
WORKDIR /project

# Copy the current directory contents into the container at /app
ADD requirements.txt /project

# install additional stuffs
RUN apt-get update
RUN apt-get install vim -y 
RUN apt-get install python -y
RUN apt-get install python-pip -y

# Install any needed packages specified in requirements.txt
RUN pip install --upgrade pip
RUN pip install --trusted-host pypi.python.org -r /project/requirements.txt

CMD ["/bin/bash"]
