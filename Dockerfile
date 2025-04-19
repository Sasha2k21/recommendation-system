FROM jupyter/pyspark-notebook:latest

# Set working directory
WORKDIR /home/project

# Copy files into container
COPY . /home/project

# Install any additional Python dependencies
RUN pip install --no-cache-dir -r requirements.txt
