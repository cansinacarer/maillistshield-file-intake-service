FROM python:3.13-slim
LABEL maintainer="Cansin Acarer https://cacarer.com"

# Update package list and install whois and other dependencies
RUN apt-get -y update && \
    apt-get install --no-install-recommends -y whois && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy the application files
ADD . /my-app
WORKDIR /my-app

# Install the requirements
RUN pip install -r requirements.txt

# Make sure the messages reach the console
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["python","main.py"]