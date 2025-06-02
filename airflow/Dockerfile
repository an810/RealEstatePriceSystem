FROM apache/airflow:2.10.5-python3.10

USER root

# Install Chromium and dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl \
    chromium \
    chromium-driver \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables for Selenium to use Chromium
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMIUM_PATH=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver
ENV CHROMIUM_FLAGS="--headless --no-sandbox --disable-dev-shm-usage"

# Create a symbolic link for Chrome binary to make it easier to find
RUN ln -sf /usr/bin/chromium /usr/bin/google-chrome

USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    && pip install --no-cache-dir undetected-chromedriver==3.5.5 