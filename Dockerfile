FROM python:3.10

WORKDIR /app

COPY requirements.txt .

# Install alat build yang diperlukan untuk membangun dlib
RUN apt-get update && apt-get install -y \
    cmake \
    make \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]
