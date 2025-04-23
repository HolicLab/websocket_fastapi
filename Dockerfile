FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    git \
    python3.10 \ 
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN pip install fastapi uvicorn aiohttp

WORKDIR /holic

CMD ["tail", "-f", "/dev/null"]

# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "18001", "--reload"]