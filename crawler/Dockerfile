# 指定一個 image 來源
# FROM python:3.8-slim-buster
FROM python:3.8.20-slim-bullseye AS base

# Install the dependencies
# build 時使用，會執行此命令
RUN apt-get update && \
apt-get -y install tzdata gcc build-essential git wget unzip bzip2 sudo ca-certificates && \
apt-get clean && \
rm -rf /var/lib/apt/lists/*

# Python environment
## prevent failing to install pycairo
RUN apt-get update && apt-get install -y pkg-config libcairo2-dev
## librosa dependencies for model FEMH
# RUN apt-get update && apt-get install -y libsndfile1
## opencv dependencies for model yolov4
# RUN apt-get update && apt-get install -y ffmpeg libsm6 libxext6 libgl1

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app 
# ADD：將檔案加到 images 內
ADD . /app

# RUN apt update && apt install python3.8-dev
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

CMD ["./deamon.sh"]
