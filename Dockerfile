FROM python:3.10-slim

# 设置时区环境变量为上海时区
ENV TZ=Asia/Shanghai

# 更新时区和安装 tzdata 包
RUN apt-get update && apt-get install -y tzdata

# 安装依赖
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    curl \
    unzip \
    libnss3 \
    libgdk-pixbuf2.0-0 \
    libgtk-3-0 \
    libxss1 \
    libasound2 \
    libxtst6 \
    fonts-liberation \
    libappindicator3-1 \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

# 下载 Google Chrome 的签名密钥并添加到可信密钥
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome-keyring.gpg

# 添加 Google Chrome 的官方源
RUN echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list

# 更新包列表并安装 Google Chrome
RUN apt-get update && apt-get install -y google-chrome-stable && rm -rf /var/lib/apt/lists/*

# 验证 Chrome 安装
RUN google-chrome --version

## 配置国内镜像源（这里以阿里云镜像为例）
#RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple

WORKDIR /crawlerapi
COPY . /crawlerapi
RUN pip config set global.index-url https://pypi.org/simple
# 安装依赖
RUN pip install --upgrade pip && \
    pip install --upgrade --default-timeout=100000 -r requirements.txt

#ENTRYPOINT ["python", "manage.py", "start", "all","-u","nginx"]
ENTRYPOINT ["python", "main.py"]
