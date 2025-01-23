FROM python:3.10-slim

# 设置时区环境变量为上海时区
ENV TZ=Asia/Shanghai

# 更新时区和安装 tzdata 包
RUN apt-get update && apt-get install -y tzdata

## 配置国内镜像源（这里以阿里云镜像为例）
#RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple

WORKDIR /crawlerapi
COPY . /crawlerapi
RUN pip config set global.index-url https://pypi.org/simple
# 安装依赖
RUN pip install --no-cache-dir --no-deps --ignore-requires-python -r requirements.txt

#ENTRYPOINT ["python", "manage.py", "start", "all","-u","nginx"]
ENTRYPOINT ["python", "main.py"]
