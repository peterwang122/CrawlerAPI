import asyncio
import os
from datetime import datetime

from sanic import Sanic
from sanic.response import json as json_sanic
import json
from sanic.request import Request
import time
import hashlib
from log.logger_config import logger
import json as json_lib
import atexit
import smtplib
import redis
from config import REDIS_CONFIG
from util.list_api import list_api
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


app = Sanic(__name__)
redis_client = redis.Redis(db=12,**REDIS_CONFIG)
# 验证函数
def verify_request(token, timestamp, secret_key):
    # 计算token
    calculated_token = hashlib.sha256((secret_key + str(timestamp) + secret_key).encode('utf-8')).hexdigest()
    return token == calculated_token


@app.before_server_start
async def setup(app, loop):
    app.config.REQUEST_TIMEOUT = 1800  # 设置请求超时时间为120秒
    app.config.RESPONSE_TIMEOUT = 1800  # 设置请求超时时间为120秒

@app.exception(Exception)
async def handle_exception(request, exception):
    print(f"An error occurred: {exception}")
    import traceback
    full_stack =traceback.print_exc()
    return json_sanic({"error": str(exception)}, status=500)


# 用于顺序执行任务的消费者任务
# 用于顺序执行任务的消费者任务
async def task_runner():
    while True:
        # 从 Redis 队列中获取任务
        task = redis_client.lpop('task_queue')
        if task is None:
            await asyncio.sleep(1)  # 如果队列为空，等待 1 秒后重试
            continue

        # 解析任务数据
        data = json.loads(task)
        print(f"Dequeued task: {data}")

        # 执行任务
        await list_api(data)

        # 标记任务完成
        print(f"Task completed: {data}")

# 在应用启动时加载未完成的任务
@app.before_server_start
async def setup(app, loop):
    # 启动任务处理的消费者
    loop.create_task(task_runner())

@app.route('/api/data/list', methods=['POST'])
async def handle_list(request: Request):
    # 获取当前的日期和时间
    current_time = datetime.now()

    # 打印当前时间（默认格式：年-月-日 时:分:秒.毫秒）
    print(current_time)
    # 获取请求头和请求体
    token = request.headers.get('token')
    timestamp = request.headers.get('timestamp')
    data = request.json

    # 验证请求头
    secret_key = "69c5fcebaa65b560eaf06c3fbeb481ae44b8d618"  # 测试环境的秘钥, 根据环境配置选择秘钥
    if not verify_request(token, timestamp, secret_key):
        return json_sanic({"error": "Unauthorized"}, status=401)

    if not data.get("text") or data["text"] == "":
        return json_sanic({"status": 404, "error": "The 'text' field cannot be an empty string."})
    # 将任务放入 Redis 队列
    redis_client.rpush('task_queue', json.dumps(data))
    print(f"Enqueued task: {data}")
    # code, info, e = result  # 从结果中解包任务返回的值（同步阻塞，等待任务完成）
    return json_sanic({"status": 200, "info": "Task started successfully."})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=False, workers=1)
