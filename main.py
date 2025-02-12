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
# 定义双队列映射
TASK_QUEUES = {
    "SearchtermCrawlerAsin": ["high_priority_queue_NA", "high_priority_queue_EU", "high_priority_queue_FE"],
    "CrawlerAsin": "low_priority_queue"
}


# 新增地区映射判断函数
def determine_queue(market):
    """根据market参数确定地区队列"""
    market = str(market).upper()
    na_markets = {"US", "MX", "CA", "BR"}
    fe_markets = {"JP", "AU", "SG"}

    if market in na_markets:
        return "high_priority_queue_NA"
    if market in fe_markets:
        return "high_priority_queue_FE"
    return "high_priority_queue_EU"  # 其他情况默认EU队列


def verify_request(token, timestamp, secret_key):
    """请求验证函数"""
    calculated_token = hashlib.sha256(
        (secret_key + str(timestamp) + secret_key).encode('utf-8')
    ).hexdigest()
    return token == calculated_token


@app.before_server_start
async def setup(app, _):
    """服务启动初始化（完整修复版）"""
    # 展开所有队列配置（处理列表和字符串类型）
    all_queues = []
    for q in TASK_QUEUES.values():
        if isinstance(q, list):
            all_queues.extend([str(queue) for queue in q])
        else:
            all_queues.append(str(q))

    # 为每个物理队列启动独立消费者
    for queue in set(all_queues):  # 去重确保唯一性
        app.add_task(task_processor(queue))

    # 超时设置保持不变
    app.config.REQUEST_TIMEOUT = 1800
    app.config.RESPONSE_TIMEOUT = 1800


async def task_processor(queue_name: str):
    """通用任务处理器（完整修复版）"""
    while True:
        try:
            # 强制类型转换确保安全
            queue_name = str(queue_name)

            # 原子性获取任务
            task = redis_client.lpop(queue_name)
            if not task:
                await asyncio.sleep(0.5)
                continue

            data = json.loads(task)
            print(f"[{queue_name}] 开始处理任务: {data}")

            # 执行实际任务
            await list_api(data)

            print(f"[{queue_name}] 任务完成: {data}")

        except json.JSONDecodeError:
            print(f"无效任务数据: {task}")
        except Exception as e:
            print(f"任务处理失败: {str(e)}")
            # 增强的重试逻辑
            try:
                current_length = redis_client.llen(queue_name)
                if current_length < 1000:
                    # 使用管道保证原子性操作
                    with redis_client.pipeline() as pipe:
                        pipe.rpush(queue_name, task)
                        pipe.execute()
                    print(f"任务重新入队: {task}")
            except Exception as pipe_error:
                print(f"重试入队失败: {str(pipe_error)}")


@app.exception(Exception)
async def global_exception_handler(request, exception):
    """全局异常处理"""
    error_msg = f"请求 {request.url} 出错: {str(exception)}"
    print(error_msg)
    return json_sanic({"error": error_msg}, status=500)


@app.route('/api/data/list', methods=['POST'])
async def handle_task(request: Request):
    """任务提交接口"""
    # 身份验证
    secret_key = "69c5fcebaa65b560eaf06c3fbeb481ae44b8d618"
    if not verify_request(
            request.headers.get('token'),
            request.headers.get('timestamp'),
            secret_key
    ):
        return json_sanic({"error": "认证失败"}, status=401)

    # 数据校验
    data = request.json
    if not data.get("text"):
        return json_sanic({"error": "text字段不能为空"}, status=400)

    # 任务类型校验
    task_type = data.get("position")
    if task_type not in TASK_QUEUES:
        return json_sanic({
            "status": 400,
            "error": "无效任务类型",
            "allowed_types": list(TASK_QUEUES.keys())
        }, status=400)

    # 确定目标队列
    if task_type == "SearchtermCrawlerAsin":
        if not data.get("market"):
            return json_sanic({"error": "高优先级任务必须包含market参数"}, status=400)
        target_queue = determine_queue(data["market"])
    else:
        target_queue = TASK_QUEUES[task_type]

    # 检查重复任务
    task_json = json.dumps(data, sort_keys=True)
    duplicate = False

    # 根据任务类型决定检查范围
    if task_type == "SearchtermCrawlerAsin":
        check_queues = TASK_QUEUES["SearchtermCrawlerAsin"]
    else:
        check_queues = [target_queue]

    for q in check_queues:
        existing_tasks = redis_client.lrange(q, 0, -1)
        if any(task.decode('utf-8') == task_json for task in existing_tasks):
            duplicate = True
            break

    if duplicate:
        return json_sanic({
            "status": 200,
            "info": "任务已存在",
            "queue": target_queue
        })

    # 提交任务
    redis_client.rpush(target_queue, task_json)
    print(f"任务已提交到队列 {target_queue}: {data}")

    return json_sanic({
        "status": 200,
        "info": "任务提交成功",
        "queue": target_queue,
        "position": redis_client.llen(target_queue)
    })


# 队列监控接口
@app.route('/queues/status', methods=['GET'])
async def queue_monitor(request: Request):
    """队列状态查询"""
    status = {}
    for ttype, qname in TASK_QUEUES.items():
        status[ttype] = {
            "queue_name": qname,
            "pending_tasks": redis_client.llen(qname),
            "oldest_task": redis_client.lindex(qname, 0),
            "newest_task": redis_client.lindex(qname, -1)
        }
    return json_sanic(status)




if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)