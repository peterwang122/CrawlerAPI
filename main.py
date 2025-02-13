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
redis_client = redis.Redis(db=12, **REDIS_CONFIG)


class AppState:
    running = False
    active_tasks = set()
    worker_queues = {}  # 记录各工作进程处理的队列


TASK_QUEUES = {
    "SearchtermCrawlerAsin": ["high_priority_queue_NA", "high_priority_queue_EU", "high_priority_queue_FE"],
    "CrawlerAsin": ["low_priority_queue"]
}


def determine_queue(market):
    market = str(market).upper()
    na_markets = {"US", "MX", "CA", "BR"}
    fe_markets = {"JP", "AU", "SG"}
    return "high_priority_queue_NA" if market in na_markets else \
        "high_priority_queue_FE" if market in fe_markets else \
            "high_priority_queue_EU"


@app.before_server_start
async def setup(app, _):
    AppState.running = True
    worker_id = os.getenv("SANIC_WORKER_NAME", "main")

    # 分配当前工作进程处理的队列
    all_queues = [q for sublist in TASK_QUEUES.values() for q in sublist]
    worker_index = int(worker_id.split("-")[-1]) if worker_id != "main" else 0
    assigned_queues = [all_queues[worker_index % len(all_queues)]]

    AppState.worker_queues[worker_id] = assigned_queues
    print(f"Worker {worker_id} 负责队列: {assigned_queues}")

    # 初始化浏览器配置
    app.config.PYPPETEER_ARGS = [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        f'--user-data-dir=/tmp/pyppeteer_{worker_id}',
        f'--remote-debugging-port={9222 + worker_index}'
    ]

    # 启动队列处理器
    for queue in assigned_queues:
        app.add_task(task_processor(queue))

    app.config.REQUEST_TIMEOUT = 1800
    app.config.RESPONSE_TIMEOUT = 1800


@app.before_server_stop
async def teardown(app, _):
    AppState.running = False
    worker_id = os.getenv("SANIC_WORKER_NAME", "main")

    # 等待任务完成
    start_time = time.time()
    while AppState.active_tasks and (time.time() - start_time) < 30:
        print(f"Worker {worker_id} 等待任务完成 (剩余: {len(AppState.active_tasks)})...")
        await asyncio.sleep(1)

    # 取消剩余任务
    for task in AppState.active_tasks.copy():
        task.cancel()

    # 清理浏览器资源
    await pyppeteer.__pyppeteer_launcher.killChrome()
    print(f"Worker {worker_id} 资源清理完成")


async def task_processor(queue_name: str):
    current_task = asyncio.current_task()
    AppState.active_tasks.add(current_task)

    try:
        while AppState.running:
            task_data = await fetch_task(queue_name)
            if not task_data:
                continue

            try:
                await process_task(queue_name, task_data)
            except Exception as e:
                await handle_task_error(queue_name, task_data, e)
    finally:
        AppState.active_tasks.discard(current_task)


async def fetch_task(queue_name):
    try:
        task = redis_client.lpop(queue_name)
        if task:
            return json.loads(task)
        await asyncio.sleep(0.1)
    except Exception as e:
        print(f"获取任务失败: {str(e)}")
    return None


async def process_task(queue_name, task_data):
    print(f"[{queue_name}] 开始处理任务: {task_data}")
    try:
        await asyncio.wait_for(
            list_api(task_data),
            timeout=1800
        )
        print(f"[{queue_name}] 任务完成: {task_data}")
    except asyncio.TimeoutError:
        print(f"[{queue_name}] 任务超时: {task_data}")
        await handle_retry(queue_name, task_data)
    except Exception as e:
        print(f"[{queue_name}] 任务异常: {str(e)}")
        await handle_retry(queue_name, task_data)


async def handle_task_error(queue_name, task_data, error):
    print(f"[{queue_name}] 任务处理失败: {str(error)}")
    await handle_retry(queue_name, json.dumps(task_data))


async def handle_retry(queue_name, task):
    if redis_client.llen(queue_name) < 1000:
        redis_client.rpush(queue_name, task)
        print(f"任务重新入队: {task}")

# 监控接口
@app.route('/queues/status', methods=['GET'])
async def queue_monitor(request: Request):
    """修复后的状态接口"""
    status = {}
    all_queues = []
    for q in TASK_QUEUES.values():
        all_queues.extend(q if isinstance(q, list) else [q])

    for queue in set(all_queues):
        oldest = redis_client.lindex(queue, 0)
        newest = redis_client.lindex(queue, -1)

        status[queue] = {
            "pending_tasks": redis_client.llen(queue),
            "oldest_task": oldest.decode('utf-8') if oldest else None,
            "newest_task": newest.decode('utf-8') if newest else None
        }

    return json_sanic(status)


def verify_request(token, timestamp, secret_key):
    """请求验证函数"""
    calculated_token = hashlib.sha256(
        (secret_key + str(timestamp) + secret_key).encode('utf-8')
    ).hexdigest()
    return token == calculated_token


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






if __name__ == '__main__':
    worker_count = len(TASK_QUEUES["SearchtermCrawlerAsin"]) + len(TASK_QUEUES["CrawlerAsin"])
    app.run(host='0.0.0.0', port=8000,
        workers=worker_count,
        single_process=False,
        access_log=False,
        auto_reload=False
    )