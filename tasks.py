import json

import redis

redis_client = redis.Redis(db=12)  # 使用你的Redis配置

TASK_QUEUES = {
    "SearchtermCrawlerAsin": ["high_priority_queue_NA", "high_priority_queue_EU", "high_priority_queue_FE"],
    "CrawlerAsin": "low_priority_queue"
}

# 新增迁移相关配置
OLD_HIGH_QUEUE = "high_priority_queue"  # 原队列名称
TEMP_QUEUE = "migration_temp_queue"  # 迁移临时队列
DEAD_LETTER_QUEUE = "dead_letter_queue"  # 死信队列

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
def migrate_legacy_queue(batch_size=100):
    """迁移旧队列任务到新队列（安全原子操作）"""
    print(f"开始迁移旧队列任务，每次处理 {batch_size} 条")

    migrated_count = 0
    while True:
        # 使用原子操作转移任务
        task = redis_client.rpoplpush(OLD_HIGH_QUEUE, TEMP_QUEUE)
        if not task:
            break  # 队列已空

        try:
            data = json.loads(task)
            if "market" not in data:
                raise ValueError("缺少market字段")

            # 确定目标队列
            target_queue = determine_queue(data["market"])

            # 检查是否已存在（原子操作）
            with redis_client.pipeline() as pipe:
                pipe.multi()
                # 检查所有可能队列
                for q in TASK_QUEUES["SearchtermCrawlerAsin"]:
                    pipe.lpos(q, task)
                exists = any(pipe.execute())

                if not exists:
                    pipe.rpush(target_queue, task)
                pipe.lrem(TEMP_QUEUE, 1, task)  # 移除临时队列任务
                pipe.execute()

                migrated_count += 1
                if migrated_count % 100 == 0:
                    print(f"已迁移 {migrated_count} 条任务")

        except Exception as e:
            print(f"迁移失败任务: {task}，错误: {str(e)}")
            redis_client.lpush(DEAD_LETTER_QUEUE, task)
            redis_client.lrem(TEMP_QUEUE, 0, task)  # 清理临时队列

    print(f"迁移完成，共迁移 {migrated_count} 条任务")
    _cleanup_migration()


def _cleanup_migration():
    """迁移后清理"""
    # 安全删除空队列
    for q in [OLD_HIGH_QUEUE, TEMP_QUEUE]:
        if redis_client.llen(q) == 0:
            redis_client.delete(q)
    print("已清理迁移临时队列")

# 使用方法（在服务启动后调用）：
# migrate_legacy_queue(batch_size=500)