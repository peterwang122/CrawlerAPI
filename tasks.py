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


def recover_legacy_queue():
    """紧急恢复原队列（先执行这个！）"""
    # 从临时队列恢复
    temp_count = redis_client.llen(TEMP_QUEUE)
    if temp_count > 0:
        print(f"正在从临时队列恢复 {temp_count} 条任务...")
        for _ in range(temp_count):
            task = redis_client.rpoplpush(TEMP_QUEUE, OLD_HIGH_QUEUE)
            print(f"已恢复任务: {task[:50]}...")

    # 从死信队列恢复
    dead_count = redis_client.llen(DEAD_LETTER_QUEUE)
    if dead_count > 0:
        print(f"正在从死信队列恢复 {dead_count} 条任务...")
        for _ in range(dead_count):
            task = redis_client.rpoplpush(DEAD_LETTER_QUEUE, OLD_HIGH_QUEUE)
            print(f"已恢复任务: {task[:50]}...")

    print(f"恢复完成，原队列现有任务数: {redis_client.llen(OLD_HIGH_QUEUE)}")


def safe_migrate_legacy_queue(batch_size=100):
    """安全迁移方法（兼容旧Redis版本）"""
    print("启动安全迁移流程...")
    recover_legacy_queue()  # 先执行恢复

    total = redis_client.llen(OLD_HIGH_QUEUE)
    print(f"待迁移任务总数: {total}")

    for _ in range(min(batch_size, total)):
        # 原子操作转移任务
        task = redis_client.rpoplpush(OLD_HIGH_QUEUE, TEMP_QUEUE)
        if not task:
            break

        try:
            data = json.loads(task)
            if "market" not in data:
                raise ValueError("缺少market字段")

            target_queue = determine_queue(data["market"])
            task_str = task.decode('utf-8')

            # 兼容旧Redis版本的检查方法
            exists = False
            for q in TASK_QUEUES["SearchtermCrawlerAsin"]:
                if task_str in [t.decode('utf-8') for t in redis_client.lrange(q, 0, -1)]:
                    exists = True
                    break

            if not exists:
                redis_client.rpush(target_queue, task)

            # 确认迁移成功后删除临时队列任务
            redis_client.lrem(TEMP_QUEUE, 1, task)

        except Exception as e:
            print(f"迁移失败任务已保留在临时队列: {task[:50]}... 错误: {str(e)}")
            # 将任务移回原队列
            redis_client.rpoplpush(TEMP_QUEUE, OLD_HIGH_QUEUE)

    remaining = redis_client.llen(OLD_HIGH_QUEUE)
    print(f"迁移进度: 已处理 {total - remaining}/{total} 条任务")

if __name__ == "__main__":
    safe_migrate_legacy_queue()