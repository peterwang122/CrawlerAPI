import json

import redis

r = redis.Redis(db=12)  # 使用你的Redis配置

TASK_QUEUES = {
    "SearchtermCrawlerAsin": "high_priority_queue",  # 高优先级队列
    "CrawlerAsin": "low_priority_queue"  # 普通优先级队列
}
def migrate_tasks():
    old_queue = 'task_queue'
    new_queue_mapping = {
        'high_priority_queue': 0,  # 假设旧任务60%为高优先级
        'low_priority_queue': 0  # 40%为低优先级
    }

    while True:
        task = r.lpop(old_queue)
        if not task:
            break

        # 解析任务判断类型（根据实际业务逻辑调整）
        data = json.loads(task)
        if 'position' in data:  # 如果旧数据已有类型标识
            target = TASK_QUEUES.get(data['position'], 'low_priority_queue')
        else:  # 无类型则根据业务规则判断
            target = 'high_priority_queue' if data.get('position') else 'low_priority_queue'

        r.rpush(target, task)
        new_queue_mapping[target] += 1

    print(f"迁移完成，分布: {new_queue_mapping}")


if __name__ == '__main__':
    migrate_tasks()