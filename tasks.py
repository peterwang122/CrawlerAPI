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
        'high_priority_queue': 0,
        'low_priority_queue': 0,
        'default_queue': 0  # 新增默认队列
    }

    # 明确定义任务类型映射
    TASK_TYPE_MAPPING = {
        "SearchtermCrawlerAsin": "high_priority_queue",
        "CrawlerAsin": "low_priority_queue"
    }

    while True:
        task = r.lpop(old_queue)
        if not task:
            break

        try:
            data = json.loads(task)
            task_type = data.get('position')

            # 严格类型判断
            if task_type in TASK_TYPE_MAPPING:
                target = TASK_TYPE_MAPPING[task_type]
            else:
                target = 'default_queue'  # 未定义类型进入默认队列

            r.rpush(target, task)
            new_queue_mapping[target] += 1

        except json.JSONDecodeError:
            print(f"无效任务数据: {task}")
            r.rpush('corrupted_tasks', task)  # 异常数据单独存放

    print("迁移结果统计:")
    for queue, count in new_queue_mapping.items():
        print(f"{queue}: {count} tasks")


if __name__ == '__main__':
    migrate_tasks()