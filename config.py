import os
from typing import Dict, Any

# Redis 配置
REDIS_CONFIG: Dict[str, Any] = {
    "host": os.getenv("REDIS_HOST", "localhost"),  # Redis 主机地址
    "port": int(os.getenv("REDIS_PORT", 6379)),   # Redis 端口
    # "db": int(os.getenv("REDIS_DB", 11)),          # Redis 数据库编号
    "password": os.getenv("REDIS_PASSWORD", None) # Redis 密码（可选）
}


