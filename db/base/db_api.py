import json
import os
from datetime import datetime
import aiomysql
from tenacity import retry, stop_after_attempt, wait_fixed
import redis
from config import REDIS_CONFIG
from typing import Dict, Any
from util.automatic_configuration import update_configuration


class BaseDb:
    def __init__(self, db, brand, market, log=False):
        self.brand = brand
        self.market = market
        self.db = db
        self.redis_client = redis.Redis(db=11,**REDIS_CONFIG)  # 使用 config 中的 Redis 配置
        if log:
            self.db_info = self.load_log_db_info()
        else:
            self.db_info = self.load_db_info()

        self.conn = None

    def load_db_info(self) -> Dict[str, Any]:
        # 生成缓存键
        cache_key = f"db_info:{self.db}"

        # 尝试从 Redis 缓存中获取数据
        cached_data = self.redis_client.get(cache_key)
        if cached_data:
            # 如果缓存中有数据，直接返回
            return json.loads(cached_data)
        else:
            print("缓存未命中，更新配置...")
            # 更新配置并获取新的数据库信息
            update_configuration(self.db)

            # 再次尝试从缓存中获取数据
            cached_data = self.redis_client.get(cache_key)
            if cached_data:
                print(f"更新后缓存数据: {cached_data}")
                return json.loads(cached_data)
            else:
                raise ValueError(f"无法加载数据库信息: {self.db}")

    def load_log_db_info(self) -> Dict[str, Any]:
        # 生成缓存键
        cache_key = f"db_info_log:{self.db}"

        # 尝试从 Redis 缓存中获取数据
        cached_data = self.redis_client.get(cache_key)
        if cached_data:
            # 如果缓存中有数据，直接返回
            return json.loads(cached_data)
        else:
            print("缓存未命中，更新配置...")
            # 更新配置并获取新的数据库信息
            update_configuration(self.db)

            # 再次尝试从缓存中获取数据
            cached_data = self.redis_client.get(cache_key)
            if cached_data:
                print(f"更新后缓存数据: {cached_data}")
                return json.loads(cached_data)
            else:
                raise ValueError(f"无法加载数据库信息: {self.db}")

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
    async def connect(self, db_info):
        try:
            conn = await aiomysql.connect(**db_info)  # 异步连接数据库
            print("Connected to amazon_mysql database!")
            return conn
        except Exception as error:
            print("Error while connecting to amazon_mysql:", error)
            raise

    async def close_connection(self):
        if self.conn:
            try:
                self.conn.close()
                print("Database connection closed.")
            except Exception as e:
                print(f"Error occurred while closing connection: {e}")

    def get_timestamp(self):
        # 获取当前时间
        current_time = datetime.now()
        timestamp = int(current_time.timestamp())
        date_string = current_time.strftime("%Y-%m-%d")
        # 组合日期和时间戳
        date_timestamp_string = f"{date_string}_{timestamp}"
        return date_timestamp_string


    async def init(self):
        # 在新的异步方法中处理数据库连接
        self.conn = await self.connect(self.db_info)


if __name__ == '__main__':
    BaseDb('amazon_74_TOPDC','TOPDC','US').load_db_info()
