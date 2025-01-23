import json
import os
import shutil

import yaml
from util.InserOnlineData import ProcessShowData
from configuration.path import get_config_path
import redis
from config import REDIS_CONFIG

redis_client = redis.Redis(db=11,**REDIS_CONFIG)


def automatic_configuration():
    # 切换到目标数据库（例如数据库 1）
    redis_client.select(11)

    # 清空当前数据库（数据库 1）
    redis_client.flushdb()

    print("Redis 缓存已清除")
    data = {
        "CloseFlag": 0  # 1 关闭的 0 没有关闭的
    }
    data, msg = ProcessShowData.user_account_info(post_data=data)
    for item in msg['data']:
        print(item['ID'])
        print('-------------------------')
        if item['DbName'] and item['LogDbName']:
            # 分割 DbName 和 LogDbName
            db_names = item['DbName'].split(',')
            log_db_names = item['LogDbName'].split(',')

            # 处理 DbName 和 LogDbName
            for db_name, log_db_name in zip(db_names, log_db_names):
                db_info = db_name.strip()
                cache_brand_key = f"brand_info:{db_info}"
                brand_info = {
                    'UID': item['ID'],
                    "db": db_info,
                    "is_new": item['IsDistributor'],
                }
                redis_client.set(cache_brand_key, json.dumps(brand_info))
                print(f"缓存写入成功: {cache_brand_key}")

                # 使用传入的 db 作为缓存键
                cache_key = f"db_info:{db_info}"
                db_info_data = {
                    "host": "192.168.2.139",
                    "user": "wanghequan",
                    "password": "WHq123123Aa",
                    "port": 3306,
                    "db": db_info,
                    "charset": "utf8mb4",
                    "use_unicode": True
                }
                redis_client.set(cache_key, json.dumps(db_info_data))
                print(f"缓存写入成功: {cache_key}")

                log_db_info = log_db_name.strip()
                db_log_info = {
                    "host": "192.168.2.123",
                    "user": "wanghequan",
                    "password": "WHq123123Aa",
                    "port": 3308,
                    "db": log_db_info,
                    "charset": "utf8mb4",
                    "use_unicode": True
                }
                # Update the JSON file with new dbinfo
                cache_log_key = f"db_info_log:{db_info}"
                redis_client.set(cache_log_key, json.dumps(db_log_info))
                print(f"缓存写入成功: {cache_log_key}")


def update_configuration(db):
    data = {
        "CloseFlag": 0  # 1 关闭的 0 没有关闭的
    }
    data, msg = ProcessShowData.user_account_info(post_data=data)

    # 添加一个标志变量，用于判断是否找到匹配的 db
    found = False

    for item in msg['data']:
        if db in item['DbName']:
            found = True  # 找到匹配的 db
            print(item['ID'])
            print('-------------------------')
            if item['DbName'] and item['LogDbName']:
                # 分割 DbName 和 LogDbName
                db_names = item['DbName'].split(',')
                log_db_names = item['LogDbName'].split(',')

                # 处理 DbName 和 LogDbName
                for db_name, log_db_name in zip(db_names, log_db_names):
                    db_info = db_name.strip()
                    cache_brand_key = f"brand_info:{db_info}"
                    brand_info = {
                        'UID': item['ID'],
                        "db": db_info,
                        "is_new": item['IsDistributor'],
                    }
                    redis_client.set(cache_brand_key, json.dumps(brand_info))
                    print(f"缓存写入成功: {cache_brand_key}")

                    # 使用传入的 db 作为缓存键
                    cache_key = f"db_info:{db_info}"
                    db_info_data = {
                        "host": "192.168.2.139",
                        "user": "wanghequan",
                        "password": "WHq123123Aa",
                        "port": 3306,
                        "db": db_info,
                        "charset": "utf8mb4",
                        "use_unicode": True
                    }
                    redis_client.set(cache_key, json.dumps(db_info_data))
                    print(f"缓存写入成功: {cache_key}")

                    log_db_info = log_db_name.strip()
                    db_log_info = {
                        "host": "192.168.2.123",
                        "user": "wanghequan",
                        "password": "WHq123123Aa",
                        "port": 3308,
                        "db": log_db_info,
                        "charset": "utf8mb4",
                        "use_unicode": True
                    }
                    # Update the JSON file with new dbinfo
                    cache_log_key = f"db_info_log:{db_info}"
                    redis_client.set(cache_log_key, json.dumps(db_log_info))
                    print(f"缓存写入成功: {cache_log_key}")
            break
    # 如果循环结束后没有找到匹配的 db，抛出异常
    if not found:
        raise ValueError(f"{db} 该数据库不存在")
if __name__ == '__main__':
    # automatic_configuration()
    # res = find_brand_by_uid(1)
    # update_configuration("amazon_outdoormaster")
    # print(res)
    data = {
        "CloseFlag": 0  # 1 关闭的 0 没有关闭的
    }
    data, msg = ProcessShowData.user_account_info(post_data=data)
    print(data, msg)