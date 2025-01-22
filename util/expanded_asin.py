import asyncio
import os
import random
import ssl
from decimal import Decimal
from datetime import datetime, timedelta
import pandas as pd
import pymysql
import aiohttp
from aiohttp import ClientTimeout
from lxml import html
import json
import csv
import time
from time import sleep
from db.tools_db_new_sp import DbNewSpTools
from db.tools_db_sp import DbSpTools

# def load_db_info(db,brand,market):
#     # 从 JSON 文件加载数据库信息
#     db_info_path = os.path.join(get_config_path(), 'db_info.json')
#     with open(db_info_path, 'r') as f:
#         db_info_json = json.load(f)
#
#     if db not in db_info_json:
#         raise ValueError(f"Unknown db '{db}'")
#
#     brand_info = db_info_json[db][brand]
#
#     # 如果指定了国家
#     if market:
#         # 检查国家是否在品牌信息中
#         if market in brand_info:
#             return brand_info[market]
#         # 如果没有找到具体国家的信息，检查是否有默认信息
#         if 'default' in brand_info:
#             return brand_info['default']
#
#     # 返回默认信息
#     return brand_info.get('default', {})
#
# def load_db_info_log(db,brand,market):
#     # 从 JSON 文件加载数据库信息
#     db_info_path = os.path.join(get_config_path(), 'db_info_log.json')
#     with open(db_info_path, 'r') as f:
#         db_info_json = json.load(f)
#
#     if db not in db_info_json:
#         raise ValueError(f"Unknown db '{db}'")
#
#     brand_info = db_info_json[db][brand]
#
#     # 如果指定了国家
#     if market:
#         # 检查国家是否在品牌信息中
#         if market in brand_info:
#             return brand_info[market]
#         # 如果没有找到具体国家的信息，检查是否有默认信息
#         if 'default' in brand_info:
#             return brand_info['default']
#
#     # 返回默认信息
#     return brand_info.get('default', {})
#
# def campaign_info(db, brand, market,num):
#     query = f"""
# WITH campaign_list AS (
#             SELECT DISTINCT campaignId
#             FROM amazon_campaigns_list_sp
#             WHERE state = 'ENABLED'
#             AND targetingtype = 'MANUAL'
#             AND market = '{market}'
#             AND LOWER(campaign_name) LIKE '%deep%'
#             AND LOWER(campaign_name) LIKE '%0514%'
#             and startDate < DATE_SUB(CURDATE(), INTERVAL 0 DAY )
#
# )
# SELECT
#     a.campaignId,
#     CASE
#         -- 如果 count(1) 为 NULL，使用 0 代替，并且如果 700 - count(1) > 400，则返回 400
#         WHEN {int(float(num))+200} - IFNULL(COUNT(b.targetId), 0) > 400 THEN 400
#         ELSE {int(float(num))+200} - IFNULL(COUNT(b.targetId), 0)
#     END AS calculated_value
# FROM
#     campaign_list a
# LEFT JOIN
#     amazon_targets_list_sp b
#     ON a.campaignId = b.campaignId
#     AND b.expression LIKE '%EXPANDED%'
#     AND b.state = 'ENABLED'
# GROUP BY
# 		a.campaignId
# HAVING
#     -- 如果 count(1) 为 NULL 或者 count(1) 小于 500，仍然返回结果
#     IFNULL(COUNT(b.campaignId), 0) < {int(float(num))}
#     """
#     try:
#         db_config = load_db_info(db, brand, market)
#         connection = pymysql.connect(**db_config)
#         with connection.cursor(pymysql.cursors.DictCursor) as cursor:
#             cursor.execute(query)
#             resultall = cursor.fetchall()
#             return resultall
#     finally:
#         connection.close()
#
# def data_info(db, brand, market,classification_id,day):
#     query = f"""
# SELECT
#     COUNT(*) AS total_last_7_days,
#     COUNT(CASE WHEN DATE(Date) = CURRENT_DATE THEN 1 END) AS total_today
# FROM expanded_asin_info
# WHERE market = '{market}'
# AND classification_id = '{classification_id}'
# AND Date > CURRENT_DATE- INTERVAL {int(float(day))+1} DAY
#     """
#     try:
#         db_config = load_db_info_log(db, brand, market)
#         connection = pymysql.connect(**db_config)
#         with connection.cursor(pymysql.cursors.DictCursor) as cursor:
#             cursor.execute(query)
#             # 获取结果的第一行
#             result = cursor.fetchone()
#             # 返回两个统计值: total_last_7_days 和 total_today
#             return result['total_last_7_days'], result['total_today']
#     finally:
#         connection.close()

def get_proxies(region):
    proxies = "http://192.168.2.165:7890"
    if region in ("JP","US"):
        print("有代理")
        return proxies
    else:
        return None


def generate_urls(market, classification_rank_classification_id):
    # URL 模板
    url_templates = {
    "US": [
        "https://www.amazon.com/Best-Sellers-Clothing-Shoes-Jewelry-Mens-Thermal-Underwear-Tops/zgbs/fashion/{}/ref=zg_bs_pg_1_fashion?_encoding=UTF8&pg=1",
        "https://www.amazon.com/Best-Sellers-Clothing-Shoes-Jewelry-Mens-Thermal-Underwear-Tops/zgbs/fashion/{}/ref=zg_bs_pg_2_fashion?_encoding=UTF8&pg=2"
    ],
    "UK": [
        "https://www.amazon.co.uk/Best-Sellers-Fashion-Mens-Thermal-Sets/zgbs/fashion/{}/ref=zg_bs_pg_1_fashion?_encoding=UTF8&pg=1",
        "https://www.amazon.co.uk/Best-Sellers-Fashion-Mens-Thermal-Sets/zgbs/fashion/{}/ref=zg_bs_pg_2_fashion?_encoding=UTF8&pg=2"
    ],
    "JP": [
        "https://www.amazon.co.jp/-/zh/gp/bestsellers/fashion/{}/ref=zg_bs_pg_1_fashion?ie=UTF8&pg=1",
        "https://www.amazon.co.jp/-/zh/gp/bestsellers/fashion/{}/ref=zg_bs_pg_2_fashion?ie=UTF8&pg=2"
    ],
    "DE": [
        "https://www.amazon.de/gp/bestsellers/fashion/{}/ref=zg_bs_pg_1_fashion?ie=UTF8&pg=1",
        "https://www.amazon.de/gp/bestsellers/fashion/{}/ref=zg_bs_pg_2_fashion?ie=UTF8&pg=2"
    ],
    "FR": [
        "https://www.amazon.fr/gp/bestsellers/fashion/{}/ref=zg_bs_pg_1_fashion?ie=UTF8&pg=1",
        "https://www.amazon.fr/gp/bestsellers/fashion/{}/ref=zg_bs_pg_2_fashion?ie=UTF8&pg=2"
    ],
    "IT": [
        "https://www.amazon.it/gp/bestsellers/fashion/{}/ref=zg_bs_pg_1_fashion?ie=UTF8&pg=1",
        "https://www.amazon.it/gp/bestsellers/fashion/{}/ref=zg_bs_pg_2_fashion?ie=UTF8&pg=2"
    ],
    "ES": [
        "https://www.amazon.es/gp/bestsellers/fashion/{}/ref=zg_bs_pg_1_fashion?ie=UTF8&pg=1",
        "https://www.amazon.es/gp/bestsellers/fashion/{}/ref=zg_bs_pg_2_fashion?ie=UTF8&pg=2"
    ],
    "AU": [
        "https://www.amazon.com.au/gp/bestsellers/computers/{}/ref=zg_bs_pg_1_computers?ie=UTF8&pg=1",
        "https://www.amazon.com.au/gp/bestsellers/computers/{}/ref=zg_bs_pg_2_computers?ie=UTF8&pg=2"
    ],
    "CA": [
        "https://www.amazon.ca/Best-Sellers-Clothing-Shoes-Accessories-Mens-Fleece-Coats/zgbs/fashion/{}/ref=zg_bs_pg_1_fashion?_encoding=UTF8&pg=1",
        "https://www.amazon.ca/Best-Sellers-Clothing-Shoes-Accessories-Mens-Fleece-Coats/zgbs/fashion/{}/ref=zg_bs_pg_2_fashion?_encoding=UTF8&pg=2"
    ],
    "MX": [
        "https://www.amazon.com.mx/gp/bestsellers/shoes/{}/ref=zg_bs_pg_1_shoes?ie=UTF8&pg=1",
        "https://www.amazon.com.mx/gp/bestsellers/shoes/{}/ref=zg_bs_pg_2_shoes?ie=UTF8&pg=2"
    ],
    "AE": [
        "https://www.amazon.ae/gp/bestsellers/pet-products/{}/ref=zg_bs_pg_1_pet-products?ie=UTF8&pg=1",
        "https://www.amazon.ae/gp/bestsellers/pet-products/{}/ref=zg_bs_pg_2_pet-products?ie=UTF8&pg=2"
    ]
    }
    if market not in url_templates:
        raise ValueError(f"未知的市场：{market}")
    return [url.format(classification_rank_classification_id) for url in url_templates[market]]


async def pachong(db, brand, market, classification_rank_classification_id):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    urls = generate_urls(market, classification_rank_classification_id)
    all_asin_data = []

    async def extract_asin_data(url, retries=3):
        timeout = ClientTimeout(total=None, connect=None, sock_connect=None, sock_read=None)
        for attempt in range(retries):
            async with aiohttp.ClientSession(timeout=timeout) as session:
                try:
                    # 使用 aiohttp 发送 GET 请求
                    async with session.get(url, headers=headers, proxy=get_proxies(market)) as response:
                        if response.status == 200:
                            content = await response.read()  # 获取响应内容
                            tree = html.fromstring(content)  # 使用 lxml 解析 HTML
                            asin_list = []

                            # XPath 查询
                            product_elements = tree.xpath('//*[@data-client-recs-list]')
                            for element in product_elements:
                                asin_value = element.attrib.get('data-client-recs-list')
                                if asin_value:
                                    asin_list.append(asin_value)
                                else:
                                    print(f"请求失败，状态码: {response.status}")

                            if len(asin_list) > 0:
                                return asin_list
                            else:
                                await asyncio.sleep(random.uniform(3, 5))  # 如果请求失败，等待5秒后重试
                                return []
                        else:
                            print(f"请求失败，状态码：{response.status}")
                            await asyncio.sleep(1)  # 如果请求失败，等待1秒后重试


                except (aiohttp.ClientError, ssl.SSLError) as e:

                    print(f"Attempt {attempt + 1} failed: {e}")

                    if attempt < retries - 1:

                        await asyncio.sleep(1)  # 等待5秒后重试

                    else:

                        print("Max retries reached, giving up.")

                        return []

                except Exception as e:

                    print(f"Unexpected error: {e}")

                    return []

    max_attempts = 100  # 设置最大尝试次数
    attempts = 0  # 尝试计数器

    while len(all_asin_data) < 2 and attempts < max_attempts:
        all_asin_data = []  # 清空之前的数据
        for url in urls:
            asin_data = await extract_asin_data(url)
            if asin_data is not None:  # 确保返回的数据是可迭代的
                all_asin_data.extend(asin_data)

        if len(all_asin_data) < 2:
            print(f"当前数据量 {len(all_asin_data)}，重新获取数据...")

        attempts += 1  # 增加尝试次数

    # 如果循环超过最大次数，还未获取到足够的数据
    if len(all_asin_data) < 2:
        return f"经过 {max_attempts} 次尝试后，仍未获取到分类{classification_rank_classification_id}的top100竞品ASIN"

    updates = []
    today = datetime.today()
    cur_time = today.strftime('%Y-%m-%d')
    for asin_value in all_asin_data:
        try:
            json_data = json.loads(asin_value)
            for item in json_data:
                product_id = item.get('id')
                rank = item['metadataMap'].get('render.zg.rank')

                updates.append({
                    'market': market,
                    'classification_id': classification_rank_classification_id,
                    'Asin': product_id,
                    'Rank': rank,
                    'Date': cur_time
                })
        except json.JSONDecodeError:
            print("JSON 解码错误")

    api = DbNewSpTools(db, brand, market)
    await api.init()
    await api.batch_expanded_asin_info(updates)
    return f"已抓取分类{classification_rank_classification_id}的top100竞品ASIN"



async def expanded_asin(db,brand,market,num,day):
    print(num)
    db1 = DbNewSpTools(db,brand,market)
    db2 = DbSpTools(db,brand,market)
    info = await db2.campaign_info(num)
    if not info:
        info = [{"campaignId": 88, "calculated_value": 100}]
    api = DbSpTools(db, brand, market)
    sql_results = await api.get_classification_id(market)
    valid_campaign_data = {campaign['campaignId']: campaign['calculated_value'] for campaign in info}
    if sql_results is not None and not sql_results.empty:
        result = sql_results.groupby(['classification_rank_classification_id'])[
            ['classification_rank_title','campaignId']].agg(list)
        massage = []
        for classification_id, series in result.iterrows():
            print(f"分类id：{classification_id}")
            classification_rank_titles = series['classification_rank_title']
            campaignIds = series['campaignId']
            # 查找是否有一个 campaignId 在 valid_campaign_data 中
            valid_values = [valid_campaign_data[campaignId] for campaignId in campaignIds if
                            campaignId in valid_campaign_data]

            count1, count2 = await db1.data_info(classification_id, day)
            # 如果有有效的 campaignId，则取出最大值
            if valid_values and count2 == 0:
                calculated_value = max(valid_values)
            else:
                if count1 == 0:
                    print(f"分类id：{classification_id}已经{int(float(day))}天没有爬取")
                else:
                    print(f"分类id：{classification_id}无需要添加的campaignId，跳过")
                    continue  # 没有找到有效的 campaignId，则跳过
            classification_rank_classification_id = classification_id
            # loop = asyncio.get_event_loop()
            # loop.run_until_complete(pachong(db, brand, market, classification_rank_classification_id))
            info = await pachong(db, brand, market, classification_rank_classification_id)
            massage.append(info)
        print(massage)
        if massage:
            updates = []
            updates.append({
                'market': market,
                'classification_id': f"品牌{brand} 国家{market}托管ASIN已爬取分类完成",
                'Asin': "已完成",
                'Rank': "1000",
                'Date': datetime.today().strftime('%Y-%m-%d')
            })
            api = DbNewSpTools(db, brand, market)
            await api.init()
            await api.batch_expanded_asin_info(updates)
            return massage
        else:
            updates = []
            updates.append({
                'market': market,
                'classification_id': f"品牌{brand} 国家{market}托管ASIN无需要爬取分类",
                'Asin': "已完成",
                'Rank': "1000",
                'Date': datetime.today().strftime('%Y-%m-%d')
            })
            api = DbNewSpTools(db, brand, market)
            await api.init()
            await api.batch_expanded_asin_info(updates)
            return [f"品牌{brand} 国家{market}托管ASIN无需要爬取分类"]
    else:
        updates = []
        updates.append({
            'market': market,
            'classification_id': f"品牌{brand} 国家{market}托管ASIN无小分类，无TOP100竞品ASIN",
            'Asin': "已完成",
            'Rank': "1000",
            'Date': datetime.today().strftime('%Y-%m-%d')
        })
        api = DbNewSpTools(db, brand, market)
        await api.init()
        await api.batch_expanded_asin_info(updates)
        return [f"品牌{brand} 国家{market}托管ASIN无小分类，无TOP100竞品ASIN"]


if __name__ == "__main__":
    asyncio.run(expanded_asin('amazon_64_ZAPJQL', 'ZAPJQL', 'JP', 500, 5))

