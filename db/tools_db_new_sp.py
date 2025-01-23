import json
import os
import asyncio

import aiomysql
import pymysql
import pandas as pd

from db.base.db_api import BaseDb


class DbNewSpTools(BaseDb):
    def __init__(self, db, brand, market, log=True):
        super().__init__(db, brand, market, log)

    async def batch_expanded_asin_info(self, updates):
        try:
            async with self.conn.cursor() as cursor:
                for update in updates:
                    # 查询是否已经存在相同的记录
                    query_check = """
                    SELECT COUNT(*) FROM expanded_asin_info
                    WHERE `classification_id` = %s AND `Asin` = %s AND `Date` = %s
                    """
                    await cursor.execute(query_check, (update['classification_id'], update['Asin'], update['Date']))
                    result = await cursor.fetchone()

                    # 如果不存在，则执行插入操作
                    if result[0] == 0:
                        query_insert = """
                        INSERT INTO expanded_asin_info (`market`, `classification_id`, `Asin`, `Rank`, `Date`)
                        VALUES (%s, %s, %s, %s, %s)
                        """
                        await cursor.execute(query_insert, (
                        update['market'], update['classification_id'], update['Asin'], update['Rank'], update['Date']))
                await self.conn.commit()
                print("Records inserted successfully into expanded_asin_info table")
        except Exception as e:
            print(f"Error occurred when inserting into expanded_asin_info: {e}")
        finally:
            # 确保连接关闭
            await self.close_connection()

    async def data_info(self, classification_id, day):
        try:
            # 确保数据库连接已初始化
            if self.conn is None:
                await self.init()  # 初始化连接
            conn = self.conn
            query = f"""
SELECT
    COUNT(*) AS total_last_7_days,
    COUNT(CASE WHEN DATE(Date) = CURRENT_DATE THEN 1 END) AS total_today
FROM expanded_asin_info
WHERE market = '{self.market}'
AND classification_id = '{classification_id}'
AND Date > CURRENT_DATE- INTERVAL {int(float(day)) + 1} DAY
            """
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query)
                result = await cursor.fetchall()
                return result[0]['total_last_7_days'], result[0]['total_today']
        except Exception as error:
            # print("get_profileId Error while querying data:", error)
            return None,None




