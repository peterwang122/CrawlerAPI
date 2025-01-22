import json
import os
from datetime import datetime
import aiomysql
import pandas as pd
import pymysql
from db.base.db_api import BaseDb



def get_timestamp():
    # 获取当前时间
    current_time = datetime.now()
    timestamp = int(current_time.timestamp())
    date_string = current_time.strftime("%Y-%m-%d")
    # 组合日期和时间戳
    date_timestamp_string = f"{date_string}_{timestamp}"
    return date_timestamp_string

class DbSpTools(BaseDb):
    def __init__(self, db, brand, market):
        super().__init__(db, brand, market)

    async def get_profileId(self, market):
        # 低于 平均ACOS值 30% 以上的 campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            # 确保数据库连接已初始化
            if self.conn is None:
                await self.init()  # 初始化连接
            conn = self.conn
            # 使用异步游标执行 SQL 查询
            query = f"""
SELECT DISTINCT profileId, region FROM amazon_profile
WHERE countryCode = '{market}'
            """
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query)
                result = await cursor.fetchall()
            if result:
                df = pd.DataFrame(result)
                print("get_profileId Data successfully!")
                return df.loc[0, 'profileId'], df.loc[0, 'region']
            else:
                return None, None  # 如果没有结果返回空
        except Exception as error:
            print("get_profileId Error while querying data:", error)
            return None, None

    async def get_classification_id(self, market):
        # 低于 平均ACOS值 30% 以上的 campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            # 确保数据库连接已初始化
            if self.conn is None:
                await self.init()  # 初始化连接
            conn = self.conn
            # 使用异步游标执行 SQL 查询
            query = f"""
    SELECT
        CASE
            WHEN info_ext.parent_asins = ""
                OR info_ext.parent_asins IS NULL THEN info_ext.asin
            ELSE info_ext.parent_asins
        END AS parent_asins,
        info_ext.classification_rank_classification_id,
        info_ext.classification_rank_title,
        product_sp.campaignId AS campaignId,
        b.campaign_name AS campaignName,
        product_sp.adGroupId AS adGroupId,
        product_sp.market AS market
    FROM
        amazon_sp_productads_list AS product_sp
        LEFT JOIN amazon_product_info_extended AS info_ext
            ON info_ext.asin = product_sp.asin
            AND info_ext.market = product_sp.market
        LEFT JOIN amazon_campaigns_list_sp b
            ON b.campaignId = product_sp.campaignId
        LEFT JOIN amazon_adgroups_list_sp c
            ON c.adgroupId = product_sp.adGroupId
    WHERE
        info_ext.market = '{market}'
        AND product_sp.market = '{market}'
        AND b.state = 'ENABLED'
        AND product_sp.state IN ('ENABLED', 'PAUSED')
        AND c.state = 'ENABLED'
        AND b.campaign_name LIKE '%DeepBI%'
        AND b.campaign_name LIKE '%0514%'
        AND classification_rank_classification_id IS NOT NULL
        AND classification_rank_classification_id != ''
    GROUP BY
        parent_asins,
        product_sp.campaignId
    ORDER BY
        parent_asins
            """
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query)
                result = await cursor.fetchall()
            if result:
                df = pd.DataFrame(result)
                print("get_profileId Data successfully!")
                return df
            else:
                return None
        except Exception as error:
            print("get_profileId Error while querying data:", error)
            return None


    async def get_classification_title(self, market):
        # 低于 平均ACOS值 30% 以上的 campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            # 确保数据库连接已初始化
            if self.conn is None:
                await self.init()  # 初始化连接
            conn = self.conn
            # 使用异步游标执行 SQL 查询
            query = f"""
    SELECT
        CASE
            WHEN info_ext.parent_asins = ""
                OR info_ext.parent_asins IS NULL THEN info_ext.asin
            ELSE info_ext.parent_asins
        END AS parent_asins,
        info_ext.classification_rank_classification_id,
        info_ext.classification_rank_title,
        product_sp.campaignId AS campaignId,
        b.campaign_name AS campaignName,
        product_sp.adGroupId AS adGroupId,
        product_sp.market AS market
    FROM
        amazon_sp_productads_list AS product_sp
        LEFT JOIN amazon_product_info_extended AS info_ext
            ON info_ext.asin = product_sp.asin
            AND info_ext.market = product_sp.market
        LEFT JOIN amazon_campaigns_list_sp b
            ON b.campaignId = product_sp.campaignId
        LEFT JOIN amazon_adgroups_list_sp c
            ON c.adgroupId = product_sp.adGroupId
    WHERE
        info_ext.market = '{market}'
        AND product_sp.market = '{market}'
        AND b.state = 'ENABLED'
        AND product_sp.state IN ('ENABLED', 'PAUSED')
        AND c.state = 'ENABLED'
        AND b.campaign_name LIKE '%DeepBI%'
        AND b.campaign_name LIKE '%0514%'
#         AND classification_rank_classification_id IS NOT NULL
#         AND classification_rank_classification_id != ''
    GROUP BY
        info_ext.parent_asins,
        product_sp.campaignId
    ORDER BY
        info_ext.parent_asins
            """
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query)
                result = await cursor.fetchall()
            if result:
                df = pd.DataFrame(result)
                print("get_profileId Data successfully!")
                return df
            else:
                return None
        except Exception as error:
            print("get_profileId Error while querying data:", error)
            return None

    async def get_serachterm(self, market,parent_asin,day,order):
        print("get_serachterm")
        # 低于 平均ACOS值 30% 以上的 campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            # 确保数据库连接已初始化
            if self.conn is None:
                await self.init()  # 初始化连接
            conn = self.conn
            # 使用异步游标执行 SQL 查询
            query = f"""
WITH a AS (
	SELECT DISTINCT adGroupId
	FROM
	amazon_product_info_extended a
	LEFT JOIN
	amazon_sp_productads_list b ON a.asin = b.asin AND a.market = b.market
	WHERE
	parent_asins = '{parent_asin}' OR a.asin = '{parent_asin}'
	),
	b AS (
    SELECT
        b.searchTerm,
        SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE , INTERVAL {int(float(day))} day) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m
    FROM
        amazon_search_term_reports_sp b
    JOIN
        a ON b.adGroupId = a.adGroupId
    WHERE
        b.date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY) AND CURRENT_DATE - INTERVAL 1 DAY
        AND b.market = '{market}'
        AND NOT (b.searchTerm LIKE 'b0%' AND LENGTH(b.searchTerm) = 10) -- 添加排除条件
    GROUP BY
        b.searchTerm
)
SELECT DISTINCT searchTerm FROM b
WHERE
 b.ORDER_1m >= {int(float(order))}
ORDER BY
 b.ORDER_1m DESC
            """
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query)
                result = await cursor.fetchall()
            if result:
                df = pd.DataFrame(result)
                print("get_profileId Data successfully!")
                return df['searchTerm'].tolist()
            else:
                return []
        except Exception as error:
            print("get_profileId Error while querying data:", error)
            return []

    async def campaign_info(self, num):
        try:
            # 确保数据库连接已初始化
            if self.conn is None:
                await self.init()  # 初始化连接
            conn = self.conn
            query = f"""
WITH campaign_list AS (
            SELECT DISTINCT campaignId
            FROM amazon_campaigns_list_sp
            WHERE state = 'ENABLED'
            AND targetingtype = 'MANUAL'
            AND market = '{self.market}'
            AND LOWER(campaign_name) LIKE '%deep%'
            AND LOWER(campaign_name) LIKE '%0514%'
#             and startDate < DATE_SUB(CURDATE(), INTERVAL 0 DAY )

)
SELECT
    a.campaignId,
    CASE
        -- 如果 count(1) 为 NULL，使用 0 代替，并且如果 700 - count(1) > 400，则返回 400
        WHEN {int(float(num))+200} - IFNULL(COUNT(b.targetId), 0) > 400 THEN 400
        ELSE {int(float(num))+200} - IFNULL(COUNT(b.targetId), 0)
    END AS calculated_value
FROM
    campaign_list a
LEFT JOIN
    amazon_targets_list_sp b
    ON a.campaignId = b.campaignId
    AND b.expression LIKE '%EXPANDED%'
    AND b.state = 'ENABLED'
GROUP BY
        a.campaignId
HAVING
    -- 如果 count(1) 为 NULL 或者 count(1) 小于 500，仍然返回结果
    IFNULL(COUNT(b.campaignId), 0) < {int(float(num))}
            """
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query)
                result = await cursor.fetchall()
                return result
        except Exception as error:
            print("get_profileId Error while querying data:", error)
            return None
