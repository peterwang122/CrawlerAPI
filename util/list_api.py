import sys
from method.sp_api import searchterm_CrawlerAsin, list_CrawlerAsin
from log.logger_config import logger
async def list_api(data):
    if data['type'] == 'SP':
        code, info, e = await sp_api(data)
    # elif data['type'] == 'SD':
    #     code = sd_api(data)
    return code, info, e

async def sp_api(data):
    if data['require'] == 'list':
        if data['position'] == 'SearchtermCrawlerAsin':
            print(data)
            code,info,e = await searchterm_CrawlerAsin(data['db'],data['brand'],data['market'],data['ID'],data['text'],data['num'])
            logger.info(data)
            logger.info(info)
        elif data['position'] == 'CrawlerAsin':
            print(data)
            code,info,e = await list_CrawlerAsin(data['db'],data['brand'],data['market'],data['text'],data['ID'])
            logger.info(data)
            logger.info(info)
    return code, info, e
