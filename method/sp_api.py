from util.searchterm_asin import searchterm_asin
from util.expanded_asin import expanded_asin


async def searchterm_CrawlerAsin(db, brand, market, day, order,num):
    try:
        info1 = await searchterm_asin(db, brand, market, day, order,num)

        return 200, info1, None

        # 在返回之前异步执行任务
        # 等待任务完成（如果需要等待结果的话）
        # info2 =  task
        # info.append(info2)

    except Exception as e:
        print(e)
        return 500, None, e  # Internal Server Error

async def list_CrawlerAsin(db, brand, market,num,day):
    try:
        info = []
        info1 = await expanded_asin(db, brand, market,num,day)
        info.append(info1)
        # info2 = asyncio.run(searchterm_asin(self.db, self.brand, self.market, day, order))
        # info.append(info2)
        print(info)
        return 200, info, None
    except Exception as e:
        print(e)
        import traceback
        full_stack = traceback.print_exc()
        return 500, None, str(e)  # Internal Server Error
