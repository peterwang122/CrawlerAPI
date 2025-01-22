import os
import logging
from logging.handlers import TimedRotatingFileHandler

# 指定日志文件存储的目录
log_directory = os.path.dirname(os.path.abspath(__file__))  # 你希望日志文件生成的目录

# 确保目录存在，如果不存在则创建
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# 创建一个 TimedRotatingFileHandler，按天滚动日志文件
log_handler = TimedRotatingFileHandler(
    filename=os.path.join(log_directory, 'api.log'),  # 日志文件名，生成在指定目录
    when='midnight',                                    # 按天滚动
    interval=1,                                        # 每天滚动
    backupCount=7                                      # 只保留最近 7 天的日志
)

# 设置日志格式
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler.setFormatter(formatter)

# 配置全局日志记录器
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)
