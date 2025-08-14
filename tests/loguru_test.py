from loguru import logger
from utils.logger_config import app_logger as logger
logger.trace("跟踪信息")
logger.debug("调试信息")
logger.info("普通信息")
logger.success("成功信息")  # 这个很特别，绿色显示
logger.warning("警告信息")
logger.error("错误信息")
logger.critical("严重错误")

import sys
import os
from datetime import datetime
from pathlib import Path
import glob

logger.info(Path(__file__).parent.parent)


def add(aa:list):
    sum = 0
    for each in aa:
        sum += each
        logger.info(f"now adds {each} and sum = {sum}")
    logger.success(f"sum:{sum}")
    return sum

add([1,2,3,4,5])


@logger.catch
def my_function(x, y, z):
    return 1 / (x + y + z)

res = my_function(0,0,0)