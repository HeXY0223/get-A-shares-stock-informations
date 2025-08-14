from loguru import logger
import sys
import os
from datetime import datetime
from pathlib import Path
import glob


class LoggerManager:
    def __init__(self):
        self.log_file_path = None
        self.project_root = Path(__file__).parent.parent.parent  # 获取src目录
        self._setup_logger()

    def _get_next_log_file(self):
        """获取下一个可用的日志文件名"""
        # 使用项目根目录下的logs文件夹
        logs_dir = self.project_root / "logs"
        logs_dir.mkdir(exist_ok=True)

        today = datetime.now().strftime("%Y%m%d")
        pattern = str(logs_dir / f"{today}-*.log")

        existing_files = glob.glob(pattern)
        if not existing_files:
            next_num = 1
        else:
            numbers = []
            for file in existing_files:
                try:
                    num = int(Path(file).stem.split('-')[-1])
                    numbers.append(num)
                except ValueError:
                    continue
            next_num = max(numbers, default=0) + 1

        return str(logs_dir / f"{today}-{next_num}.log")

    def _setup_logger(self):
        """配置日志器"""
        # 移除默认处理器
        logger.remove()

        # 获取日志文件路径
        self.log_file_path = self._get_next_log_file()

        # 控制台输出
        logger.add(
            sys.stderr,
            format="<green>{time:MM-DD HH:mm:ss}</green> | <level>{level: <7}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> | <level>{message}</level>",
            level="INFO",
            colorize=True
        )

        # 文件输出
        logger.add(
            self.log_file_path,
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {process}:{thread} | {name}:{function}:{line} | {message}",
            level="TRACE",
            rotation="50 MB",
            retention="30 days",
            compression="zip",
            encoding="utf-8",
            enqueue=True
        )

        # 错误单独记录
        error_file = self.log_file_path.replace('.log', '_error.log')
        logger.add(
            error_file,
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {name}:{function}:{line} | {message} | {exception}",
            level="ERROR",
            retention="90 days"
        )

        logger.info(f"📝 日志系统启动成功")
        logger.info(f"📁 主日志文件: {self.log_file_path}")
        logger.info(f"🔴 错误日志文件: {error_file}")
        logger.info(f"📂 项目根目录: {self.project_root}")


# 创建全局实例
log_manager = LoggerManager()
app_logger = logger
