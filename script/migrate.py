#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
华为云OBS到联通云OSS批量迁移工具启动脚本
"""

import sys
import os
import traceback
import argparse
from dotenv import load_dotenv

# 加载.env文件
load_dotenv()

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from log.logger import logger
from core.migrate_manager import MigrateManager


def main():
    """
    主函数
    """
    try:
        logger.info("华为云OBS→联通云OSS批量迁移工具启动", module="main")
        
        # 解析命令行参数
        parser = argparse.ArgumentParser(description="华为云OBS到联通云OSS批量迁移工具")
        parser.add_argument('--limit', type=int, default=None, help='限制迁移的文件数量，用于测试')
        args = parser.parse_args()
        
        # 检查环境变量是否配置
        required_env_vars = [
            'OBS_ACCESS_KEY',
            'OBS_SECRET_KEY',
            'OSS_ACCESS_KEY',
            'OSS_SECRET_KEY'
        ]
        
        missing_vars = []
        for var in required_env_vars:
            if var not in os.environ:
                missing_vars.append(var)
        
        if missing_vars:
            logger.error(f"缺少必要的环境变量：{', '.join(missing_vars)}", module="main")
            print(f"错误：缺少必要的环境变量：{', '.join(missing_vars)}")
            print("请先配置以下环境变量：")
            for var in required_env_vars:
                print(f"  - {var}")
            sys.exit(1)
        
        # 初始化迁移管理器并启动迁移
        migrate_manager = MigrateManager()
        # 传递limit参数给迁移管理器
        if args.limit:
            migrate_manager.file_limit = args.limit
        migrate_manager.start_migration()
        
        logger.info("华为云OBS→联通云OSS批量迁移工具执行完成", module="main")
        sys.exit(0)
        
    except KeyboardInterrupt:
        logger.info("用户中断了迁移任务", module="main")
        print("\n用户中断了迁移任务")
        sys.exit(1)
        
    except Exception as e:
        logger.critical(f"迁移任务发生致命错误：{str(e)}", module="main")
        logger.critical(traceback.format_exc(), module="main")
        print(f"错误：迁移任务发生致命错误：{str(e)}")
        print("详细错误信息请查看日志文件")
        sys.exit(1)


if __name__ == "__main__":
    main()