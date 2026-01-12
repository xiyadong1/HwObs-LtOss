#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
华为云OBS到联通云OSS批量迁移工具重试脚本
用于重新迁移之前失败的文件
"""

import sys
import os
import argparse
import traceback

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from log.logger import logger
from log.migrate_logger import migrate_logger
from core.obs_client import get_obs_client
from core.migrate_task import MigrateTask
from config.config_loader import config_loader


def main():
    """
    主函数
    """
    try:
        logger.info("华为云OBS→联通云OSS批量迁移工具（重试脚本）启动", module="retry_main")
        
        # 解析命令行参数
        parser = argparse.ArgumentParser(description="重试迁移失败的文件")
        parser.add_argument("-d", "--date", help="失败文件清单的日期（格式：YYYY-MM-DD），默认为今天")
        args = parser.parse_args()
        
        retry_date = args.date if args.date else None
        
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
            logger.error(f"缺少必要的环境变量：{', '.join(missing_vars)}", module="retry_main")
            print(f"错误：缺少必要的环境变量：{', '.join(missing_vars)}")
            print("请先配置以下环境变量：")
            for var in required_env_vars:
                print(f"  - {var}")
            sys.exit(1)
        
        # 加载失败文件清单
        failed_list = migrate_logger.load_failed_list(retry_date)
        
        if not failed_list:
            logger.info("没有找到需要重试的失败文件", module="retry_main")
            print("没有找到需要重试的失败文件")
            sys.exit(0)
        
        logger.info(f"共找到{len(failed_list)}个需要重试的失败文件", module="retry_main")
        print(f"开始重试迁移{len(failed_list)}个失败文件...")
        
        # 获取OBS客户端和迁移任务
        obs_client = get_obs_client()
        migrate_task = MigrateTask()
        
        # 重试迁移每个失败文件
        success_count = 0
        failed_count = 0
        new_failed_list = []
        
        for i, failed_item in enumerate(failed_list):
            obs_path = failed_item.get('obs_path')
            
            print(f"\r正在重试 {i+1}/{len(failed_list)}: {obs_path}", end="", flush=True)
            logger.info(f"开始重试文件 {i+1}/{len(failed_list)}: {obs_path}", module="retry_main")
            
            try:
                # 获取文件信息
                # 注意：这里需要重新获取文件信息，因为原始的失败记录可能没有完整的文件信息
                # 我们需要单独获取这个文件的信息
                file_info = None
                
                # 列举单个文件（通过前缀匹配）
                # 这里使用listObjects API，Prefix设置为文件路径，MaxKeys设置为1
                resp = obs_client.client.listObjects(
                    Bucket=obs_client.bucket_name,
                    Prefix=obs_path,
                    MaxKeys=1
                )
                
                if resp.status < 300 and hasattr(resp.body, 'contents'):
                    for content in resp.body.contents:
                        if content.key == obs_path:
                            file_info = {
                                "key": content.key,
                                "size": int(content.size),
                                "etag": content.etag.strip('"')
                            }
                            break
                
                if not file_info:
                    logger.error(f"文件不存在：{obs_path}", module="retry_main")
                    failed_count += 1
                    new_failed_list.append({
                        "obs_path": obs_path,
                        "error_msg": "文件不存在"
                    })
                    continue
                
                # 重试迁移文件
                result = migrate_task.migrate_file(file_info)
                
                if result['status'] == "success":
                    success_count += 1
                    logger.info(f"文件重试成功：{obs_path}", module="retry_main")
                else:
                    failed_count += 1
                    new_failed_list.append({
                        "obs_path": obs_path,
                        "error_msg": result['error_msg']
                    })
                    logger.error(f"文件重试失败：{obs_path}，错误：{result['error_msg']}", module="retry_main")
                    
            except Exception as e:
                failed_count += 1
                error_msg = str(e)
                new_failed_list.append({
                    "obs_path": obs_path,
                    "error_msg": error_msg
                })
                logger.error(f"文件重试异常：{obs_path}，错误：{error_msg}", module="retry_main")
                logger.error(traceback.format_exc(), module="retry_main")
        
        # 生成重试报告
        print(f"\n\n重试迁移完成！")
        print(f"总文件数：{len(failed_list)}")
        print(f"成功数：{success_count}")
        print(f"失败数：{failed_count}")
        
        logger.info("=" * 60, module="retry_main")
        logger.info("重试迁移任务完成！", module="retry_main")
        logger.info(f"总文件数：{len(failed_list)}", module="retry_main")
        logger.info(f"成功数：{success_count}", module="retry_main")
        logger.info(f"失败数：{failed_count}", module="retry_main")
        
        # 生成新的失败文件清单
        if new_failed_list:
            today = retry_date if retry_date else migrate_logger.today
            failed_file = os.path.join(migrate_logger.log_path, f'failed_{today}_retry.txt')
            
            with open(failed_file, 'w', encoding='utf-8') as f:
                for item in new_failed_list:
                    f.write(f"{item['obs_path']}\t{item['error_msg']}\n")
            
            print(f"\n新的失败文件清单已保存至：{failed_file}")
            logger.info(f"新的失败文件清单已保存至：{failed_file}", module="retry_main")
        
        logger.info("=" * 60, module="retry_main")
        logger.info("华为云OBS→联通云OSS批量迁移工具（重试脚本）执行完成", module="retry_main")
        
        sys.exit(0)
        
    except KeyboardInterrupt:
        logger.info("用户中断了重试任务", module="retry_main")
        print("\n用户中断了重试任务")
        sys.exit(1)
        
    except Exception as e:
        logger.critical(f"重试任务发生致命错误：{str(e)}", module="retry_main")
        logger.critical(traceback.format_exc(), module="retry_main")
        print(f"错误：重试任务发生致命错误：{str(e)}")
        print("详细错误信息请查看日志文件")
        sys.exit(1)


if __name__ == "__main__":
    main()