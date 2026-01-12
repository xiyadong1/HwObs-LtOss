#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
迁移管理器模块
负责管理多线程迁移任务和监控迁移进度
"""

import time
import threading
import queue
from log.logger import logger
from log.migrate_logger import migrate_logger
from core.obs_client import OBSClient
from core.oss_client import OSSClient
from core.migrate_task import MigrateTask
from config.config_loader import config_loader


class MigrateManager:
    """
    迁移管理器类
    """
    
    def __init__(self):
        """
        初始化迁移管理器
        """
        # 获取并发配置
        concurrency_config = config_loader.get_concurrency_config()
        self.thread_count = concurrency_config.get('thread_count', 50)
        
        # 获取迁移配置
        migrate_config = config_loader.get_migrate_config()
        self.progress_interval = migrate_config.get('progress_interval', 5)
        
        # 获取桶映射配置
        self.bucket_mappings = config_loader.get_bucket_mappings()
        
        # 任务队列
        self.task_queue = queue.Queue()
        
        # 线程池
        self.threads = []
        
        # 进度信息
        self.total_files = 0
        self.processed_files = 0
        self.start_time = None
        
        # 进度锁
        self.progress_lock = threading.Lock()
        
        logger.info(f"迁移管理器已初始化，并发线程数：{self.thread_count}", module="migrate_manager")
        logger.info(f"桶映射配置：{self.bucket_mappings}", module="migrate_manager")
    
    def worker(self):
        """
        工作线程函数
        """
        while True:
            try:
                # 从队列获取任务
                task_info = self.task_queue.get(timeout=1)
                
                if task_info is None:
                    # 收到终止信号
                    break
                
                file_info = task_info['file_info']
                bucket_mapping = task_info['bucket_mapping']
                
                # 为每个任务创建独立的客户端实例
                obs_config = {
                    'bucket_name': bucket_mapping['obs_bucket'],
                    'prefix': bucket_mapping['obs_prefix'],
                    'exclude_suffixes': bucket_mapping['exclude_suffixes']
                }
                
                oss_config = {
                    'bucket_name': bucket_mapping['oss_bucket'],
                    'target_prefix': bucket_mapping['oss_prefix']
                }
                
                obs_client = OBSClient(bucket_config=obs_config)
                oss_client = OSSClient(bucket_config=oss_config)
                migrate_task = MigrateTask(obs_client=obs_client, oss_client=oss_client)
                
                # 根据文件大小选择合适的迁移方法
                file_size = file_info.get('size', 0)
                if migrate_task.should_use_streaming(file_size):
                    # 大文件使用流式迁移
                    migrate_task.migrate_file_stream(file_info)
                else:
                    # 小文件使用普通迁移
                    migrate_task.migrate_file(file_info)
                
                # 关闭客户端连接
                obs_client.close()
                oss_client.close()
                
                # 更新进度
                with self.progress_lock:
                    self.processed_files += 1
                
                # 标记任务完成
                self.task_queue.task_done()
                
            except queue.Empty:
                # 队列为空，退出线程
                break
            except Exception as e:
                logger.error(f"工作线程异常：{str(e)}", module="migrate_manager")
                self.task_queue.task_done()
    
    def monitor_progress(self):
        """
        监控迁移进度
        """
        while self.processed_files < self.total_files:
            with self.progress_lock:
                processed = self.processed_files
            
            percentage = (processed / self.total_files * 100) if self.total_files > 0 else 0
            
            # 控制台输出进度
            print(f"\r迁移进度：{processed}/{self.total_files} ({percentage:.2f}%)", end="", flush=True)
            
            # 日志记录进度
            logger.info(f"迁移进度：{processed}/{self.total_files} ({percentage:.2f}%)", module="migrate_manager")
            
            # 等待指定时间后再次更新进度
            time.sleep(self.progress_interval)
    
    def start_migration(self):
        """
        开始迁移任务
        """
        logger.info("开始批量迁移任务", module="migrate_manager")
        self.start_time = time.time()
        
        # 1. 获取所有需要迁移的文件
        logger.info("开始列举OBS桶中的文件...", module="migrate_manager")
        
        all_files_to_migrate = []
        
        if self.bucket_mappings:
            # 多桶迁移模式
            for bucket_mapping in self.bucket_mappings:
                logger.info(f"处理桶映射：OBS桶={bucket_mapping['obs_bucket']} -> OSS桶={bucket_mapping['oss_bucket']}", module="migrate_manager")
                
                # 为当前桶映射创建OBS客户端
                obs_config = {
                    'bucket_name': bucket_mapping['obs_bucket'],
                    'prefix': bucket_mapping['obs_prefix'],
                    'exclude_suffixes': bucket_mapping['exclude_suffixes']
                }
                
                obs_client = OBSClient(bucket_config=obs_config)
                
                # 列举当前桶中的文件
                files = list(obs_client.list_objects())
                file_count = len(files)
                
                logger.info(f"从OBS桶 {bucket_mapping['obs_bucket']} 找到 {file_count} 个文件", module="migrate_manager")
                
                # 将文件信息和桶映射信息一起保存
                for file_info in files:
                    all_files_to_migrate.append({
                        'file_info': file_info,
                        'bucket_mapping': bucket_mapping
                    })
                
                # 关闭OBS客户端
                obs_client.close()
        else:
            # 单桶迁移模式（向后兼容）
            from core.obs_client import get_obs_client
            
            obs_client = get_obs_client()
            files = list(obs_client.list_objects())
            file_count = len(files)
            
            logger.info(f"从OBS桶 {obs_client.bucket_name} 找到 {file_count} 个文件", module="migrate_manager")
            
            # 为每个文件创建默认的桶映射信息
            default_bucket_mapping = {
                'obs_bucket': obs_client.bucket_name,
                'oss_bucket': config_loader.get_oss_config().get('bucket_name'),
                'obs_prefix': obs_client.prefix,
                'oss_prefix': config_loader.get_oss_config().get('target_prefix', ''),
                'exclude_suffixes': obs_client.exclude_suffixes
            }
            
            for file_info in files:
                all_files_to_migrate.append({
                    'file_info': file_info,
                    'bucket_mapping': default_bucket_mapping
                })
        
        self.total_files = len(all_files_to_migrate)
        
        logger.info(f"共找到{self.total_files}个文件需要迁移", module="migrate_manager")
        migrate_logger.update_total_files(self.total_files)
        
        if self.total_files == 0:
            logger.warning("没有找到需要迁移的文件", module="migrate_manager")
            return
        
        # 2. 重置进度计数
        self.processed_files = 0
        
        # 3. 启动工作线程
        logger.info(f"启动{self.thread_count}个工作线程...", module="migrate_manager")
        for _ in range(self.thread_count):
            thread = threading.Thread(target=self.worker)
            thread.daemon = True
            thread.start()
            self.threads.append(thread)
        
        # 4. 启动进度监控线程
        progress_thread = threading.Thread(target=self.monitor_progress)
        progress_thread.daemon = True
        progress_thread.start()
        
        # 5. 将任务加入队列
        logger.info("将文件迁移任务加入队列...", module="migrate_manager")
        for task_info in all_files_to_migrate:
            self.task_queue.put(task_info)
        
        # 6. 等待所有任务完成
        logger.info("等待所有迁移任务完成...", module="migrate_manager")
        self.task_queue.join()
        
        # 7. 等待所有线程退出
        for _ in range(self.thread_count):
            self.task_queue.put(None)  # 发送终止信号
        
        for thread in self.threads:
            thread.join(timeout=5)
        
        # 8. 输出最终进度
        with self.progress_lock:
            processed = self.processed_files
        
        percentage = (processed / self.total_files * 100) if self.total_files > 0 else 0
        print(f"\r迁移进度：{processed}/{self.total_files} ({percentage:.2f}%)\n", flush=True)
        
        # 9. 生成汇总报告
        self.generate_report()
    
    def generate_report(self):
        """
        生成迁移汇总报告
        """
        end_time = time.time()
        total_duration = end_time - self.start_time
        
        report = migrate_logger.generate_daily_report()
        
        logger.info("=" * 60, module="migrate_manager")
        logger.info("迁移任务完成！", module="migrate_manager")
        logger.info(f"总迁移文件数：{report.get('total_files')}", module="migrate_manager")
        logger.info(f"成功迁移数：{report.get('success_files')}", module="migrate_manager")
        logger.info(f"失败迁移数：{report.get('failed_files')}", module="migrate_manager")
        logger.info(f"总耗时：{total_duration:.2f}秒", module="migrate_manager")
        
        if report.get('failed_files', 0) > 0:
            logger.warning(f"失败文件清单已保存至：./migrate_log/failed_{report.get('date')}.txt", module="migrate_manager")
            logger.warning(f"可使用重试脚本重新迁移失败文件", module="migrate_manager")
        
        logger.info("汇总报告已生成：./migrate_log/report_{}.json".format(report.get('date')), module="migrate_manager")
        logger.info("=" * 60, module="migrate_manager")
