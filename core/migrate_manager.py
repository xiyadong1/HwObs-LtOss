#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
迁移管理器模块
负责管理多线程迁移任务和监控迁移进度
"""

import time
import threading
import queue
import signal
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
        
        # 控制台输出锁，确保并发时输出不混乱
        self.console_lock = threading.Lock()
        
        # 优雅终止标志
        self.exit_flag = False
        
        logger.info(f"迁移管理器已初始化，并发线程数：{self.thread_count}", module="migrate_manager")
        logger.info(f"桶映射配置：{self.bucket_mappings}", module="migrate_manager")
    
    def _signal_handler(self, signum, frame):
        """
        信号处理函数，用于捕获Ctrl+C信号
        """
        logger.info("接收到终止信号，正在优雅终止迁移任务...", module="migrate_manager")
        self.exit_flag = True
        
        # 清空任务队列
        while not self.task_queue.empty():
            try:
                self.task_queue.get(timeout=0.1)
                self.task_queue.task_done()
            except queue.Empty:
                break
        
        logger.info("已清空任务队列", module="migrate_manager")
    
    def worker(self):
        """
        工作线程函数
        """
        while True:
            try:
                # 检查退出标志
                if self.exit_flag:
                    break
                
                try:
                    # 从队列获取任务
                    task_info = self.task_queue.get(timeout=1)
                except queue.Empty:
                    # 队列为空，继续等待
                    continue
                
                if task_info is None:
                    # 收到终止信号
                    break
                
                # 再次检查退出标志
                if self.exit_flag:
                    self.task_queue.task_done()
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
                
            except Exception as e:
                logger.error(f"工作线程异常：{str(e)}", module="migrate_manager")
                # 更新进度（异常情况下也计数）
                with self.progress_lock:
                    self.processed_files += 1
                self.task_queue.task_done()
    
    def monitor_progress(self):
        """
        监控迁移进度
        """
        start_time = time.time()
        
        while not self.exit_flag:
            with self.progress_lock:
                processed = self.processed_files
                current_total = self.total_files
            
            if processed == 0:
                if current_total == 0:
                    # 正在列举文件，显示提示信息
                    with self.console_lock:
                        print("\r正在列举文件...", end="", flush=True)
                else:
                    # 已完成文件列举，但还没有开始处理文件
                    with self.console_lock:
                        print(f"\r已找到 {current_total} 个文件，准备开始迁移...", end="", flush=True)
            else:
                if current_total > 0:
                    percentage = (processed / current_total * 100)
                    elapsed_time = time.time() - start_time
                    files_per_second = processed / elapsed_time
                    remaining_files = current_total - processed
                    remaining_time = remaining_files / files_per_second if files_per_second > 0 else 0
                    
                    # 格式化预计剩余时间
                    if remaining_time < 60:
                        eta = f"{remaining_time:.0f}秒"
                    elif remaining_time < 3600:
                        eta = f"{remaining_time/60:.0f}分钟"
                    else:
                        eta = f"{remaining_time/3600:.1f}小时"
                    
                    # 控制台输出详细进度信息
                    progress_msg = f"\r迁移进度：{processed}/{current_total} ({percentage:.2f}%) | "
                    progress_msg += f"速度：{files_per_second:.1f}文件/秒 | "
                    progress_msg += f"预计完成时间：{eta}"
                    with self.console_lock:
                        print(progress_msg, end="", flush=True)
                else:
                    # 总文件数还在增加中，显示已处理的文件数
                    with self.console_lock:
                        print(f"\r已处理 {processed} 个文件，文件列举中...", end="", flush=True)
            
            # 日志记录进度
            logger.info(f"迁移进度：{processed}/{current_total} ({(processed/current_total*100) if current_total>0 else 0:.2f}%)", module="migrate_manager")
            
            # 等待指定时间后再次更新进度
            time.sleep(self.progress_interval)
            
            # 检查是否所有任务都已完成
            if self.task_queue.empty() and processed > 0 and processed == current_total:
                break
        
        # 如果是因为退出标志而停止，输出终止信息
        if self.exit_flag:
            with self.progress_lock:
                current_processed = self.processed_files
                current_total = self.total_files
            print(f"\r迁移已终止，已处理：{current_processed}/{current_total}", flush=True)
    
    def _process_bucket_mapping(self, bucket_mapping):
        """
        处理单个桶映射的迁移任务
        
        Args:
            bucket_mapping (dict): 桶映射配置
        """
        obs_bucket = bucket_mapping['obs_bucket']
        oss_bucket = bucket_mapping['oss_bucket']
        
        try:
            logger.info(f"开始处理桶映射：OBS桶={obs_bucket} -> OSS桶={oss_bucket}", module="migrate_manager")
            
            # 为当前桶映射创建OBS客户端
            obs_config = {
                'bucket_name': obs_bucket,
                'prefix': bucket_mapping['obs_prefix'],
                'exclude_suffixes': bucket_mapping['exclude_suffixes']
            }
            
            obs_client = OBSClient(bucket_config=obs_config)
            
            # 列举当前桶中的文件（整个过程加锁，避免多桶输出交错）
            with self.console_lock:
                # 确保与之前的输出完全分离
                print(f"\n正在列举OBS桶 {obs_bucket} 中的文件...", end="", flush=True)
                files = list(obs_client.list_objects())
                file_count = len(files)
                # 先输出足够的空格清除当前行，再输出结果
                print(f"\r{' '*80}\r从OBS桶 {obs_bucket} 找到 {file_count} 个文件\n", end="", flush=True)
            logger.info(f"从OBS桶 {obs_bucket} 找到 {file_count} 个文件", module="migrate_manager")
            
            # 关闭OBS客户端
            obs_client.close()
            
            # 将文件信息和桶映射信息一起保存
            bucket_files_to_migrate = []
            for file_info in files:
                bucket_files_to_migrate.append({
                    'file_info': file_info,
                    'bucket_mapping': bucket_mapping
                })
            
            # 更新总文件数
            with self.progress_lock:
                self.total_files += file_count
                migrate_logger.update_total_files(self.total_files)
            
            # 将当前桶映射的文件加入任务队列
            for task_info in bucket_files_to_migrate:
                # 检查退出标志
                if self.exit_flag:
                    logger.info(f"迁移已终止，停止为桶 {obs_bucket} 添加新任务", module="migrate_manager")
                    break
                self.task_queue.put(task_info)
            
            logger.info(f"桶映射 {obs_bucket} -> {oss_bucket} 的文件已全部加入任务队列", module="migrate_manager")
            
        except Exception as e:
            logger.error(f"处理桶映射 {obs_bucket} -> {oss_bucket} 时发生异常：{str(e)}", module="migrate_manager")
            raise
    
    def start_migration(self):
        """
        开始迁移任务
        """
        # 控制台输出启动信息
        print("华为云OBS→联通云OSS批量迁移工具启动")
        print("=" * 60)
        logger.info("开始批量迁移任务", module="migrate_manager")
        self.start_time = time.time()
        
        # 注册信号处理函数
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        try:
            # 重置进度计数
            self.processed_files = 0
            self.total_files = 0
            
            # 启动工作线程池（文件级并行）
            logger.info(f"启动{self.thread_count}个工作线程...", module="migrate_manager")
            for _ in range(self.thread_count):
                thread = threading.Thread(target=self.worker)
                thread.daemon = True
                thread.start()
                self.threads.append(thread)
            
            if self.bucket_mappings:
                # 多桶并行迁移模式
                logger.info("开始多桶并行迁移...", module="migrate_manager")
                
                # 为每个桶映射创建独立的执行线程（桶级并行）
                bucket_threads = []
                for bucket_mapping in self.bucket_mappings:
                    thread = threading.Thread(
                        target=self._process_bucket_mapping,
                        args=(bucket_mapping,)
                    )
                    thread.daemon = True
                    thread.start()
                    bucket_threads.append(thread)
                    logger.info(f"已启动桶映射线程：OBS桶={bucket_mapping['obs_bucket']} -> OSS桶={bucket_mapping['oss_bucket']}", module="migrate_manager")
                
                # 等待所有桶映射线程完成文件列举和任务添加
                logger.info("等待所有桶映射线程完成...", module="migrate_manager")
                for thread in bucket_threads:
                    thread.join()
                
                # 所有文件列举完成后，启动进度监控线程
                progress_thread = threading.Thread(target=self.monitor_progress)
                progress_thread.daemon = True
                progress_thread.start()
                
            else:
                # 单桶迁移模式（向后兼容）
                logger.info("开始单桶迁移...", module="migrate_manager")
                from core.obs_client import get_obs_client
                
                obs_client = get_obs_client()
                with self.console_lock:
                    print(f"正在列举OBS桶 {obs_client.bucket_name} 中的文件...", end="", flush=True)
                files = list(obs_client.list_objects())
                file_count = len(files)
                with self.console_lock:
                    print(f"\r从OBS桶 {obs_client.bucket_name} 找到 {file_count} 个文件\n", end="", flush=True)
                logger.info(f"从OBS桶 {obs_client.bucket_name} 找到 {file_count} 个文件", module="migrate_manager")
                
                # 更新总文件数
                self.total_files = file_count
                migrate_logger.update_total_files(self.total_files)
                
                # 文件列举完成后，启动进度监控线程
                progress_thread = threading.Thread(target=self.monitor_progress)
                progress_thread.daemon = True
                progress_thread.start()
                
                # 为每个文件创建默认的桶映射信息
                default_bucket_mapping = {
                    'obs_bucket': obs_client.bucket_name,
                    'oss_bucket': config_loader.get_oss_config().get('bucket_name'),
                    'obs_prefix': obs_client.prefix,
                    'oss_prefix': config_loader.get_oss_config().get('target_prefix', ''),
                    'exclude_suffixes': obs_client.exclude_suffixes
                }
                
                # 将文件加入任务队列
                for file_info in files:
                    task_info = {
                        'file_info': file_info,
                        'bucket_mapping': default_bucket_mapping
                    }
                    self.task_queue.put(task_info)
            
            logger.info(f"共找到{self.total_files}个文件需要迁移", module="migrate_manager")
            
            if self.total_files == 0:
                logger.warning("没有找到需要迁移的文件", module="migrate_manager")
                return
            
            # 等待所有任务完成或终止
            logger.info("等待所有迁移任务完成...", module="migrate_manager")
            
            # 等待队列中的所有任务完成
            # 使用线程来等待，以便能够响应退出信号
            def wait_for_tasks():
                self.task_queue.join()
                return True
            
            # 使用线程等待任务完成，设置超时以便检查退出标志
            wait_thread = threading.Thread(target=wait_for_tasks)
            wait_thread.daemon = True
            wait_thread.start()
            
            # 等待任务完成或收到退出信号
            while wait_thread.is_alive() and not self.exit_flag:
                time.sleep(1)
            
            # 7. 等待所有线程退出
            logger.info("正在停止工作线程...", module="migrate_manager")
            
            # 发送终止信号
            for _ in range(self.thread_count):
                self.task_queue.put(None)
            
            # 等待所有线程退出
            for thread in self.threads:
                thread.join(timeout=5)
            
            # 8. 输出最终进度
            with self.progress_lock:
                processed = self.processed_files
            
            percentage = (processed / self.total_files * 100) if self.total_files > 0 else 0
            print(f"\r迁移进度：{processed}/{self.total_files} ({percentage:.2f}%)\n", flush=True)
            
            # 9. 生成汇总报告
            self.generate_report()
            
        except KeyboardInterrupt:
            # 捕获Ctrl+C信号
            logger.info("接收到Ctrl+C信号，正在优雅终止...", module="migrate_manager")
            self._signal_handler(signal.SIGINT, None)
            
            # 等待所有线程退出
            for thread in self.threads:
                thread.join(timeout=5)
            
            # 输出最终进度
            with self.progress_lock:
                processed = self.processed_files
            
            print(f"\r迁移已终止，已处理：{processed}/{self.total_files}\n", flush=True)
            
            # 生成终止报告
            logger.info("迁移任务已终止", module="migrate_manager")
        except Exception as e:
            logger.error(f"迁移任务异常：{str(e)}", module="migrate_manager")
            raise
    
    def generate_report(self):
        """
        生成迁移汇总报告
        """
        end_time = time.time()
        total_duration = end_time - self.start_time
        
        report = migrate_logger.generate_daily_report()
        
        # 控制台输出详细汇总信息
        print("\n" + "=" * 60)
        print("迁移任务完成！")
        print("=" * 60)
        
        # 全局汇总
        print(f"总迁移文件数：{report.get('total_files')}")
        print(f"成功迁移数：{report.get('success_files')}")
        print(f"失败迁移数：{report.get('failed_files')}")
        print(f"总耗时：{total_duration:.2f}秒")
        print(f"平均速度：{report.get('total_files')/total_duration:.1f}文件/秒")
        print()
        
        # 按桶汇总
        bucket_stats = report.get('bucket_stats', {})
        if bucket_stats:
            print("按桶迁移统计：")
            print("-" * 40)
            print("{:<20} {:<8} {:<8} {:<8} {:<10}".format("桶名称", "总数", "成功", "失败", "成功率"))
            print("-" * 40)
            
            for bucket_name, stats in bucket_stats.items():
                total = stats.get('total', 0)
                success = stats.get('success', 0)
                failed = stats.get('failed', 0)
                success_rate = (success / total * 100) if total > 0 else 0
                print("{:<20} {:<8} {:<8} {:<8} {:<10.1f}%".format(
                    bucket_name, total, success, failed, success_rate))
            print("-" * 40)
        
        # 失败文件信息
        if report.get('failed_files', 0) > 0:
            print()
            print(f"失败文件清单已保存至：./migrate_log/failed_{report.get('date')}.txt")
            print(f"可使用重试脚本重新迁移失败文件")
        
        print("\n" + "=" * 60)
        
        # 日志记录详细信息
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
