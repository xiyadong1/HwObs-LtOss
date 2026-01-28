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
from core.aliyun_oss_client import AliyunOSSClient
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
        
        # 按桶统计进度信息
        self.bucket_total_files = {}  # 存储每个桶的总文件数
        self.bucket_processed_files = {}  # 存储每个桶的已处理文件数
        
        # 进度锁
        self.progress_lock = threading.Lock()
        
        # 记录上次显示的进度行数，用于实时更新
        self._last_displayed_lines = 0
        
        # 控制台输出锁，确保并发时输出不混乱
        self.console_lock = threading.Lock()
        
        # 优雅终止标志
        self.exit_flag = False
        
        # 文件数量限制（用于测试）
        self.file_limit = None
        
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
                oss_config = {
                    'bucket_name': bucket_mapping['oss_bucket'],
                    'target_prefix': bucket_mapping['oss_prefix']
                }
                
                # 根据源类型选择对应的客户端
                source_type = bucket_mapping.get('source_type', 'obs')  # 默认使用华为云OBS
                if source_type == 'aliyun':
                    # 使用阿里云OSS客户端
                    aliyun_config = {
                        'bucket_name': bucket_mapping['source_bucket'],
                        'prefix': bucket_mapping['source_prefix'],
                        'exclude_suffixes': bucket_mapping['exclude_suffixes']
                    }
                    source_client = AliyunOSSClient(bucket_config=aliyun_config)
                else:
                    # 使用华为云OBS客户端
                    obs_config = {
                        'bucket_name': bucket_mapping['source_bucket'],
                        'prefix': bucket_mapping['source_prefix'],
                        'exclude_suffixes': bucket_mapping['exclude_suffixes']
                    }
                    source_client = OBSClient(bucket_config=obs_config)
                
                oss_client = OSSClient(bucket_config=oss_config)
                migrate_task = MigrateTask(obs_client=source_client, oss_client=oss_client)
                
                # 根据文件大小选择合适的迁移方法
                file_size = file_info.get('size', 0)
                if migrate_task.should_use_streaming(file_size):
                    # 大文件使用流式迁移
                    migrate_task.migrate_file_stream(file_info)
                else:
                    # 小文件使用普通迁移
                    migrate_task.migrate_file(file_info)
                
                # 关闭客户端连接
                source_client.close()
                oss_client.close()
                
                # 更新进度（成功情况）
                with self.progress_lock:
                    self.processed_files += 1
                    # 获取当前桶名称并更新该桶的进度
                    bucket_name = bucket_mapping.get('source_bucket', bucket_mapping.get('obs_bucket'))
                    self.bucket_processed_files[bucket_name] = self.bucket_processed_files.get(bucket_name, 0) + 1
                
                # 标记任务完成
                self.task_queue.task_done()
                
            except Exception as e:
                logger.error(f"工作线程异常：{str(e)}", module="migrate_manager")
                # 更新进度（异常情况下也计数）
                with self.progress_lock:
                    self.processed_files += 1
                    # 获取当前桶名称并更新该桶的进度
                    if 'bucket_mapping' in locals():
                        # 根据源类型获取桶名称
                        bucket_name = bucket_mapping.get('source_bucket', bucket_mapping.get('obs_bucket'))
                        self.bucket_processed_files[bucket_name] = self.bucket_processed_files.get(bucket_name, 0) + 1
                self.task_queue.task_done()
    
    def monitor_progress(self):
        """
        监控迁移进度，分别显示每个桶的进度
        """
        start_time = time.time()
        
        while not self.exit_flag:
            with self.progress_lock:
                processed = self.processed_files
                current_total = self.total_files
                # 复制按桶统计的进度信息（避免并发修改问题）
                bucket_total = self.bucket_total_files.copy()
                bucket_processed = self.bucket_processed_files.copy()
            
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
                    # 计算整体进度
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
                    
                    # 构建每个桶的进度信息
                    bucket_progress_lines = []
                    for bucket_name, bucket_processed_count in bucket_processed.items():
                        bucket_total_count = bucket_total.get(bucket_name, 0)
                        if bucket_total_count > 0:
                            bucket_percentage = (bucket_processed_count / bucket_total_count * 100)
                            # 计算每个桶的预计完成时间
                            bucket_remaining_files = bucket_total_count - bucket_processed_count
                            if files_per_second > 0:
                                bucket_remaining_time = bucket_remaining_files / files_per_second
                                if bucket_remaining_time < 60:
                                    bucket_eta = f"{bucket_remaining_time:.0f}秒"
                                elif bucket_remaining_time < 3600:
                                    bucket_eta = f"{bucket_remaining_time/60:.0f}分钟"
                                else:
                                    bucket_eta = f"{bucket_remaining_time/3600:.1f}小时"
                            else:
                                bucket_eta = "-"
                            
                            bucket_progress_lines.append(
                                f"{bucket_name} 迁移进度：{bucket_processed_count}/{bucket_total_count} ({bucket_percentage:.2f}%) | "
                                f"速度：{files_per_second:.1f}文件/秒 | 预计完成时间：{bucket_eta}"
                            )
                    
                    # 构建完整的进度信息字符串
                    if bucket_progress_lines:
                        # 每5个文件更新一次，避免频繁闪烁
                        if processed % 5 == 0 or processed == 1:
                            import os
                            
                            # 构建进度信息
                            progress_lines = []
                            progress_lines.append(f"云存储→联通云OSS批量迁移工具")
                            progress_lines.append("=" * 50)
                            progress_lines.append(f"")
                            
                            # 整体进度
                            progress_lines.append(f"整体迁移进度：{processed}/{current_total} ({percentage:.2f}%)")
                            progress_lines.append(f"迁移速度：{files_per_second:.1f}文件/秒")
                            progress_lines.append(f"预计完成时间：{eta}")
                            progress_lines.append(f"")
                            
                            # 各桶进度
                            progress_lines.append("各桶迁移进度：")
                            for line in bucket_progress_lines:
                                progress_lines.append(f"  {line}")
                            
                            progress_lines.append(f"")
                            progress_lines.append("按Ctrl+C终止迁移")
                            
                            # 日志记录进度（在清除屏幕之前记录，避免干扰显示）
                            logger.info(f"迁移进度：{processed}/{current_total} ({percentage:.2f}%)", module="migrate_manager")
                            
                            # 输出进度信息
                            with self.console_lock:
                                # 清除屏幕
                                os.system('cls' if os.name == 'nt' else 'clear')
                                # 输出所有进度行
                                for line in progress_lines:
                                    print(line)
                                print("", end="", flush=True)
                    else:
                        # 如果没有桶的进度信息，显示整体进度
                        progress_msg = f"\r迁移进度：{processed}/{current_total} ({percentage:.2f}%) | "
                        progress_msg += f"速度：{files_per_second:.1f}文件/秒 | "
                        progress_msg += f"预计完成时间：{eta}"
                        with self.console_lock:
                            print(progress_msg, end="", flush=True)
                else:
                    # 总文件数还在增加中，显示已处理的文件数
                    with self.console_lock:
                        print(f"\r已处理 {processed} 个文件，文件列举中...", end="", flush=True)
            
            # 日志记录已移至进度显示内部，避免干扰输出
            
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
        # 根据源类型获取源桶名称
        source_type = bucket_mapping.get('source_type', 'obs')
        source_bucket = bucket_mapping.get('source_bucket', bucket_mapping.get('obs_bucket'))
        oss_bucket = bucket_mapping['oss_bucket']
        
        try:
            logger.info(f"开始处理桶映射：{source_type}桶={source_bucket} -> OSS桶={oss_bucket}", module="migrate_manager")
            
            # 为当前桶映射创建对应的源客户端
            if source_type == 'aliyun':
                source_config = {
                    'bucket_name': source_bucket,
                    'prefix': bucket_mapping.get('source_prefix', bucket_mapping.get('obs_prefix', '')),
                    'exclude_suffixes': bucket_mapping['exclude_suffixes']
                }
                source_client = AliyunOSSClient(bucket_config=source_config)
            else:
                source_config = {
                    'bucket_name': source_bucket,
                    'prefix': bucket_mapping.get('source_prefix', bucket_mapping.get('obs_prefix', '')),
                    'exclude_suffixes': bucket_mapping['exclude_suffixes']
                }
                source_client = OBSClient(bucket_config=source_config)
            
            # 列举当前桶中的文件（整个过程加锁，避免多桶输出交错）
            with self.console_lock:
                # 确保与之前的输出完全分离
                print(f"\n正在列举{source_type}桶 {source_bucket} 中的文件...", end="", flush=True)
                files = list(source_client.list_objects())
                file_count = len(files)
                # 先输出足够的空格清除当前行，再输出结果
                print(f"\r{' '*80}\r从{source_type}桶 {source_bucket} 找到 {file_count} 个文件\n", end="", flush=True)
            logger.info(f"从{source_type}桶 {source_bucket} 找到 {file_count} 个文件", module="migrate_manager")
            
            # 关闭源客户端
            source_client.close()
            
            # 将文件信息和桶映射信息一起保存，同时应用文件限制
            bucket_files_to_migrate = []
            for file_info in files:
                # 检查是否达到文件数量限制
                if self.file_limit is not None:
                    with self.progress_lock:
                        if self._added_files >= self.file_limit:
                            logger.info(f"已达到文件数量限制({self.file_limit})，停止添加新文件", module="migrate_manager")
                            break
                        # 只有在未达到限制时才添加文件
                        bucket_files_to_migrate.append({
                            'file_info': file_info,
                            'bucket_mapping': bucket_mapping
                        })
                        self._added_files += 1
                else:
                    # 没有文件限制，添加所有文件
                    bucket_files_to_migrate.append({
                        'file_info': file_info,
                        'bucket_mapping': bucket_mapping
                    })
            
            # 计算实际添加到任务队列中的文件数量
            actual_count = len(bucket_files_to_migrate)
            
            # 更新总文件数和按桶统计的总文件数
            with self.progress_lock:
                self.total_files += actual_count
                migrate_logger.update_total_files(self.total_files)
                # 初始化当前桶的统计信息
                if source_bucket not in self.bucket_total_files:
                    self.bucket_total_files[source_bucket] = 0
                    self.bucket_processed_files[source_bucket] = 0
                # 更新当前桶的总文件数
                self.bucket_total_files[source_bucket] += actual_count
            
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
            
            # 初始化文件限制计数器
            self._added_files = 0
            
            # 启动工作线程池（文件级并行）
            logger.info(f"启动{self.thread_count}个工作线程...", module="migrate_manager")
            for _ in range(self.thread_count):
                thread = threading.Thread(target=self.worker)
                thread.daemon = True
                thread.start()
                self.threads.append(thread)
            
            if self.bucket_mappings:
                # 多桶迁移模式
                logger.info("开始多桶迁移...", module="migrate_manager")
                
                # 收集所有桶的文件信息
                all_files_to_migrate = []
                
                # 为每个桶映射创建独立的执行线程（桶级并行）
                bucket_threads = []
                # 使用字典来存储每个桶的文件生成器
                bucket_file_generators = {}
                
                # 同步锁
                bucket_lock = threading.Lock()
                
                def process_bucket_and_generate(bucket_mapping):
                    """处理单个桶映射并将文件信息生成为迭代器"""
                    # 根据源类型获取源桶名称
                    source_type = bucket_mapping.get('source_type', 'obs')
                    source_bucket = bucket_mapping.get('source_bucket', bucket_mapping.get('obs_bucket'))
                    oss_bucket = bucket_mapping['oss_bucket']
                    
                    logger.info(f"开始处理桶映射：{source_type}桶={source_bucket} -> OSS桶={oss_bucket}", module="migrate_manager")
                    
                    # 为当前桶映射创建对应的源客户端
                    if source_type == 'aliyun':
                        source_config = {
                            'bucket_name': source_bucket,
                            'prefix': bucket_mapping.get('source_prefix', bucket_mapping.get('obs_prefix', '')),
                            'exclude_suffixes': bucket_mapping['exclude_suffixes']
                        }
                        source_client = AliyunOSSClient(bucket_config=source_config)
                    else:
                        source_config = {
                            'bucket_name': source_bucket,
                            'prefix': bucket_mapping.get('source_prefix', bucket_mapping.get('obs_prefix', '')),
                            'exclude_suffixes': bucket_mapping['exclude_suffixes']
                        }
                        source_client = OBSClient(bucket_config=source_config)
                    
                    # 列举当前桶中的文件（整个过程加锁，避免多桶输出交错）
                    with self.console_lock:
                        # 确保与之前的输出完全分离
                        print(f"\n正在列举{source_type}桶 {source_bucket} 中的文件...", end="", flush=True)
                        files = list(source_client.list_objects())
                        file_count = len(files)
                        # 先输出足够的空格清除当前行，再输出结果
                        print(f"\r{' '*80}\r从{source_type}桶 {source_bucket} 找到 {file_count} 个文件\n", end="", flush=True)
                    logger.info(f"从{source_type}桶 {source_bucket} 找到 {file_count} 个文件", module="migrate_manager")
                    
                    # 关闭源客户端
                    source_client.close()
                    
                    # 创建文件生成器
                    def file_generator():
                        for file_info in files:
                            yield {
                                'file_info': file_info,
                                'bucket_mapping': bucket_mapping
                            }
                    
                    # 存储生成器
                    with bucket_lock:
                        bucket_file_generators[source_bucket] = file_generator()
                
                # 启动线程处理每个桶映射
                for bucket_mapping in self.bucket_mappings:
                    thread = threading.Thread(
                        target=process_bucket_and_generate,
                        args=(bucket_mapping,)
                    )
                    thread.daemon = True
                    thread.start()
                    bucket_threads.append(thread)
                    source_type = bucket_mapping.get('source_type', 'obs')
                    source_bucket = bucket_mapping.get('source_bucket', bucket_mapping.get('obs_bucket'))
                    logger.info(f"已启动桶映射线程：{source_type}桶={source_bucket} -> OSS桶={bucket_mapping['oss_bucket']}", module="migrate_manager")
                
                # 等待所有桶映射线程完成文件列举
                logger.info("等待所有桶映射线程完成...", module="migrate_manager")
                for thread in bucket_threads:
                    thread.join()
                
                # 从多个桶中均匀获取文件，应用文件限制
                added_count = 0
                buckets = list(bucket_file_generators.keys())
                bucket_index = 0
                
                # 从每个桶中交替获取文件
                while (self.file_limit is None or added_count < self.file_limit) and len(buckets) > 0:
                    # 确保索引在有效范围内
                    if bucket_index >= len(buckets):
                        bucket_index = 0
                    
                    if len(buckets) == 0:
                        # 所有桶都没有更多文件了
                        break
                    
                    current_bucket = buckets[bucket_index]
                    file_generator = bucket_file_generators[current_bucket]
                    
                    try:
                        # 尝试从当前桶获取下一个文件
                        file_info_with_mapping = next(file_generator)
                        all_files_to_migrate.append(file_info_with_mapping)
                        added_count += 1
                        
                        # 检查是否达到文件限制
                        if self.file_limit is not None and added_count >= self.file_limit:
                            logger.info(f"已达到文件数量限制({self.file_limit})，停止添加新文件", module="migrate_manager")
                            break
                    except StopIteration:
                        # 当前桶没有更多文件了，从列表中移除
                        buckets.pop(bucket_index)
                        logger.info(f"OBS桶 {current_bucket} 的文件已全部处理完毕", module="migrate_manager")
                        continue
                    
                    # 移动到下一个桶
                    bucket_index = (bucket_index + 1) % len(buckets)
                
                # 将收集到的文件加入任务队列
                logger.info(f"共收集到{len(all_files_to_migrate)}个文件，准备开始迁移...", module="migrate_manager")
                
                # 更新总文件数和按桶统计的总文件数
                bucket_file_counts = {}
                for file_info_with_mapping in all_files_to_migrate:
                    bucket_mapping = file_info_with_mapping['bucket_mapping']
                    bucket_name = bucket_mapping.get('source_bucket', bucket_mapping.get('obs_bucket'))
                    bucket_file_counts[bucket_name] = bucket_file_counts.get(bucket_name, 0) + 1
                    
                    # 将文件加入任务队列
                    self.task_queue.put(file_info_with_mapping)
                
                # 更新总文件数和按桶统计的总文件数
                with self.progress_lock:
                    self.total_files = len(all_files_to_migrate)
                    migrate_logger.update_total_files(self.total_files)
                    
                    # 初始化按桶统计的总文件数
                    for bucket_name, count in bucket_file_counts.items():
                        if bucket_name not in self.bucket_total_files:
                            self.bucket_total_files[bucket_name] = 0
                            self.bucket_processed_files[bucket_name] = 0
                        self.bucket_total_files[bucket_name] += count
                
                logger.info(f"已将{len(all_files_to_migrate)}个文件加入任务队列", module="migrate_manager")
                
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
