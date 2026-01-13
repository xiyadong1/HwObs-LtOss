#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
迁移任务模块
负责单个文件的迁移任务和重试逻辑
"""

import time
import threading
from log.logger import logger
from log.migrate_logger import migrate_logger
from core.obs_client import get_obs_client
from core.oss_client import get_oss_client
from config.config_loader import config_loader


class MigrateTask:
    """
    文件迁移任务类
    """
    
    def __init__(self, obs_client=None, oss_client=None):
        """
        初始化迁移任务
        
        Args:
            obs_client (OBSClient, optional): OBS客户端实例
            oss_client (OSSClient, optional): OSS客户端实例
        """
        # 如果没有提供客户端实例，则使用默认客户端
        self.obs_client = obs_client or get_obs_client()
        self.oss_client = oss_client or get_oss_client()
        
        # 获取重试配置
        retry_config = config_loader.get_retry_config()
        self.max_retry = retry_config.get('max_attempts', 3)
        self.retry_interval = retry_config.get('interval', 5)
        
        logger.info(f"迁移任务已初始化，重试次数：{self.max_retry}，重试间隔：{self.retry_interval}秒", module="migrate_task")
    
    def migrate_file(self, file_info):
        """
        迁移单个文件
        
        Args:
            file_info (dict): 文件信息，包含key、size、etag
            
        Returns:
            dict: 迁移结果
        """
        obs_path = file_info.get('key')
        file_size = file_info.get('size')
        etag = file_info.get('etag')
        
        start_time = time.time()
        success = False
        error_msg = ""
        attempt = 0
        
        while attempt < self.max_retry and not success:
            attempt += 1
            
            try:
                logger.info(f"开始迁移文件（第{attempt}次尝试）：{obs_path}，大小：{file_size}字节", module="migrate_task")
                
                # 从OBS获取文件内容
                content = self.obs_client.get_object(obs_path)
                
                # 上传到OSS
                success, upload_error = self.oss_client.upload_file(obs_path, content, file_size, etag)
                
                if not success:
                    error_msg = upload_error
                    logger.error(f"文件迁移失败（第{attempt}次尝试）：{obs_path}，错误：{error_msg}", module="migrate_task")
                    
                    # 如果不是最后一次尝试，则等待重试
                    if attempt < self.max_retry:
                        logger.info(f"等待{self.retry_interval}秒后重试...", module="migrate_task")
                        time.sleep(self.retry_interval)
            
            except Exception as e:
                error_msg = str(e)
                logger.error(f"文件迁移异常（第{attempt}次尝试）：{obs_path}，错误：{error_msg}", module="migrate_task")
                
                # 如果不是最后一次尝试，则等待重试
                if attempt < self.max_retry:
                    logger.info(f"等待{self.retry_interval}秒后重试...", module="migrate_task")
                    time.sleep(self.retry_interval)
        
        # 计算迁移耗时
        duration = time.time() - start_time
        
        # 记录迁移结果
        status = "success" if success else "failed"
        migrate_logger.log_file_migrate(obs_path, self.oss_client.get_target_path(obs_path), 
                                      file_size, duration, status, error_msg, 
                                      obs_bucket=self.obs_client.bucket_name)
        
        logger.info(f"文件迁移完成：{obs_path}，状态：{status}，耗时：{duration:.2f}秒", module="migrate_task")
        
        return {
            "obs_path": obs_path,
            "status": status,
            "duration": duration,
            "error_msg": error_msg
        }
    
    def migrate_file_stream(self, file_info):
        """
        流式迁移单个文件（适用于大文件）
        
        Args:
            file_info (dict): 文件信息，包含key、size、etag
            
        Returns:
            dict: 迁移结果
        """
        obs_path = file_info.get('key')
        file_size = file_info.get('size')
        etag = file_info.get('etag')
        
        start_time = time.time()
        success = False
        error_msg = ""
        attempt = 0
        
        while attempt < self.max_retry and not success:
            attempt += 1
            
            try:
                logger.info(f"开始流式迁移文件（第{attempt}次尝试）：{obs_path}，大小：{file_size}字节", module="migrate_task")
                
                # 从OBS获取文件流
                resp = self.obs_client.get_object_stream(obs_path)
                
                if resp.status < 300:
                    # 上传到OSS（流式）
                    success, upload_error = self.oss_client.upload_file_stream(obs_path, resp.body.response, file_size, etag)
                    
                    if not success:
                        error_msg = upload_error
                        logger.error(f"流式迁移失败（第{attempt}次尝试）：{obs_path}，错误：{error_msg}", module="migrate_task")
                        
                        # 如果不是最后一次尝试，则等待重试
                        if attempt < self.max_retry:
                            logger.info(f"等待{self.retry_interval}秒后重试...", module="migrate_task")
                            time.sleep(self.retry_interval)
                else:
                    error_msg = resp.errorMessage
                    logger.error(f"OBS获取文件流失败（第{attempt}次尝试）：{obs_path}，错误：{error_msg}", module="migrate_task")
                    
                    # 如果不是最后一次尝试，则等待重试
                    if attempt < self.max_retry:
                        logger.info(f"等待{self.retry_interval}秒后重试...", module="migrate_task")
                        time.sleep(self.retry_interval)
            
            except Exception as e:
                error_msg = str(e)
                logger.error(f"流式迁移异常（第{attempt}次尝试）：{obs_path}，错误：{error_msg}", module="migrate_task")
                
                # 如果不是最后一次尝试，则等待重试
                if attempt < self.max_retry:
                    logger.info(f"等待{self.retry_interval}秒后重试...", module="migrate_task")
                    time.sleep(self.retry_interval)
        
        # 计算迁移耗时
        duration = time.time() - start_time
        
        # 记录迁移结果
        status = "success" if success else "failed"
        migrate_logger.log_file_migrate(obs_path, self.oss_client.get_target_path(obs_path), 
                                      file_size, duration, status, error_msg, 
                                      obs_bucket=self.obs_client.bucket_name)
        
        logger.info(f"流式迁移完成：{obs_path}，状态：{status}，耗时：{duration:.2f}秒", module="migrate_task")
        
        return {
            "obs_path": obs_path,
            "status": status,
            "duration": duration,
            "error_msg": error_msg
        }
    
    def should_use_streaming(self, file_size):
        """
        判断是否应该使用流式迁移
        
        Args:
            file_size (int): 文件大小（字节）
            
        Returns:
            bool: 是否使用流式迁移
        """
        # 获取并发配置
        concurrency_config = config_loader.get_concurrency_config()
        streaming_threshold = concurrency_config.get(
            'streaming_threshold', 5 * 1024 * 1024 * 10  # 默认50MB
        )

        # 对于大于流式迁移阈值的文件，使用流式迁移
        return file_size > streaming_threshold
