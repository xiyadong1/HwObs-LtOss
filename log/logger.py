#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日志模块
负责配置和提供日志记录功能
"""

import os
import logging
import threading
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
from config.config_loader import config_loader


class Logger:
    """日志记录器类"""
    
    def __init__(self):
        """
        初始化日志记录器
        """
        self.logger = logging.getLogger('obs_to_oss_migrator')
        self.logger.setLevel(self._get_log_level())
        self.logger.propagate = False
        
        # 确保日志目录存在
        self.log_path = config_loader.get('log.path', './migrate_log/')
        os.makedirs(self.log_path, exist_ok=True)
        
        # 配置日志格式
        self.formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(module_name)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # 添加控制台处理器
        self._add_console_handler()
        
        # 添加文件处理器（按小时分割）
        self._add_file_handler()
        
        # 线程锁，确保并发写入安全
        self.lock = threading.Lock()
    
    def _get_log_level(self):
        """
        获取日志级别
        
        Returns:
            int: 日志级别常量
        """
        level_str = config_loader.get('log.level', 'INFO').upper()
        level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        return level_map.get(level_str, logging.INFO)
    
    def _add_console_handler(self):
        """
        添加控制台日志处理器，只输出关键信息
        """
        console_handler = logging.StreamHandler()
        # 控制台只输出WARNING级别以上的日志（关键信息）
        console_handler.setLevel(logging.WARNING)
        # 简化控制台日志格式，在每条日志前添加换行符，避免与进度信息重叠
        console_formatter = logging.Formatter(
            '\n%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
    
    def _add_file_handler(self):
        """
        添加文件日志处理器（按小时分割）
        """
        log_file = os.path.join(self.log_path, f'migrate_{datetime.now().strftime("%Y-%m-%d")}.log')
        
        # 按小时分割日志文件
        file_handler = TimedRotatingFileHandler(
            filename=log_file,
            when='H',  # 按小时分割
            interval=1,
            backupCount=config_loader.get('log.backup_count', 7) * 24,  # 保留7天的日志
            encoding='utf-8'
        )
        
        file_handler.setLevel(self._get_log_level())
        file_handler.setFormatter(self.formatter)
        file_handler.suffix = '%H.log'  # 日志文件名后缀
        
        self.logger.addHandler(file_handler)
    
    def debug(self, message, module='main'):
        """
        记录DEBUG级别的日志
        
        Args:
            message (str): 日志消息
            module (str): 模块名称
        """
        with self.lock:
            self.logger.debug(message, extra={'module_name': module})
    
    def info(self, message, module='main'):
        """
        记录INFO级别的日志
        
        Args:
            message (str): 日志消息
            module (str): 模块名称
        """
        with self.lock:
            self.logger.info(message, extra={'module_name': module})
    
    def warning(self, message, module='main'):
        """
        记录WARNING级别的日志
        
        Args:
            message (str): 日志消息
            module (str): 模块名称
        """
        with self.lock:
            self.logger.warning(message, extra={'module_name': module})
    
    def error(self, message, module='main'):
        """
        记录ERROR级别的日志
        
        Args:
            message (str): 日志消息
            module (str): 模块名称
        """
        with self.lock:
            self.logger.error(message, extra={'module_name': module})
    
    def critical(self, message, module='main'):
        """
        记录CRITICAL级别的日志
        
        Args:
            message (str): 日志消息
            module (str): 模块名称
        """
        with self.lock:
            self.logger.critical(message, extra={'module_name': module})


# 单例模式
logger = Logger()