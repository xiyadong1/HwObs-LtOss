#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置加载模块
负责从YAML配置文件和环境变量加载配置参数
"""

import os
import yaml
from dotenv import load_dotenv


class ConfigLoader:
    """配置加载器类"""
    
    def __init__(self, config_file="config/config.yaml"):
        """
        初始化配置加载器
        
        Args:
            config_file (str): 配置文件路径
        """
        self.config_file = config_file
        self.config = {}
        self.load_config()
    
    def load_config(self):
        """
        加载配置文件和环境变量
        """
        # 加载环境变量
        load_dotenv()
        
        # 加载YAML配置文件
        with open(self.config_file, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        # 从环境变量加载华为云OBS认证信息
        self.config['obs']['access_key'] = os.getenv('OBS_ACCESS_KEY')
        self.config['obs']['secret_key'] = os.getenv('OBS_SECRET_KEY')
        
        # 从环境变量加载联通云OSS认证信息
        self.config['oss']['access_key'] = os.getenv('OSS_ACCESS_KEY')
        self.config['oss']['secret_key'] = os.getenv('OSS_SECRET_KEY')
        
        # 从环境变量加载阿里云OSS认证信息
        if 'aliyun' in self.config:
            env_access_key = os.getenv('ALIYUN_ACCESS_KEY')
            if env_access_key:
                self.config['aliyun']['access_key'] = env_access_key
                
            env_secret_key = os.getenv('ALIYUN_SECRET_KEY')
            if env_secret_key:
                self.config['aliyun']['secret_key'] = env_secret_key
    
    def get(self, key_path, default=None):
        """
        获取配置值
        
        Args:
            key_path (str): 配置键路径，如 "concurrency.thread_count"
            default: 默认值
            
        Returns:
            配置值
        """
        keys = key_path.split('.')
        value = self.config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value
    
    def get_obs_config(self):
        """
        获取华为云OBS配置
        
        Returns:
            dict: OBS配置字典
        """
        return self.config.get('obs', {})
    
    def get_oss_config(self):
        """
        获取联通云OSS配置
        
        Returns:
            dict: OSS配置字典
        """
        return self.config.get('oss', {})
    
    def get_concurrency_config(self):
        """
        获取并发配置
        
        Returns:
            dict: 并发配置字典
        """
        return self.config.get('concurrency', {})
    
    def get_retry_config(self):
        """
        获取重试配置
        
        Returns:
            dict: 重试配置字典
        """
        return self.config.get('retry', {})
    
    def get_log_config(self):
        """
        获取日志配置
        
        Returns:
            dict: 日志配置字典
        """
        return self.config.get('log', {})
    
    def get_migrate_config(self):
        """
        获取迁移配置
        
        Returns:
            dict: 迁移配置字典
        """
        return self.config.get('migrate', {})
    
    def get_bucket_mappings(self):
        """
        获取桶映射配置
        
        Returns:
            list: 桶映射列表
        """
        return self.config.get('bucket_mappings', [])
    
    def get_aliyun_config(self):
        """
        获取阿里云OSS配置
        
        Returns:
            dict: 阿里云OSS配置字典
        """
        return self.config.get('aliyun', {})


# 单例模式
config_loader = ConfigLoader()
