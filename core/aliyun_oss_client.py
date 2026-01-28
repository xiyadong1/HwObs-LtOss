#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
阿里云OSS客户端模块
负责阿里云OSS桶的连接和文件操作
"""

import threading
import datetime
from urllib.parse import urlparse, quote, unquote
import oss2
from log.logger import logger
from config.config_loader import config_loader


class AliyunOSSClient:
    """阿里云OSS客户端类"""
    
    def __init__(self, bucket_config=None):
        """
        初始化阿里云OSS客户端
        
        Args:
            bucket_config (dict, optional): 桶特定配置，包含bucket_name, prefix, exclude_suffixes等
        """
        aliyun_config = config_loader.get_aliyun_config()
        
        # 从配置获取阿里云OSS连接参数
        self.endpoint = aliyun_config.get('endpoint')
        self.access_key = aliyun_config.get('access_key')
        self.secret_key = aliyun_config.get('secret_key')
        
        # 验证必要的配置是否存在
        if not self.access_key or not self.secret_key:
            logger.warning("阿里云OSS认证信息不完整，请检查环境变量ALIYUN_ACCESS_KEY和ALIYUN_SECRET_KEY是否正确配置", module="aliyun_oss_client")
        
        # 如果提供了桶特定配置，则优先使用
        if bucket_config:
            self.bucket_name = bucket_config.get('bucket_name', aliyun_config.get('bucket_name'))
            self.prefix = bucket_config.get('prefix', aliyun_config.get('prefix', ''))
            self.exclude_suffixes = bucket_config.get('exclude_suffixes', aliyun_config.get('exclude_suffixes', []))
        else:
            self.bucket_name = aliyun_config.get('bucket_name')
            self.prefix = aliyun_config.get('prefix', '')
            self.exclude_suffixes = aliyun_config.get('exclude_suffixes', [])
        
        # 确保endpoint包含协议前缀
        if not self.endpoint.startswith('http://') and not self.endpoint.startswith('https://'):
            self.endpoint = f"https://{self.endpoint}"
        
        logger.debug(f"阿里云OSS认证信息：access_key={self.access_key[:10]}...，endpoint={self.endpoint}", module="aliyun_oss_client")
        
        # 使用阿里云官方SDK初始化客户端
        try:
            auth = oss2.Auth(self.access_key, self.secret_key)
            self.bucket = oss2.Bucket(auth, self.endpoint, self.bucket_name)
            logger.info(f"阿里云OSS客户端已初始化，桶名：{self.bucket_name}，endpoint：{self.endpoint}", module="aliyun_oss_client")
        except Exception as e:
            logger.error(f"阿里云OSS客户端初始化失败：{str(e)}", module="aliyun_oss_client")
            raise
    
    def list_objects(self):
        """
        列举阿里云OSS桶中的所有文件（支持前缀过滤和后缀排除）
        
        Yields:
            dict: 文件信息，包含key（文件路径）、size（文件大小）、etag（文件MD5）
        """
        try:
            # 使用官方SDK的迭代器列举所有文件
            for obj in oss2.ObjectIterator(self.bucket, prefix=self.prefix):
                file_path = obj.key
                file_size = obj.size
                etag = obj.etag.strip('"')  # 去除ETag的引号
                
                # 处理分片上传的ETag（格式：MD5值-分片数），只保留MD5值部分
                if '-' in etag:
                    etag = etag.split('-')[0]
                    logger.debug(f"阿里云OSS文件ETag包含分片标记，已处理：{file_path} -> {etag}", module="aliyun_oss_client")
                
                # 检查是否需要排除该文件（基于后缀）
                exclude = False
                for suffix in self.exclude_suffixes:
                    if file_path.endswith(suffix):
                        exclude = True
                        logger.debug(f"排除文件：{file_path}（后缀：{suffix}）", module="aliyun_oss_client")
                        break
                
                if not exclude:
                    yield {
                        "key": file_path,
                        "size": file_size,
                        "etag": etag
                    }
        except Exception as e:
            logger.error(f"阿里云OSS列举文件失败：{str(e)}", module="aliyun_oss_client")
            raise
    
    def get_object(self, object_key):
        """
        获取阿里云OSS对象的内容
        
        Args:
            object_key (str): OSS对象键
            
        Returns:
            bytes: 对象内容
        """
        try:
            result = self.bucket.get_object(object_key)
            return result.read()
        except Exception as e:
            logger.error(f"阿里云OSS获取文件失败：{object_key}，错误：{str(e)}", module="aliyun_oss_client")
            raise
    
    def get_object_stream(self, object_key):
        """
        获取阿里云OSS对象的流（适用于大文件）
        
        Args:
            object_key (str): OSS对象键
            
        Returns:
            object: 兼容OBS客户端响应结构的对象
        """
        try:
            # 官方SDK的get_object()返回的对象支持直接作为流使用
            stream_obj = self.bucket.get_object(object_key)
            
            # 创建一个简单的响应对象，兼容OBS客户端的接口
            response = type('Response', (), {})
            response.status = 200
            response.body = type('Body', (), {})
            response.body.response = stream_obj
            response.errorMessage = None
            
            return response
        except Exception as e:
            logger.error(f"阿里云OSS获取文件流失败：{object_key}，错误：{str(e)}", module="aliyun_oss_client")
            
            # 创建错误响应对象
            response = type('Response', (), {})
            response.status = 500
            response.body = None
            response.errorMessage = str(e)
            
            return response
    
    def get_object_metadata(self, object_key):
        """
        获取阿里云OSS对象的元数据
        
        Args:
            object_key (str): OSS对象键
            
        Returns:
            dict: 对象元数据
        """
        try:
            result = self.bucket.head_object(object_key)
            return result.headers
        except Exception as e:
            logger.error(f"阿里云OSS获取元数据失败：{object_key}，错误：{str(e)}", module="aliyun_oss_client")
            raise
    
    def close(self):
        """
        关闭阿里云OSS客户端连接
        """
        # 官方SDK的Bucket对象不需要显式关闭连接
        logger.info("阿里云OSS客户端已关闭", module="aliyun_oss_client")