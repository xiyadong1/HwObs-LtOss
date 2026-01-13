#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
华为云OBS客户端模块
负责OBS桶的连接和文件操作
"""

import threading
from obs import ObsClient as HuaweiObsClient
from log.logger import logger
from config.config_loader import config_loader


class OBSClient:
    """华为云OBS客户端类"""
    
    def __init__(self, bucket_config=None):
        """
        初始化OBS客户端
        
        Args:
            bucket_config (dict, optional): 桶特定配置，包含bucket_name, prefix, exclude_suffixes等
        """
        obs_config = config_loader.get_obs_config()
        
        # 从配置获取OBS连接参数
        self.endpoint = obs_config.get('endpoint')
        self.access_key = obs_config.get('access_key')
        self.secret_key = obs_config.get('secret_key')
        
        # 调试信息
        logger.debug(f"bucket_config: {bucket_config}", module="obs_client")
        logger.debug(f"obs_config: {obs_config}", module="obs_client")
        
        # 如果提供了桶特定配置，则优先使用
        if bucket_config:
            self.bucket_name = bucket_config.get('bucket_name', obs_config.get('bucket_name'))
            self.prefix = bucket_config.get('prefix', obs_config.get('prefix', ''))
            self.exclude_suffixes = bucket_config.get('exclude_suffixes', obs_config.get('exclude_suffixes', []))
        else:
            self.bucket_name = obs_config.get('bucket_name')
            self.prefix = obs_config.get('prefix', '')
            self.exclude_suffixes = obs_config.get('exclude_suffixes', [])
        
        # 调试信息
        logger.debug(f"self.bucket_name: {self.bucket_name}", module="obs_client")
        
        # 初始化OBS客户端
        self.client = HuaweiObsClient(
            access_key_id=self.access_key,
            secret_access_key=self.secret_key,
            server=self.endpoint
        )
        
        logger.info(f"OBS客户端已初始化，桶名：{self.bucket_name}", module="obs_client")
    
    def list_objects(self):
        """
        列举OBS桶中的所有文件（支持前缀过滤和后缀排除）
        
        Yields:
            dict: 文件信息，包含key（文件路径）、size（文件大小）、etag（文件MD5）
        """
        marker = None
        max_keys = 1000  # 每次列举的最大文件数
        
        while True:
            # 列举OBS桶中的文件
            try:
                # 调试：打印传递给listObjects的参数
                logger.debug(f"调用listObjects参数：Bucket={self.bucket_name}, Prefix={self.prefix}, Marker={marker}, MaxKeys={max_keys}", module="obs_client")
                
                # 修复参数名：使用bucketName而不是Bucket（华为云SDK使用驼峰命名）
                resp = self.client.listObjects(
                    bucketName=self.bucket_name,
                    prefix=self.prefix,
                    marker=marker,
                    max_keys=max_keys
                )
                
                if resp.status < 300:
                    # 处理列举结果
                    for content in resp.body.contents:
                        file_path = content.key
                        file_size = int(content.size)
                        etag = content.etag.strip('"')  # 去除ETag的引号
                        
                        # 检查是否需要排除该文件（基于后缀）
                        exclude = False
                        for suffix in self.exclude_suffixes:
                            if file_path.endswith(suffix):
                                exclude = True
                                logger.debug(f"排除文件：{file_path}（后缀：{suffix}）", module="obs_client")
                                break
                        
                        if not exclude:
                            yield {
                                "key": file_path,
                                "size": file_size,
                                "etag": etag
                            }
                    
                    # 检查是否还有更多文件
                    has_more = False
                    if hasattr(resp.body, 'is_truncated') and resp.body.is_truncated:
                        has_more = True
                    
                    if has_more:
                        # 获取下一个marker
                        if hasattr(resp.body, 'next_marker'):
                            marker = resp.body.next_marker
                        elif hasattr(resp.body, 'nextMarker'):
                            marker = resp.body.nextMarker
                        else:
                            # 如果没有marker，使用最后一个文件的key
                            if resp.body.contents:
                                marker = resp.body.contents[-1].key
                            else:
                                has_more = False
                    
                    if not has_more:
                        break
                else:
                    logger.error(f"OBS列举文件失败：{resp.errorMessage}", module="obs_client")
                    raise Exception(f"OBS列举文件失败：{resp.errorMessage}")
                    
            except Exception as e:
                logger.error(f"OBS列举文件异常：{str(e)}", module="obs_client")
                raise
    
    def get_object(self, object_key):
        """
        获取OBS对象的内容
        
        Args:
            object_key (str): OBS对象键
            
        Returns:
            bytes: 对象内容
        """
        try:
            resp = self.client.getObject(
                bucketName=self.bucket_name,
                objectKey=object_key
            )
            
            if resp.status < 300:
                # HTTPResponse对象没有content属性，使用read()方法获取内容
                return resp.body.response.read()
            else:
                logger.error(f"OBS获取文件失败：{object_key}，错误：{resp.errorMessage}", module="obs_client")
                raise Exception(f"OBS获取文件失败：{resp.errorMessage}")
                
        except Exception as e:
            logger.error(f"OBS获取文件异常：{object_key}，错误：{str(e)}", module="obs_client")
            raise
    
    def get_object_stream(self, object_key):
        """
        获取OBS对象的流（适用于大文件）
        
        Args:
            object_key (str): OBS对象键
            
        Returns:
            tuple: (响应对象, 流对象)
        """
        try:
            resp = self.client.getObject(
                bucketName=self.bucket_name,
                objectKey=object_key,
                loadStreamInMemory=False  # 不加载到内存，使用流
            )
            
            return resp
            
        except Exception as e:
            logger.error(f"OBS获取文件流失败：{object_key}，错误：{str(e)}", module="obs_client")
            raise
    
    def get_object_metadata(self, object_key):
        """
        获取OBS对象的元数据
        
        Args:
            object_key (str): OBS对象键
            
        Returns:
            dict: 对象元数据
        """
        try:
            # 检查headObject方法的参数名
            resp = self.client.headObject(
                bucketName=self.bucket_name,
                objectKey=object_key
            )
            
            if resp.status < 300:
                return resp.header
            else:
                logger.error(f"OBS获取元数据失败：{object_key}，错误：{resp.errorMessage}", module="obs_client")
                raise Exception(f"OBS获取元数据失败：{resp.errorMessage}")
                
        except Exception as e:
            logger.error(f"OBS获取元数据异常：{object_key}，错误：{str(e)}", module="obs_client")
            raise
    
    def close(self):
        """
        关闭OBS客户端连接
        """
        if self.client:
            self.client.close()
            logger.info("OBS客户端已关闭", module="obs_client")


# 单例模式
global_obs_client = None
obs_client_lock = threading.Lock()


def get_obs_client():
    """
    获取OBS客户端单例
    
    Returns:
        OBSClient: OBS客户端实例
    """
    global global_obs_client
    
    with obs_client_lock:
        if global_obs_client is None:
            global_obs_client = OBSClient()
    
    return global_obs_client