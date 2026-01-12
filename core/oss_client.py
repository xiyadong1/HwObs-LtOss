#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
联通云OSS客户端模块
负责OSS桶的连接和文件操作
"""

import os
import threading
import oss2
from log.logger import logger
from config.config_loader import config_loader


class OSSClient:
    """联通云OSS客户端类"""
    
    def __init__(self, bucket_config=None):
        """
        初始化OSS客户端
        
        Args:
            bucket_config (dict, optional): 桶特定配置，包含bucket_name, target_prefix等
        """
        oss_config = config_loader.get_oss_config()
        
        # 从配置获取OSS连接参数
        self.endpoint = oss_config.get('endpoint')
        self.access_key = oss_config.get('access_key')
        self.secret_key = oss_config.get('secret_key')
        
        # 如果提供了桶特定配置，则优先使用
        if bucket_config:
            self.bucket_name = bucket_config.get('bucket_name', oss_config.get('bucket_name'))
            self.target_prefix = bucket_config.get('target_prefix', oss_config.get('target_prefix', ''))
        else:
            self.bucket_name = oss_config.get('bucket_name')
            self.target_prefix = oss_config.get('target_prefix', '')
        
        # 确保endpoint包含协议前缀
        if not (self.endpoint.startswith('http://') or self.endpoint.startswith('https://')):
            logger.warning(f"OSS endpoint配置未包含协议前缀，自动添加https://：{self.endpoint}", module="oss_client")
            self.endpoint = f"https://{self.endpoint}"
        
        # 获取并发配置
        concurrency_config = config_loader.get_concurrency_config()
        self.chunk_size = concurrency_config.get('chunk_size', 5 * 1024 * 1024)  # 默认5MB
        
        # 获取OSS客户端配置
        oss_client_config = oss_config.get('client', {})
        connect_timeout = oss_client_config.get('connect_timeout', 30)
        connection_pool_size = oss_client_config.get('connection_pool_size', 100)
        
        # 初始化OSS认证
        auth = oss2.Auth(self.access_key, self.secret_key)
        
        # 配置OSS客户端参数，优化并发性能
        # 设置连接超时
        oss2.defaults.connect_timeout = connect_timeout
        # 设置连接池大小
        oss2.defaults.connection_pool_size = connection_pool_size
        
        # 初始化OSS桶客户端
        self.bucket = oss2.Bucket(auth, self.endpoint, self.bucket_name)
        
        logger.info(f"OSS客户端已初始化，桶名：{self.bucket_name}，endpoint：{self.endpoint}", module="oss_client")
    
    def get_target_path(self, obs_path):
        """
        根据OBS路径生成OSS目标路径
        
        Args:
            obs_path (str): OBS文件路径
            
        Returns:
            str: OSS目标路径
        """
        # 如果配置了目标前缀，则添加到路径前
        if self.target_prefix:
            return f"{self.target_prefix.rstrip('/')}/{obs_path.lstrip('/')}"
        return obs_path
    
    def upload_file(self, obs_path, content, file_size, etag):
        """
        上传文件到OSS（支持断点续传）
        
        Args:
            obs_path (str): OBS文件路径
            content (bytes): 文件内容
            file_size (int): 文件大小（字节）
            etag (str): 文件MD5值
            
        Returns:
            tuple: (success, error_msg)
        """
        oss_path = self.get_target_path(obs_path)
        
        try:
            # 检查文件是否已存在于OSS中
            exists = self.bucket.object_exists(oss_path)
            
            if exists:
                # 获取已存在文件的元数据
                existing_meta = self.bucket.head_object(oss_path)
                existing_etag = existing_meta.etag.strip('"')
                
                # 如果MD5相同，则跳过上传
                if existing_etag == etag:
                    logger.info(f"文件已存在且MD5相同，跳过上传：{oss_path}", module="oss_client")
                    return True, ""
                else:
                    logger.info(f"文件已存在但MD5不同，重新上传：{oss_path}", module="oss_client")
            
            # 上传文件
            # 对于小文件直接上传，大文件使用分片上传
            if file_size <= self.chunk_size:
                # 小文件上传
                result = self.bucket.put_object(oss_path, content)
                
                # 验证上传结果
                if result.status == 200:
                    uploaded_etag = result.etag.strip('"')
                    if uploaded_etag == etag:
                        logger.info(f"文件上传成功：{oss_path}", module="oss_client")
                        return True, ""
                    else:
                        logger.error(f"文件MD5验证失败：{oss_path}（预期：{etag}，实际：{uploaded_etag}）", module="oss_client")
                        return False, f"MD5验证失败（预期：{etag}，实际：{uploaded_etag}）"
                else:
                    logger.error(f"文件上传失败：{oss_path}，状态码：{result.status}", module="oss_client")
                    return False, f"上传失败，状态码：{result.status}"
            else:
                # 大文件分片上传
                upload_id = self.bucket.init_multipart_upload(oss_path).upload_id
                
                # 计算分片数量
                part_count = (file_size + self.chunk_size - 1) // self.chunk_size
                parts = []
                
                try:
                    # 上传分片
                    for i in range(part_count):
                        start = i * self.chunk_size
                        end = min(start + self.chunk_size, file_size)
                        part_content = content[start:end]
                        
                        result = self.bucket.upload_part(
                            oss_path,
                            upload_id,
                            i + 1,  # 分片编号从1开始
                            part_content
                        )
                        
                        if result.status == 200:
                            parts.append(oss2.models.PartInfo(i + 1, result.etag))
                        else:
                            logger.error(f"分片上传失败：{oss_path}，分片：{i+1}，状态码：{result.status}", module="oss_client")
                            return False, f"分片{i+1}上传失败，状态码：{result.status}"
                    
                    # 完成分片上传
                    result = self.bucket.complete_multipart_upload(oss_path, upload_id, parts)
                    
                    if result.status == 200:
                        uploaded_etag = result.etag.strip('"')
                        if uploaded_etag == etag:
                            logger.info(f"大文件上传成功：{oss_path}", module="oss_client")
                            return True, ""
                        else:
                            logger.error(f"大文件MD5验证失败：{oss_path}（预期：{etag}，实际：{uploaded_etag}）", module="oss_client")
                            return False, f"MD5验证失败（预期：{etag}，实际：{uploaded_etag}）"
                    else:
                        logger.error(f"完成分片上传失败：{oss_path}，状态码：{result.status}", module="oss_client")
                        return False, f"完成分片上传失败，状态码：{result.status}"
                    
                except Exception as e:
                    # 取消分片上传
                    self.bucket.abort_multipart_upload(oss_path, upload_id)
                    logger.error(f"大文件上传异常：{oss_path}，错误：{str(e)}", module="oss_client")
                    return False, str(e)
        
        except oss2.exceptions.AccessDenied as e:
            logger.error(f"OSS上传权限不足：{oss_path}，错误：{str(e)}", module="oss_client")
            return False, f"权限不足：{str(e)}"
        except oss2.exceptions.NoSuchBucket as e:
            logger.error(f"OSS桶不存在：{self.bucket_name}，错误：{str(e)}", module="oss_client")
            return False, f"桶不存在：{str(e)}"
        except oss2.exceptions.RequestError as e:
            logger.error(f"OSS网络请求错误：{oss_path}，错误：{str(e)}", module="oss_client")
            return False, f"网络请求错误：{str(e)}"
        except oss2.exceptions.ServerError as e:
            logger.error(f"OSS服务器错误：{oss_path}，错误：{str(e)}", module="oss_client")
            return False, f"服务器错误：{str(e)}"
        except oss2.exceptions.OssError as e:
            logger.error(f"OSS上传异常：{oss_path}，错误：{str(e)}", module="oss_client")
            return False, str(e)
        
        except Exception as e:
            logger.error(f"上传文件异常：{oss_path}，错误：{str(e)}", module="oss_client")
            return False, str(e)
    
    def upload_file_stream(self, obs_path, file_stream, file_size, etag):
        """
        流式上传文件到OSS（支持大文件分片上传）
        
        Args:
            obs_path (str): OBS文件路径
            file_stream (file-like object): 文件流
            file_size (int): 文件大小（字节）
            etag (str): 文件MD5值
            
        Returns:
            tuple: (success, error_msg)
        """
        oss_path = self.get_target_path(obs_path)
        
        try:
            # 检查文件是否已存在于OSS中
            exists = self.bucket.object_exists(oss_path)
            
            if exists:
                # 获取已存在文件的元数据
                existing_meta = self.bucket.head_object(oss_path)
                existing_etag = existing_meta.etag.strip('"')
                
                # 如果MD5相同，则跳过上传
                if existing_etag == etag:
                    logger.info(f"文件已存在且MD5相同，跳过上传：{oss_path}", module="oss_client")
                    return True, ""
            
            # 根据文件大小选择上传方式
            if file_size <= self.chunk_size:
                # 小文件直接流式上传
                result = self.bucket.put_object(oss_path, file_stream)
                
                if result.status == 200:
                    uploaded_etag = result.etag.strip('"')
                    if uploaded_etag == etag:
                        logger.info(f"小文件流式上传成功：{oss_path}", module="oss_client")
                        return True, ""
                    else:
                        logger.error(f"小文件流式上传MD5验证失败：{oss_path}（预期：{etag}，实际：{uploaded_etag}）", module="oss_client")
                        return False, f"MD5验证失败（预期：{etag}，实际：{uploaded_etag}）"
                else:
                    logger.error(f"小文件流式上传失败：{oss_path}，状态码：{result.status}", module="oss_client")
                    return False, f"上传失败，状态码：{result.status}"
            else:
                # 大文件分片流式上传
                upload_id = self.bucket.init_multipart_upload(oss_path).upload_id
                parts = []
                
                try:
                    # 分块读取并上传
                    part_number = 1
                    while True:
                        # 读取一块数据
                        chunk_data = file_stream.read(self.chunk_size)
                        if not chunk_data:
                            break
                        
                        # 上传分片
                        result = self.bucket.upload_part(
                            oss_path,
                            upload_id,
                            part_number,
                            chunk_data
                        )
                        
                        if result.status == 200:
                            parts.append(oss2.models.PartInfo(part_number, result.etag))
                            part_number += 1
                        else:
                            logger.error(f"大文件流式上传分片失败：{oss_path}，分片：{part_number}，状态码：{result.status}", module="oss_client")
                            return False, f"分片{part_number}上传失败，状态码：{result.status}"
                    
                    # 完成分片上传
                    result = self.bucket.complete_multipart_upload(oss_path, upload_id, parts)
                    
                    if result.status == 200:
                        uploaded_etag = result.etag.strip('"')
                        if uploaded_etag == etag:
                            logger.info(f"大文件流式上传成功：{oss_path}", module="oss_client")
                            return True, ""
                        else:
                            logger.error(f"大文件流式上传MD5验证失败：{oss_path}（预期：{etag}，实际：{uploaded_etag}）", module="oss_client")
                            return False, f"MD5验证失败（预期：{etag}，实际：{uploaded_etag}）"
                    else:
                        logger.error(f"大文件流式上传完成失败：{oss_path}，状态码：{result.status}", module="oss_client")
                        return False, f"完成分片上传失败，状态码：{result.status}"
                    
                except Exception as e:
                    # 取消分片上传
                    self.bucket.abort_multipart_upload(oss_path, upload_id)
                    logger.error(f"大文件流式上传异常：{oss_path}，错误：{str(e)}", module="oss_client")
                    return False, str(e)
        
        except oss2.exceptions.AccessDenied as e:
            logger.error(f"OSS流式上传权限不足：{oss_path}，错误：{str(e)}", module="oss_client")
            return False, f"权限不足：{str(e)}"
        except oss2.exceptions.NoSuchBucket as e:
            logger.error(f"OSS桶不存在：{self.bucket_name}，错误：{str(e)}", module="oss_client")
            return False, f"桶不存在：{str(e)}"
        except oss2.exceptions.RequestError as e:
            logger.error(f"OSS流式上传网络请求错误：{oss_path}，错误：{str(e)}", module="oss_client")
            return False, f"网络请求错误：{str(e)}"
        except oss2.exceptions.ServerError as e:
            logger.error(f"OSS流式上传服务器错误：{oss_path}，错误：{str(e)}", module="oss_client")
            return False, f"服务器错误：{str(e)}"
        except oss2.exceptions.OssError as e:
            logger.error(f"OSS流式上传异常：{oss_path}，错误：{str(e)}", module="oss_client")
            return False, str(e)
        
        except Exception as e:
            logger.error(f"流式上传文件异常：{oss_path}，错误：{str(e)}", module="oss_client")
            return False, str(e)
    
    def close(self):
        """
        关闭OSS客户端连接
        """
        # OSS客户端不需要显式关闭
        logger.info("OSS客户端已关闭", module="oss_client")


# 单例模式
global_oss_client = None
oss_client_lock = threading.Lock()


def get_oss_client():
    """
    获取OSS客户端单例
    
    Returns:
        OSSClient: OSS客户端实例
    """
    global global_oss_client
    
    with oss_client_lock:
        if global_oss_client is None:
            global_oss_client = OSSClient()
    
    return global_oss_client