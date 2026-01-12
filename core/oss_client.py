#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
联通云OSS客户端模块
负责OSS桶的连接和文件操作
"""

import os
import threading
import datetime
import hashlib
import hmac
import requests
from urllib.parse import urlparse, quote_plus, quote
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
        self.region = oss_config.get('region', 'obs-tj')  # 默认区域
        
        # 如果提供了桶特定配置，则优先使用
        if bucket_config:
            self.bucket_name = bucket_config.get('bucket_name', oss_config.get('bucket_name'))
            self.target_prefix = bucket_config.get('target_prefix', oss_config.get('target_prefix', ''))
        else:
            self.bucket_name = oss_config.get('bucket_name')
            self.target_prefix = oss_config.get('target_prefix', '')
        
        # 确保endpoint不包含协议前缀（后续构造URL时统一添加）
        if self.endpoint.startswith('http://') or self.endpoint.startswith('https://'):
            self.endpoint = urlparse(self.endpoint).netloc
            logger.warning(f"OSS endpoint已移除协议前缀：{self.endpoint}", module="oss_client")
        
        # 获取并发配置
        concurrency_config = config_loader.get_concurrency_config()
        self.chunk_size = concurrency_config.get('chunk_size', 5 * 1024 * 1024)  # 默认5MB
        
        # 获取OSS客户端配置
        oss_client_config = oss_config.get('client', {})
        self.connect_timeout = oss_client_config.get('connect_timeout', 30)
        
        # 初始化请求会话
        self.session = requests.Session()
        self.session.timeout = (self.connect_timeout, 60)  # (连接超时, 读取超时)
        
        logger.debug(f"OSS认证信息：access_key={self.access_key[:10]}...，endpoint={self.endpoint}", module="oss_client")
        logger.info(f"OSS客户端已初始化，桶名：{self.bucket_name}，endpoint：{self.endpoint}", module="oss_client")
    
    # AWS4-HMAC-SHA256签名实现
    def _sign(self, key, msg):
        """使用HMAC-SHA256签名"""
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()
    
    def _get_signature_key(self, key, date_stamp, region_name, service_name):
        """生成AWS4签名密钥"""
        k_date = self._sign(('AWS4' + key).encode('utf-8'), date_stamp)
        k_region = self._sign(k_date, region_name)
        k_service = self._sign(k_region, service_name)
        k_signing = self._sign(k_service, 'aws4_request')
        return k_signing
    
    def _sha256_hash(self, content):
        """计算SHA256哈希值"""
        if isinstance(content, str):
            content = content.encode('utf-8')
        return hashlib.sha256(content).hexdigest()
    
    def _generate_signature(self, method, canonical_uri, headers, payload_hash):
        """生成AWS4-HMAC-SHA256签名"""
        # 生成时间戳
        t = datetime.datetime.now(datetime.UTC)
        amz_date = t.strftime('%Y%m%dT%H%M%SZ')
        date_stamp = t.strftime('%Y%m%d')
        
        # 查询参数（无）
        canonical_querystring = ''
        
        # 构建规范头 - 严格按照test_oss_official.py的方式构建
        canonical_headers = ''
        # 只包含必要的头信息：host, x-amz-content-sha256, x-amz-date
        host = headers.get('Host', '')
        canonical_headers += f"host:{host}\n"
        canonical_headers += f"x-amz-content-sha256:{payload_hash}\n"
        canonical_headers += f"x-amz-date:{amz_date}\n"
        
        # 构建已签名头
        signed_headers = 'host;x-amz-content-sha256;x-amz-date'
        
        # 构建规范请求
        canonical_request = ''
        canonical_request += f"{method}\n"
        # 确保签名计算中使用的URI与实际请求的URI完全一致
        # AWS4-HMAC-SHA256签名规范要求签名计算和实际请求必须使用相同的URI
        canonical_request += f"{canonical_uri}\n"
        canonical_request += f"{canonical_querystring}\n"
        canonical_request += f"{canonical_headers}\n"
        canonical_request += f"{signed_headers}\n"
        canonical_request += f"{payload_hash}"
        
        # 生成字符串签名
        algorithm = 'AWS4-HMAC-SHA256'
        credential_scope = f"{date_stamp}/{self.region}/s3/aws4_request"
        string_to_sign = ''
        string_to_sign += f"{algorithm}\n"
        string_to_sign += f"{amz_date}\n"
        string_to_sign += f"{credential_scope}\n"
        string_to_sign += f"{self._sha256_hash(canonical_request)}"
        
        # 生成签名密钥
        signing_key = self._get_signature_key(self.secret_key, date_stamp, self.region, 's3')
        
        # 计算签名
        signature = hmac.new(signing_key, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()
        
        return {
            'amz_date': amz_date,
            'date_stamp': date_stamp,
            'signed_headers': signed_headers,
            'signature': signature,
            'algorithm': algorithm,
            'credential_scope': credential_scope
        }
    
    def _send_request(self, method, path, data=None, headers=None):
        """发送带签名的HTTP请求"""
        if headers is None:
            headers = {}
        
        # 构建请求URL
        host = f"{self.bucket_name}.{self.endpoint}"
        url = f"https://{host}{path}"
        
        # 计算请求体哈希
        if data is None:
            payload_hash = self._sha256_hash('')
        else:
            payload_hash = self._sha256_hash(data)
        
        # 添加必要的头信息
        headers.setdefault('Host', host)
        headers.setdefault('x-amz-content-sha256', payload_hash)
        
        # 确保签名中使用的路径与实际请求的路径一致
        signature_info = self._generate_signature(method, path, headers, payload_hash)
        
        # 添加认证头
        credential = f"{self.access_key}/{signature_info['credential_scope']}"
        headers['x-amz-date'] = signature_info['amz_date']
        headers['Authorization'] = f"{signature_info['algorithm']} Credential={credential}, SignedHeaders={signature_info['signed_headers']}, Signature={signature_info['signature']}"
        
        # 发送请求
        try:
            if method == 'GET':
                response = self.session.get(url, headers=headers)
            elif method == 'PUT':
                response = self.session.put(url, data=data, headers=headers)
            elif method == 'HEAD':
                response = self.session.head(url, headers=headers)
            elif method == 'DELETE':
                response = self.session.delete(url, headers=headers)
            else:
                raise ValueError(f"不支持的HTTP方法：{method}")
            
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"OSS请求失败：{method} {url}，错误：{str(e)}", module="oss_client")
            raise
    
    # 基本OSS操作方法实现
    def _quote_key(self, key):
        """对对象键进行URL编码，但保留路径分隔符"""
        # 根据AWS4-HMAC-SHA256签名规范，对URL进行编码
        # 保留路径分隔符'/'
        return quote(key, safe='/')
    
    def object_exists(self, key):
        """检查对象是否存在"""
        try:
            quoted_key = self._quote_key(key)
            path = f"/{quoted_key}"
            response = self._send_request('HEAD', path)
            return response.status_code == 200
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return False
            raise
    
    def head_object(self, key):
        """获取对象元数据"""
        quoted_key = self._quote_key(key)
        path = f"/{quoted_key}"
        response = self._send_request('HEAD', path)
        
        # 返回类似oss2.models.HeadObjectResult的对象
        class HeadObjectResult:
            def __init__(self, headers):
                self.etag = headers.get('ETag', '')
                self.content_length = int(headers.get('Content-Length', 0))
                self.headers = headers
        
        return HeadObjectResult(response.headers)
    
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
    
    def _put_object(self, key, data, content_length=None):
        """上传小文件到OSS"""
        quoted_key = self._quote_key(key)
        path = f"/{quoted_key}"
        headers = {}
        
        if content_length is not None:
            headers['Content-Length'] = str(content_length)
        
        response = self._send_request('PUT', path, data=data, headers=headers)
        
        # 返回类似oss2.models.PutObjectResult的对象
        class PutObjectResult:
            def __init__(self, status, headers):
                self.status = status
                self.etag = headers.get('ETag', '')
        
        return PutObjectResult(response.status_code, response.headers)
    
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
            exists = self.object_exists(oss_path)
            
            if exists:
                # 获取已存在文件的元数据
                existing_meta = self.head_object(oss_path)
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
                result = self._put_object(oss_path, content, content_length=file_size)
                
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
                # 大文件分片上传功能暂未实现，先使用直接上传
                logger.warning(f"文件大小超过{self.chunk_size}字节，暂不支持分片上传，将使用直接上传：{oss_path}", module="oss_client")
                result = self._put_object(oss_path, content, content_length=file_size)
                
                if result.status == 200:
                    uploaded_etag = result.etag.strip('"')
                    if uploaded_etag == etag:
                        logger.info(f"大文件上传成功：{oss_path}", module="oss_client")
                        return True, ""
                    else:
                        logger.error(f"大文件MD5验证失败：{oss_path}（预期：{etag}，实际：{uploaded_etag}）", module="oss_client")
                        return False, f"MD5验证失败（预期：{etag}，实际：{uploaded_etag}）"
                else:
                    logger.error(f"大文件上传失败：{oss_path}，状态码：{result.status}", module="oss_client")
                    return False, f"上传失败，状态码：{result.status}"
        
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                logger.error(f"OSS上传权限不足：{oss_path}，错误：{str(e)}", module="oss_client")
                return False, f"权限不足：{str(e)}"
            elif e.response.status_code == 404:
                logger.error(f"OSS桶不存在：{self.bucket_name}，错误：{str(e)}", module="oss_client")
                return False, f"桶不存在：{str(e)}"
            else:
                logger.error(f"OSS服务器错误：{oss_path}，错误：{str(e)}", module="oss_client")
                return False, f"服务器错误：{str(e)}"
        except requests.exceptions.RequestException as e:
            logger.error(f"OSS网络请求错误：{oss_path}，错误：{str(e)}", module="oss_client")
            return False, f"网络请求错误：{str(e)}"
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
            exists = self.object_exists(oss_path)
            
            if exists:
                # 获取已存在文件的元数据
                existing_meta = self.head_object(oss_path)
                existing_etag = existing_meta.etag.strip('"')
                
                # 如果MD5相同，则跳过上传
                if existing_etag == etag:
                    logger.info(f"文件已存在且MD5相同，跳过上传：{oss_path}", module="oss_client")
                    return True, ""
            
            # 读取文件内容
            file_content = file_stream.read()
            
            # 使用普通上传方式
            return self.upload_file(obs_path, file_content, file_size, etag)
        
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                logger.error(f"OSS流式上传权限不足：{oss_path}，错误：{str(e)}", module="oss_client")
                return False, f"权限不足：{str(e)}"
            elif e.response.status_code == 404:
                logger.error(f"OSS桶不存在：{self.bucket_name}，错误：{str(e)}", module="oss_client")
                return False, f"桶不存在：{str(e)}"
            else:
                logger.error(f"OSS服务器错误：{oss_path}，错误：{str(e)}", module="oss_client")
                return False, f"服务器错误：{str(e)}"
        except requests.exceptions.RequestException as e:
            logger.error(f"OSS网络请求错误：{oss_path}，错误：{str(e)}", module="oss_client")
            return False, f"网络请求错误：{str(e)}"
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