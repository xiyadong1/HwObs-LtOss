#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
联通云OSS操作工具
支持批量和单独的上传、下载、删除功能
完全独立，不依赖于迁移工具的代码
"""

import argparse
import os
import sys
import time
import logging
import threading
import hashlib
import hmac
import requests
import yaml
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, quote, urlencode

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(os.path.dirname(__file__), 'oss_tool.log'), encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class ConfigLoader:
    """独立的配置加载器"""
    
    def __init__(self, config_file=None):
        """
        初始化配置加载器
        
        Args:
            config_file (str, optional): 配置文件路径
        """
        self.config_file = config_file
        self.config = {
            'oss': {
                'endpoint': 'obs-tj.cucloud.cn',
                'access_key': '',
                'secret_key': '',
                'region': 'obs-tj'
            },
            'concurrency': {
                'chunk_size': 5 * 1024 * 1024  # 5MB
            }
        }
        
        if config_file:
            self.load_config()
    
    def load_config(self):
        """
        加载配置文件
        """
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                loaded_config = yaml.safe_load(f)
                
            if loaded_config:
                # 更新OSS配置
                if 'oss' in loaded_config:
                    self.config['oss'].update(loaded_config['oss'])
                
                # 更新并发配置
                if 'concurrency' in loaded_config:
                    self.config['concurrency'].update(loaded_config['concurrency'])
                    
            logger.info(f"成功加载配置文件: {self.config_file}")
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            sys.exit(1)
    
    def get_oss_config(self):
        """
        获取OSS配置
        
        Returns:
            dict: OSS配置
        """
        return self.config['oss']
    
    def get_concurrency_config(self):
        """
        获取并发配置
        
        Returns:
            dict: 并发配置
        """
        return self.config['concurrency']


class OSSClient:
    """独立的联通云OSS客户端"""
    
    def __init__(self, config, bucket_name=None):
        """
        初始化OSS客户端
        
        Args:
            config (dict): OSS配置
            bucket_name (str, optional): 桶名称
        """
        self.endpoint = config.get('endpoint', 'obs-tj.cucloud.cn')
        self.access_key = config.get('access_key')
        self.secret_key = config.get('secret_key')
        self.region = config.get('region', 'obs-tj')
        self.bucket_name = bucket_name
        
        # 确保endpoint不包含协议前缀
        if self.endpoint.startswith('http://') or self.endpoint.startswith('https://'):
            self.endpoint = urlparse(self.endpoint).netloc
            logger.info(f"OSS endpoint已移除协议前缀：{self.endpoint}")
        
        # 获取并发配置
        concurrency_config = config.get('concurrency', {})
        self.chunk_size = concurrency_config.get('chunk_size', 5 * 1024 * 1024)  # 默认5MB
        
        # 初始化请求会话
        self.session = requests.Session()
        self.session.timeout = (30, 60)  # (连接超时, 读取超时)
        
        logger.debug(f"OSS认证信息：access_key={self.access_key[:10]}...，endpoint={self.endpoint}")
        logger.info(f"OSS客户端已初始化，桶名：{self.bucket_name}，endpoint：{self.endpoint}")
    
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
        import datetime
        
        # 生成时间戳
        t = datetime.datetime.now(datetime.UTC)
        amz_date = t.strftime('%Y%m%dT%H%M%SZ')
        date_stamp = t.strftime('%Y%m%d')
        
        # 查询参数（无）
        canonical_querystring = ''
        
        # 构建规范头
        canonical_headers = ''
        host = headers.get('Host', '')
        canonical_headers += f"host:{host}\n"
        canonical_headers += f"x-amz-content-sha256:{payload_hash}\n"
        canonical_headers += f"x-amz-date:{amz_date}\n"
        
        # 构建已签名头
        signed_headers = 'host;x-amz-content-sha256;x-amz-date'
        
        # 构建规范请求
        canonical_request = ''
        canonical_request += f"{method}\n"
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
        """
        发送带签名的HTTP请求
        
        Args:
            method (str): HTTP方法
            path (str): 请求路径
            data (bytes, optional): 请求体数据
            headers (dict, optional): 请求头
            
        Returns:
            requests.Response: 响应对象
        """
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
        
        # 生成签名
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
            logger.error(f"OSS请求失败：{method} {url}，错误：{str(e)}")
            raise
    
    def _quote_key(self, key):
        """对对象键进行URL编码，但保留路径分隔符"""
        return quote(key, safe='/')
    
    def object_exists(self, key):
        """
        检查对象是否存在
        
        Args:
            key (str): 对象键
            
        Returns:
            bool: 对象是否存在
        """
        try:
            quoted_key = self._quote_key(key)
            path = f"/{quoted_key}"
            response = self._send_request('HEAD', path)
            return response.status_code == 200
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return False
            raise
    
    def upload_file(self, local_file, oss_path):
        """
        上传文件到OSS
        
        Args:
            local_file (str): 本地文件路径
            oss_path (str): OSS文件路径
            
        Returns:
            bool: 上传是否成功
        """
        try:
            with open(local_file, 'rb') as f:
                file_content = f.read()
                file_size = os.path.getsize(local_file)
                
                # 计算文件MD5
                md5_hash = hashlib.md5()
                md5_hash.update(file_content)
                etag = md5_hash.hexdigest()
                
                # 检查文件是否已存在
                exists = self.object_exists(oss_path)
                if exists:
                    logger.warning(f"文件已存在: {oss_path}")
                    return True
                
                # 上传文件
                quoted_key = self._quote_key(oss_path)
                path = f"/{quoted_key}"
                headers = {'Content-Length': str(file_size)}
                
                response = self._send_request('PUT', path, data=file_content, headers=headers)
                
                if response.status_code == 200:
                    uploaded_etag = response.headers.get('ETag', '').strip('"')
                    if uploaded_etag == etag:
                        logger.info(f"文件上传成功: {local_file} -> {oss_path}")
                        return True
                    else:
                        logger.error(f"文件MD5验证失败: {local_file} -> {oss_path} (预期: {etag}, 实际: {uploaded_etag})")
                        return False
                else:
                    logger.error(f"文件上传失败: {local_file} -> {oss_path}, 状态码: {response.status_code}")
                    return False
        except Exception as e:
            logger.error(f"上传文件异常: {local_file} -> {oss_path}, 错误: {str(e)}")
            return False
    
    def download_file(self, oss_path, local_file):
        """
        从OSS下载文件
        
        Args:
            oss_path (str): OSS文件路径
            local_file (str): 本地文件路径
            
        Returns:
            bool: 下载是否成功
        """
        try:
            # 创建本地目录
            os.makedirs(os.path.dirname(local_file), exist_ok=True)
            
            # 下载文件
            quoted_key = self._quote_key(oss_path)
            path = f"/{quoted_key}"
            
            response = self._send_request('GET', path)
            
            # 写入文件
            with open(local_file, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"文件下载成功: {oss_path} -> {local_file}")
            return True
        except Exception as e:
            logger.error(f"下载文件异常: {oss_path} -> {local_file}, 错误: {str(e)}")
            return False
    
    def delete_file(self, oss_path):
        """
        删除OSS中的文件
        
        Args:
            oss_path (str): OSS文件路径
            
        Returns:
            bool: 删除是否成功
        """
        try:
            # 检查文件是否存在
            if self.object_exists(oss_path):
                # 删除文件
                quoted_key = self._quote_key(oss_path)
                path = f"/{quoted_key}"
                
                self._send_request('DELETE', path)
                logger.info(f"文件删除成功: {oss_path}")
                return True
            else:
                logger.warning(f"文件不存在: {oss_path}")
                return True
        except Exception as e:
            logger.error(f"删除文件异常: {oss_path}, 错误: {str(e)}")
            return False
    
    def list_objects(self, prefix='', recursive=False):
        """
        列出OSS桶中的对象
        
        Args:
            prefix (str, optional): 前缀过滤
            recursive (bool, optional): 是否递归列出子目录中的文件
            
        Returns:
            list: 对象键列表
        """
        try:
            params = {}
            if prefix:
                params['prefix'] = prefix
            
            if not recursive:
                params['delimiter'] = '/'
            
            # 构建查询字符串
            query_string = urlencode(params)
            if query_string:
                path = f"/?{query_string}"
            else:
                path = f"/"
            
            # 发送请求
            response = self._send_request('GET', path)
            
            # 解析XML响应
            import xml.etree.ElementTree as ET
            root = ET.fromstring(response.content)
            
            files = []
            # 尝试使用不同的命名空间，兼容不同的OSS服务提供商
            namespaces = [
                '{http://s3.amazonaws.com/doc/2006-03-01/}',
                ''  # 无命名空间
            ]
            
            for ns in namespaces:
                # 收集所有文件
                for content in root.findall(f'.//{ns}Contents'):
                    key_elem = content.find(f'{ns}Key')
                    if key_elem is not None and key_elem.text is not None:
                        key = key_elem.text
                        files.append(key)
            
            # 如果是递归列出，收集所有目录并递归调用
            if recursive:
                for ns in namespaces:
                    for common_prefix in root.findall(f'.//{ns}CommonPrefixes'):
                        prefix_elem = common_prefix.find(f'{ns}Prefix')
                        if prefix_elem is not None and prefix_elem.text is not None:
                            prefix = prefix_elem.text
                            files.extend(self.list_objects(prefix, recursive=True))
            
            return files
        except Exception as e:
            logger.error(f"列出OSS对象失败: {str(e)}")
            return []


def get_files_to_process(local_path, recursive):
    """
    获取要处理的本地文件列表
    
    Args:
        local_path (str): 本地路径
        recursive (bool): 是否递归
        
    Returns:
        list: 文件列表
    """
    files = []
    
    if os.path.isfile(local_path):
        files.append(local_path)
    elif os.path.isdir(local_path):
        if recursive:
            for root, _, filenames in os.walk(local_path):
                for filename in filenames:
                    files.append(os.path.join(root, filename))
        else:
            # 仅处理当前目录下的文件
            for item in os.listdir(local_path):
                item_path = os.path.join(local_path, item)
                if os.path.isfile(item_path):
                    files.append(item_path)
    
    return files


def calculate_oss_path(local_file, local_base, oss_prefix):
    """
    根据本地文件路径和OSS前缀计算OSS目标路径
    
    Args:
        local_file (str): 本地文件路径
        local_base (str): 本地基准路径
        oss_prefix (str): OSS前缀
        
    Returns:
        str: OSS路径
    """
    # 获取相对路径
    if os.path.isfile(local_base):
        # 如果本地路径是文件，则直接使用文件名
        relative_path = os.path.basename(local_file)
    else:
        # 如果本地路径是目录，则获取相对于该目录的路径
        relative_path = os.path.relpath(local_file, local_base)
    
    # 组合OSS路径
    if oss_prefix:
        oss_path = f"{oss_prefix.rstrip('/')}/{relative_path.replace(os.sep, '/')}"
    else:
        oss_path = relative_path.replace(os.sep, '/')
    
    return oss_path


def upload_files(args, config):
    """
    上传文件到OSS
    
    Args:
        args (argparse.Namespace): 命令行参数
        config (dict): 配置
        
    Returns:
        bool: 是否成功
    """
    # 创建OSS客户端
    oss_client = OSSClient(config['oss'], args.bucket)
    
    # 获取要上传的文件列表
    files_to_upload = get_files_to_process(args.local_path, args.recursive)
    
    if not files_to_upload:
        logger.error(f"没有找到要上传的文件: {args.local_path}")
        return False
    
    logger.info(f"找到 {len(files_to_upload)} 个文件，准备上传到 {args.bucket}")
    
    # 并发上传
    success_count = 0
    failed_count = 0
    
    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = []
        
        for local_file in files_to_upload:
            oss_path = calculate_oss_path(local_file, args.local_path, args.oss_prefix)
            futures.append(executor.submit(oss_client.upload_file, local_file, oss_path))
        
        # 处理上传结果
        for future in as_completed(futures):
            if future.result():
                success_count += 1
            else:
                failed_count += 1
    
    logger.info(f"上传完成: 成功 {success_count} 个, 失败 {failed_count} 个")
    return failed_count == 0


def download_files(args, config):
    """
    从OSS下载文件
    
    Args:
        args (argparse.Namespace): 命令行参数
        config (dict): 配置
        
    Returns:
        bool: 是否成功
    """
    # 创建OSS客户端
    oss_client = OSSClient(config['oss'], args.bucket)
    
    # 获取要下载的文件列表
    if args.recursive:
        # 递归下载，列出目录中的所有文件
        files_to_download = oss_client.list_objects(prefix=args.oss_path, recursive=True)
        if not files_to_download:
            logger.error(f"OSS目录中没有找到文件: {args.oss_path}")
            return False
    else:
        # 下载单个文件
        files_to_download = [args.oss_path]
    
    logger.info(f"找到 {len(files_to_download)} 个文件，准备下载到 {args.local_dir}")
    
    # 并发下载
    success_count = 0
    failed_count = 0
    
    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = []
        
        for oss_path in files_to_download:
            # 计算本地文件路径
            if args.recursive:
                # 保持目录结构
                local_file = os.path.join(args.local_dir, oss_path.replace('/', os.sep))
            else:
                # 直接下载到本地目录
                local_file = os.path.join(args.local_dir, os.path.basename(oss_path))
            
            futures.append(executor.submit(oss_client.download_file, oss_path, local_file))
        
        # 处理下载结果
        for future in as_completed(futures):
            if future.result():
                success_count += 1
            else:
                failed_count += 1
    
    logger.info(f"下载完成: 成功 {success_count} 个, 失败 {failed_count} 个")
    return failed_count == 0


def delete_files(args, config):
    """
    删除OSS中的文件
    
    Args:
        args (argparse.Namespace): 命令行参数
        config (dict): 配置
        
    Returns:
        bool: 是否成功
    """
    # 创建OSS客户端
    oss_client = OSSClient(config['oss'], args.bucket)
    
    # 获取要删除的文件列表
    if args.recursive:
        # 递归删除，列出目录中的所有文件
        files_to_delete = oss_client.list_objects(prefix=args.oss_path, recursive=True)
        if not files_to_delete:
            logger.error(f"OSS目录中没有找到文件: {args.oss_path}")
            return False
    else:
        # 删除单个文件
        files_to_delete = [args.oss_path]
    
    logger.info(f"找到 {len(files_to_delete)} 个文件，准备从 {args.bucket} 删除")
    
    # 并发删除
    success_count = 0
    failed_count = 0
    
    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = []
        
        for oss_path in files_to_delete:
            futures.append(executor.submit(oss_client.delete_file, oss_path))
        
        # 处理删除结果
        for future in as_completed(futures):
            if future.result():
                success_count += 1
            else:
                failed_count += 1
    
    logger.info(f"删除完成: 成功 {success_count} 个, 失败 {failed_count} 个")
    return failed_count == 0


def parse_args():
    """
    解析命令行参数
    
    Returns:
        argparse.Namespace: 命令行参数
    """
    parser = argparse.ArgumentParser(description='联通云OSS操作工具')
    
    # 全局参数
    parser.add_argument('--config', '-c', type=str, help='自定义配置文件路径')
    
    # 子命令
    subparsers = parser.add_subparsers(dest='action', required=True, 
                                      help='操作类型：upload（上传）、download（下载）、delete（删除）')
    
    # 上传命令
    upload_parser = subparsers.add_parser('upload', help='上传文件到OSS')
    upload_parser.add_argument('--bucket', '-b', type=str, required=True, help='OSS桶名称')
    upload_parser.add_argument('--local-path', '-l', type=str, required=True, help='本地文件或目录路径')
    upload_parser.add_argument('--oss-prefix', '-p', type=str, default='', help='OSS目标路径前缀')
    upload_parser.add_argument('--recursive', '-r', action='store_true', help='递归上传目录')
    upload_parser.add_argument('--concurrency', '-n', type=int, default=5, help='并发上传数量')
    
    # 下载命令
    download_parser = subparsers.add_parser('download', help='从OSS下载文件')
    download_parser.add_argument('--bucket', '-b', type=str, required=True, help='OSS桶名称')
    download_parser.add_argument('--oss-path', '-o', type=str, default='', help='OSS文件或目录路径')
    download_parser.add_argument('--local-dir', '-l', type=str, required=True, help='本地目标目录')
    download_parser.add_argument('--recursive', '-r', action='store_true', help='递归下载目录')
    download_parser.add_argument('--concurrency', '-n', type=int, default=5, help='并发下载数量')
    
    # 删除命令
    delete_parser = subparsers.add_parser('delete', help='删除OSS中的文件')
    delete_parser.add_argument('--bucket', '-b', type=str, required=True, help='OSS桶名称')
    delete_parser.add_argument('--oss-path', '-o', type=str, default='', help='OSS文件或目录路径')
    delete_parser.add_argument('--recursive', '-r', action='store_true', help='递归删除目录')
    delete_parser.add_argument('--concurrency', '-n', type=int, default=10, help='并发删除数量')
    
    return parser.parse_args()


def main():
    """
    主函数
    """
    try:
        # 解析命令行参数
        args = parse_args()
        
        # 加载配置
        config_loader = ConfigLoader(args.config)
        config = {
            'oss': config_loader.get_oss_config(),
            'concurrency': config_loader.get_concurrency_config()
        }
        
        # 根据操作类型执行相应的功能
        if args.action == 'upload':
            success = upload_files(args, config)
        elif args.action == 'download':
            success = download_files(args, config)
        elif args.action == 'delete':
            success = delete_files(args, config)
        else:
            logger.error(f"未知的操作类型: {args.action}")
            success = False
        
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("操作被用户中断")
        sys.exit(1)
    except Exception as e:
        logger.error(f"程序执行异常: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()