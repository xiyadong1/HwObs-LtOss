#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ACL 批量修改工具
用于递归修改联通云 OSS 桶内对象的访问控制列表（ACL）
支持多桶配置和单独的配置文件
"""

import os
import sys
import argparse
import yaml

# 添加父目录到 Python 路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import threading
import queue
import time
from botocore import session
from botocore.config import Config
from config.config_loader import config_loader

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ACLTool:
    """ACL 批量修改工具类"""
    
    def __init__(self, config):
        """初始化 ACL 工具
        
        Args:
            config (dict): 配置字典，包含 OSS 连接信息和 ACL 工具配置
        """
        # 配置参数
        self.endpoint_url = config.get('endpoint')
        self.access_key = config.get('access_key')
        self.secret_key = config.get('secret_key')
        self.bucket_name = config.get('bucket_name')
        
        # ACL 工具配置
        self.target_acl = config.get('target_acl', 'public-read-write')
        self.thread_count = config.get('thread_count', 10)
        self.batch_size = config.get('batch_size', 100)
        self.recursive = config.get('recursive', True)
        self.prefix = config.get('prefix', '')
        self.exclude_suffixes = config.get('exclude_suffixes', [])
        
        # 验证配置
        self._validate_config()
        
        # 创建 OSS 客户端
        self.client = self._create_oss_client()
        
        # 统计信息
        self.total_objects = 0
        self.success_count = 0
        self.failed_count = 0
        self.failed_objects = []
        
        # 线程安全队列
        self.object_queue = queue.Queue()
        
    def _validate_config(self):
        """验证配置"""
        required_configs = [
            ('endpoint', self.endpoint_url),
            ('access_key', self.access_key),
            ('secret_key', self.secret_key),
            ('bucket_name', self.bucket_name)
        ]
        
        for config_name, config_value in required_configs:
            if not config_value:
                raise ValueError(f"缺少必要配置: {config_name}")
        
        # 验证 ACL 策略
        valid_acls = ['private', 'public-read', 'public-read-write']
        if self.target_acl not in valid_acls:
            raise ValueError(f"无效的 ACL 策略: {self.target_acl}，支持的策略: {valid_acls}")
        
        logger.info(f"配置验证通过:")
        logger.info(f"  OSS Endpoint: {self.endpoint_url}")
        logger.info(f"  Bucket Name: {self.bucket_name}")
        logger.info(f"  Target ACL: {self.target_acl}")
        logger.info(f"  Thread Count: {self.thread_count}")
        logger.info(f"  Recursive: {self.recursive}")
        logger.info(f"  Prefix: {self.prefix or '无'}")
        logger.info(f"  Exclude Suffixes: {self.exclude_suffixes or '无'}")
    
    def _create_oss_client(self):
        """创建 OSS 客户端"""
        try:
            sess = session.Session()
            client = sess.create_client(
                's3',
                use_ssl=True,
                verify=False,
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                config=Config(
                    signature_version="s3v4",
                    connect_timeout=30,
                    read_timeout=30,
                    s3={'addressing_style': 'path'}
                )
            )
            logger.info("OSS 客户端创建成功")
            return client
        except Exception as e:
            logger.error(f"OSS 客户端创建失败: {str(e)}")
            raise
    
    def _list_objects(self):
        """列出桶内所有对象"""
        logger.info(f"开始列出桶内对象...")
        
        try:
            continuation_token = None
            while True:
                list_kwargs = {
                    'Bucket': self.bucket_name,
                    'MaxKeys': 1000
                }
                
                if self.prefix:
                    list_kwargs['Prefix'] = self.prefix
                
                if continuation_token:
                    list_kwargs['ContinuationToken'] = continuation_token
                
                response = self.client.list_objects_v2(**list_kwargs)
                
                # 处理对象
                if 'Contents' in response:
                    for obj in response['Contents']:
                        object_key = obj['Key']
                        
                        # 跳过目录（以 / 结尾的对象）
                        if object_key.endswith('/'):
                            continue
                        
                        # 跳过排除的文件后缀
                        if self.exclude_suffixes:
                            if any(object_key.endswith(suffix) for suffix in self.exclude_suffixes):
                                continue
                        
                        self.object_queue.put(object_key)
                        self.total_objects += 1
                
                # 检查是否还有更多对象
                if not response.get('IsTruncated'):
                    break
                
                continuation_token = response.get('NextContinuationToken')
            
            logger.info(f"对象列表获取完成，共找到 {self.total_objects} 个对象")
        except Exception as e:
            logger.error(f"列出对象失败: {str(e)}")
            raise
    
    def _process_object(self, object_key):
        """处理单个对象的 ACL 设置"""
        try:
            response = self.client.put_object_acl(
                ACL=self.target_acl,
                Bucket=self.bucket_name,
                Key=object_key
            )
            self.success_count += 1
            logger.debug(f"成功设置对象 [{object_key}] 的 ACL 为: {self.target_acl}")
            return True
        except Exception as e:
            self.failed_count += 1
            self.failed_objects.append((object_key, str(e)))
            logger.error(f"设置对象 [{object_key}] 的 ACL 失败: {str(e)}")
            return False
    
    def _worker(self):
        """工作线程函数"""
        while True:
            try:
                object_key = self.object_queue.get(block=False)
                self._process_object(object_key)
                self.object_queue.task_done()
            except queue.Empty:
                break
            except Exception as e:
                logger.error(f"工作线程异常: {str(e)}")
                self.object_queue.task_done()
    
    def run(self):
        """运行 ACL 批量修改"""
        start_time = time.time()
        logger.info("开始批量修改 ACL...")
        
        try:
            # 列出所有对象
            self._list_objects()
            
            if self.total_objects == 0:
                logger.info("没有找到需要处理的对象")
                return
            
            # 创建并启动工作线程
            threads = []
            for _ in range(min(self.thread_count, self.total_objects)):
                thread = threading.Thread(target=self._worker)
                thread.daemon = True
                thread.start()
                threads.append(thread)
            
            # 等待所有任务完成
            self.object_queue.join()
            
            # 等待所有线程结束
            for thread in threads:
                thread.join()
            
            # 打印结果
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            logger.info("\n=== ACL 批量修改完成 ===")
            logger.info(f"总对象数: {self.total_objects}")
            logger.info(f"成功修改: {self.success_count}")
            logger.info(f"失败修改: {self.failed_count}")
            logger.info(f"耗时: {elapsed_time:.2f} 秒")
            logger.info(f"平均速度: {self.total_objects / elapsed_time:.2f} 对象/秒")
            
            if self.failed_objects:
                logger.warning(f"\n失败对象列表:")
                for object_key, error_msg in self.failed_objects[:10]:  # 只显示前10个
                    logger.warning(f"  - {object_key}: {error_msg}")
                if len(self.failed_objects) > 10:
                    logger.warning(f"  ... 还有 {len(self.failed_objects) - 10} 个失败对象")
            
        except Exception as e:
            logger.error(f"批量修改 ACL 失败: {str(e)}")
            raise

def load_config(config_file):
    """加载配置文件
    
    Args:
        config_file (str): 配置文件路径
    
    Returns:
        dict: 配置字典
    """
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"配置文件不存在: {config_file}")
    
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    return config

def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='ACL 批量修改工具')
    parser.add_argument('--config', '-c', type=str, default=None,
                        help='配置文件路径，默认使用 config/config.yaml')
    args = parser.parse_args()
    
    try:
        # 加载配置
        if args.config:
            # 使用指定的配置文件
            config = load_config(args.config)
        else:
            # 使用默认配置
            config = {
                'oss': config_loader.get_oss_config(),
                'acl': config_loader.get_acl_config()
            }
        
        # 处理多桶配置
        buckets = []
        if 'buckets' in config:
            # 多桶配置
            buckets = config['buckets']
            logger.info(f"发现 {len(buckets)} 个桶配置")
        else:
            # 单桶配置
            oss_config = config.get('oss', {})
            acl_config = config.get('acl', {})
            bucket_config = {
                'endpoint': oss_config.get('endpoint'),
                'access_key': oss_config.get('access_key'),
                'secret_key': oss_config.get('secret_key'),
                'bucket_name': oss_config.get('bucket_name'),
                'target_acl': acl_config.get('target_acl', 'public-read-write'),
                'thread_count': acl_config.get('thread_count', 10),
                'batch_size': acl_config.get('batch_size', 100),
                'recursive': acl_config.get('recursive', True),
                'prefix': acl_config.get('prefix', ''),
                'exclude_suffixes': acl_config.get('exclude_suffixes', [])
            }
            buckets.append(bucket_config)
        
        # 处理每个桶
        total_success = 0
        total_failed = 0
        total_objects = 0
        
        for i, bucket_config in enumerate(buckets):
            logger.info(f"\n=== 处理桶 {i+1}/{len(buckets)}: {bucket_config['bucket_name']} ===")
            
            # 创建并运行 ACL 工具
            tool = ACLTool(bucket_config)
            tool.run()
            
            # 累计统计信息
            total_objects += tool.total_objects
            total_success += tool.success_count
            total_failed += tool.failed_count
        
        # 打印总体结果
        if len(buckets) > 1:
            logger.info("\n=== 总体执行结果 ===")
            logger.info(f"总桶数: {len(buckets)}")
            logger.info(f"总对象数: {total_objects}")
            logger.info(f"成功修改: {total_success}")
            logger.info(f"失败修改: {total_failed}")
            
    except Exception as e:
        logger.error(f"工具执行失败: {str(e)}")
        import traceback
        traceback.print_exc()
        exit(1)

if __name__ == "__main__":
    main()