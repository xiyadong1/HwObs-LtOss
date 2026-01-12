#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置灵活性验证脚本
用于验证配置参数是否正确加载和使用
"""

import os
import sys
from unittest.mock import Mock, patch

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.oss_client import OSSClient
from core.migrate_task import MigrateTask


def test_config_flexibility():
    """测试配置灵活性"""
    print("=== 开始验证配置灵活性 ===")
    
    # 自定义配置
    oss_config = {
        'endpoint': 'oss.cn-north-1.unicomcloud.com',
        'access_key': 'test-access-key',
        'secret_key': 'test-secret-key',
        'bucket_name': 'test-bucket',
        'target_prefix': 'migrated/',
        'client': {
            'connect_timeout': 60,  # 自定义超时时间
            'read_timeout': 60,
            'connection_pool_size': 200  # 自定义连接池大小
        }
    }
    
    concurrency_config = {
        'chunk_size': 5 * 1024 * 1024,  # 5MB
        'streaming_threshold': 100 * 1024 * 1024  # 100MB（自定义阈值）
    }
    
    with (patch('core.oss_client.config_loader.get_oss_config', return_value=oss_config),
          patch('core.oss_client.config_loader.get_concurrency_config', return_value=concurrency_config),
          patch('core.migrate_task.config_loader.get_oss_config', return_value=oss_config),
          patch('core.migrate_task.config_loader.get_concurrency_config', return_value=concurrency_config),
          patch('core.oss_client.config_loader.get_retry_config', return_value={'max_attempts': 3, 'interval': 5}),
          patch('core.oss_client.oss2.Auth') as mock_auth,
          patch('core.oss_client.oss2.Bucket') as mock_bucket,
          patch('core.obs_client.OBSClient') as mock_obs_client):
        
        # 1. 测试OSS客户端配置
        print("\n1. 测试OSS客户端配置...")
        
        # 保存原始defaults
        import oss2
        original_connect_timeout = oss2.defaults.connect_timeout
        original_connection_pool_size = oss2.defaults.connection_pool_size
        
        try:
            oss_client = OSSClient()
            
            # 验证配置是否正确应用
            if oss2.defaults.connect_timeout == 60:
                print("   ✓ 连接超时配置正确：60秒")
            else:
                print(f"   ✗ 连接超时配置错误，预期：60秒，实际：{oss2.defaults.connect_timeout}秒")
            
            if oss2.defaults.connection_pool_size == 200:
                print("   ✓ 连接池大小配置正确：200")
            else:
                print(f"   ✗ 连接池大小配置错误，预期：200，实际：{oss2.defaults.connection_pool_size}")
        finally:
            # 恢复原始defaults
            oss2.defaults.connect_timeout = original_connect_timeout
            oss2.defaults.connection_pool_size = original_connection_pool_size
        
        # 2. 测试流式迁移阈值配置
        print("\n2. 测试流式迁移阈值配置...")
        migrate_task = MigrateTask()
        
        # 测试不同大小的文件
        small_file = 50 * 1024 * 1024  # 50MB
        large_file = 150 * 1024 * 1024  # 150MB
        
        if not migrate_task.should_use_streaming(small_file):
            print("   ✓ 小文件（50MB）未使用流式迁移")
        else:
            print("   ✗ 小文件（50MB）错误地使用了流式迁移")
        
        if migrate_task.should_use_streaming(large_file):
            print("   ✓ 大文件（150MB）正确使用了流式迁移")
        else:
            print("   ✗ 大文件（150MB）未使用流式迁移")
    
    print("\n=== 配置灵活性验证完成 ===")


if __name__ == '__main__':
    test_config_flexibility()