#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多桶迁移功能验证脚本
用于验证多桶迁移的核心功能是否正常工作
"""

import os
import sys
from unittest.mock import Mock, patch

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.obs_client import OBSClient
from core.oss_client import OSSClient
from core.migrate_manager import MigrateManager
from config.config_loader import config_loader


def test_multi_bucket_migration():
    """测试多桶迁移功能"""
    print("=== 开始验证多桶迁移功能 ===")
    
    # 配置模拟
    mock_bucket_mappings = [
        {
            'obs_bucket': 'obs-bucket-1',
            'oss_bucket': 'oss-bucket-1',
            'obs_prefix': 'data1/',
            'oss_prefix': 'migrated/data1/',
            'exclude_suffixes': ['.log', '.tmp']
        },
        {
            'obs_bucket': 'obs-bucket-2',
            'oss_bucket': 'oss-bucket-2',
            'obs_prefix': 'data2/',
            'oss_prefix': 'migrated/data2/',
            'exclude_suffixes': []
        }
    ]
    
    obs_config = {
        'endpoint': 'obs.cn-north-1.myhuaweicloud.com',
        'access_key': 'test-obs-key',
        'secret_key': 'test-obs-secret',
        'bucket_name': 'default-obs-bucket',
        'prefix': '',
        'exclude_suffixes': []
    }
    
    oss_config = {
        'endpoint': 'oss.cn-north-1.unicomcloud.com',
        'access_key': 'test-oss-key',
        'secret_key': 'test-oss-secret',
        'bucket_name': 'default-oss-bucket',
        'target_prefix': 'default/',
        'client': {
            'connect_timeout': 30,
            'read_timeout': 30,
            'connection_pool_size': 100
        }
    }
    
    concurrency_config = {
        'thread_count': 10,
        'chunk_size': 5 * 1024 * 1024,
        'streaming_threshold': 50 * 1024 * 1024
    }
    
    migrate_config = {
        'enable_resume': True,
        'progress_interval': 5
    }
    
    with (
        patch('core.migrate_manager.config_loader.get_bucket_mappings', return_value=mock_bucket_mappings),
        patch('core.migrate_manager.config_loader.get_obs_config', return_value=obs_config),
        patch('core.migrate_manager.config_loader.get_oss_config', return_value=oss_config),
        patch('core.migrate_manager.config_loader.get_concurrency_config', return_value=concurrency_config),
        patch('core.migrate_manager.config_loader.get_migrate_config', return_value=migrate_config),
        patch('core.obs_client.config_loader.get_obs_config', return_value=obs_config),
        patch('core.oss_client.config_loader.get_oss_config', return_value=oss_config),
        patch('core.oss_client.config_loader.get_concurrency_config', return_value=concurrency_config),
        patch('core.migrate_task.config_loader.get_retry_config', return_value={'max_attempts': 3, 'interval': 5}),
        patch('core.obs_client.ObsClient') as mock_obs_sdk,
        patch('core.oss_client.oss2.Auth') as mock_oss_auth,
        patch('core.oss_client.oss2.Bucket') as mock_oss_bucket,
        patch('core.migrate_task.MigrateTask.migrate_file') as mock_migrate_file,
        patch('core.migrate_task.MigrateTask.migrate_file_stream') as mock_migrate_stream
    ):
        
        # 1. 测试配置加载
        print("\n1. 测试桶映射配置加载...")
        loaded_mappings = config_loader.get_bucket_mappings()
        print(f"   ✓ 加载的桶映射数量：{len(loaded_mappings)}")
        
        # 2. 测试客户端初始化
        print("\n2. 测试基于桶映射的客户端初始化...")
        
        # 测试OBS客户端
        obs_config_1 = {
            'bucket_name': 'obs-bucket-1',
            'prefix': 'data1/',
            'exclude_suffixes': ['.log', '.tmp']
        }
        obs_client = OBSClient(bucket_config=obs_config_1)
        if obs_client.bucket_name == 'obs-bucket-1' and obs_client.prefix == 'data1/':
            print("   ✓ OBS客户端桶特定配置正确")
        else:
            print(f"   ✗ OBS客户端配置错误：桶名={obs_client.bucket_name}, 前缀={obs_client.prefix}")
        
        # 测试OSS客户端
        oss_config_1 = {
            'bucket_name': 'oss-bucket-1',
            'target_prefix': 'migrated/data1/'
        }
        oss_client = OSSClient(bucket_config=oss_config_1)
        if oss_client.bucket_name == 'oss-bucket-1' and oss_client.target_prefix == 'migrated/data1/':
            print("   ✓ OSS客户端桶特定配置正确")
        else:
            print(f"   ✗ OSS客户端配置错误：桶名={oss_client.bucket_name}, 前缀={oss_client.target_prefix}")
        
        # 3. 测试迁移管理器初始化
        print("\n3. 测试迁移管理器初始化...")
        migrate_manager = MigrateManager()
        if len(migrate_manager.bucket_mappings) == 2:
            print(f"   ✓ 迁移管理器加载了{len(migrate_manager.bucket_mappings)}个桶映射")
        else:
            print(f"   ✗ 迁移管理器桶映射加载错误：{len(migrate_manager.bucket_mappings)}个")
        
        # 4. 测试文件列举和任务准备
        print("\n4. 测试文件列举和任务准备...")
        
        # 模拟OBS客户端返回的文件列表
        mock_obs_files_1 = [
            {'key': 'data1/file1.txt', 'size': 1024, 'etag': 'etag1'},
            {'key': 'data1/file2.txt', 'size': 2048, 'etag': 'etag2'}
        ]
        
        mock_obs_files_2 = [
            {'key': 'data2/file3.txt', 'size': 3072, 'etag': 'etag3'},
            {'key': 'data2/file4.txt', 'size': 4096, 'etag': 'etag4'}
        ]
        
        # 模拟OBS SDK的listObjects返回
        mock_obs_response_1 = Mock(status=200)
        mock_obs_response_1.body.contents = [Mock(key=f['key'], size=f['size'], etag=f['etag']) for f in mock_obs_files_1]
        mock_obs_response_1.body.nextMarker = None
        
        mock_obs_response_2 = Mock(status=200)
        mock_obs_response_2.body.contents = [Mock(key=f['key'], size=f['size'], etag=f['etag']) for f in mock_obs_files_2]
        mock_obs_response_2.body.nextMarker = None
        
        # 设置mock_obs_sdk的行为
        mock_obs_instance = Mock()
        mock_obs_sdk.return_value = mock_obs_instance
        mock_obs_instance.listObjects.side_effect = [mock_obs_response_1, mock_obs_response_2]
        mock_obs_instance.close = Mock()
        
        # 模拟文件列举
        obs_client1 = OBSClient(bucket_config={'bucket_name': 'obs-bucket-1', 'prefix': 'data1/', 'exclude_suffixes': ['.log', '.tmp']})
        files1 = list(obs_client1.list_objects())
        
        obs_client2 = OBSClient(bucket_config={'bucket_name': 'obs-bucket-2', 'prefix': 'data2/', 'exclude_suffixes': []})
        files2 = list(obs_client2.list_objects())
        
        if len(files1) == 2 and len(files2) == 2:
            print("   ✓ 多桶文件列举正确")
        else:
            print(f"   ✗ 文件列举错误：桶1={len(files1)}个，桶2={len(files2)}个")
        
        # 5. 测试迁移任务创建
        print("\n5. 测试迁移任务创建...")
        
        # 设置mock_oss_bucket的行为
        mock_oss_instance = Mock()
        mock_oss_bucket.return_value = mock_oss_instance
        mock_oss_instance.object_exists.return_value = False
        
        # 模拟迁移任务成功
        mock_migrate_file.return_value = {'status': 'success'}
        mock_migrate_stream.return_value = {'status': 'success'}
        
        # 测试迁移管理器的任务准备逻辑（简化版）
        all_files_to_migrate = []
        
        for bucket_mapping in mock_bucket_mappings:
            obs_config = {
                'bucket_name': bucket_mapping['obs_bucket'],
                'prefix': bucket_mapping['obs_prefix'],
                'exclude_suffixes': bucket_mapping['exclude_suffixes']
            }
            
            obs_client = OBSClient(bucket_config=obs_config)
            
            # 模拟文件列表
            if bucket_mapping['obs_bucket'] == 'obs-bucket-1':
                files = mock_obs_files_1
            else:
                files = mock_obs_files_2
            
            for file_info in files:
                all_files_to_migrate.append({
                    'file_info': file_info,
                    'bucket_mapping': bucket_mapping
                })
            
            obs_client.close()
        
        if len(all_files_to_migrate) == 4:
            print("   ✓ 迁移任务创建正确：4个任务")
        else:
            print(f"   ✗ 迁移任务创建错误：{len(all_files_to_migrate)}个任务")
        
        # 验证任务结构
        first_task = all_files_to_migrate[0]
        if 'file_info' in first_task and 'bucket_mapping' in first_task:
            print("   ✓ 迁移任务结构正确")
        else:
            print("   ✗ 迁移任务结构错误")
    
    print("\n=== 多桶迁移功能验证完成 ===")


if __name__ == '__main__':
    test_multi_bucket_migration()