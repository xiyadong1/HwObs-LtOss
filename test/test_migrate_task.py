#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试迁移任务模块
"""
import pytest
import time
from unittest.mock import Mock, patch
from core.migrate_task import MigrateTask


def test_migrate_task_initialization():
    """测试迁移任务初始化"""
    # 模拟依赖
    mock_obs_client = Mock()
    mock_oss_client = Mock()
    
    with patch('core.migrate_task.get_obs_client', return_value=mock_obs_client):
        with patch('core.migrate_task.get_oss_client', return_value=mock_oss_client):
            with patch('core.migrate_task.config_loader') as mock_config_loader:
                # 模拟配置
                mock_config_loader.get_retry_config.return_value = {
                    'max_attempts': 3,
                    'interval': 1
                }
                
                # 创建迁移任务实例
                migrate_task = MigrateTask()
                
                # 验证初始化参数
                assert migrate_task.obs_client == mock_obs_client
                assert migrate_task.oss_client == mock_oss_client
                assert migrate_task.max_retry == 3
                assert migrate_task.retry_interval == 1


def test_migrate_task_should_use_streaming():
    """测试是否应该使用流式上传"""
    # 模拟依赖
    mock_obs_client = Mock()
    mock_oss_client = Mock()
    
    with patch('core.migrate_task.get_obs_client', return_value=mock_obs_client):
        with patch('core.migrate_task.get_oss_client', return_value=mock_oss_client):
            with patch('core.migrate_task.config_loader') as mock_config_loader:
                # 模拟配置
                mock_config_loader.get_retry_config.return_value = {
                    'max_attempts': 3,
                    'interval': 1
                }
                mock_config_loader.get_concurrency_config.return_value = {
                    'chunk_size': 1 * 1024 * 1024  # 1MB chunk size
                }
                
                migrate_task = MigrateTask()
                
                # 测试小文件（<=10倍chunk_size = 10MB）
                assert migrate_task.should_use_streaming(5 * 1024 * 1024) is False  # 5MB
                assert migrate_task.should_use_streaming(10 * 1024 * 1024) is False  # 10MB
                
                # 测试大文件（>10倍chunk_size = 10MB）
                assert migrate_task.should_use_streaming(11 * 1024 * 1024) is True  # 11MB
                assert migrate_task.should_use_streaming(100 * 1024 * 1024) is True  # 100MB


def test_migrate_task_migrate_file_success():
    """测试成功迁移小文件"""
    # 模拟依赖
    mock_obs_client = Mock()
    mock_oss_client = Mock()
    mock_migrate_logger = Mock()
    
    with patch('core.migrate_task.get_obs_client', return_value=mock_obs_client):
        with patch('core.migrate_task.get_oss_client', return_value=mock_oss_client):
            with patch('core.migrate_task.config_loader') as mock_config_loader:
                with patch('core.migrate_task.migrate_logger', mock_migrate_logger):
                    # 模拟配置
                    mock_config_loader.get_retry_config.return_value = {
                        'max_attempts': 3,
                        'interval': 1
                    }
                    
                    migrate_task = MigrateTask()
                    
                    # 模拟OBS下载成功
                    mock_obs_client.get_object.return_value = b'test content'
                    
                    # 模拟OSS上传成功
                    mock_oss_client.upload_file.return_value = (True, None)
                    mock_oss_client.get_target_path.return_value = 'file.txt'
                    
                    # 执行迁移任务
                    file_info = {'key': 'file.txt', 'size': 1024, 'etag': 'etag123'}
                    result = migrate_task.migrate_file(file_info)
                    
                    # 验证结果
                    assert result['status'] == 'success'
                    assert result['obs_path'] == 'file.txt'
                    
                    # 验证方法调用
                    mock_obs_client.get_object.assert_called_once_with('file.txt')
                    mock_oss_client.upload_file.assert_called_once()
                    mock_migrate_logger.log_file_migrate.assert_called_once()





def test_migrate_task_migrate_file_with_retry():
    """测试文件迁移失败时的重试机制"""
    # 模拟依赖
    mock_obs_client = Mock()
    mock_oss_client = Mock()
    mock_migrate_logger = Mock()
    
    with patch('core.migrate_task.get_obs_client', return_value=mock_obs_client):
        with patch('core.migrate_task.get_oss_client', return_value=mock_oss_client):
            with patch('core.migrate_task.config_loader') as mock_config_loader:
                with patch('core.migrate_task.migrate_logger', mock_migrate_logger):
                    # 模拟配置
                    mock_config_loader.get_retry_config.return_value = {
                        'max_attempts': 3,
                        'interval': 0.1
                    }
                    
                    migrate_task = MigrateTask()
                    
                    # 模拟OBS下载成功
                    mock_obs_client.get_object.return_value = b'test content'
                    
                    # 模拟前两次上传失败，第三次成功
                    mock_oss_client.upload_file.side_effect = [
                        (False, 'Upload failed'),  # 第一次失败
                        (False, 'Upload failed'),  # 第二次失败
                        (True, None)               # 第三次成功
                    ]
                    mock_oss_client.get_target_path.return_value = 'file.txt'
                    
                    # 执行迁移任务
                    file_info = {'key': 'file.txt', 'size': 1024, 'etag': 'etag123'}
                    
                    # 记录开始时间
                    start_time = time.time()
                    result = migrate_task.migrate_file(file_info)
                    end_time = time.time()
                    
                    # 验证结果
                    assert result['status'] == 'success'
                    
                    # 验证重试次数
                    assert mock_oss_client.upload_file.call_count == 3
                    
                    # 验证是否有重试间隔
                    assert (end_time - start_time) >= 0.2  # 至少有两次重试间隔（0.1s * 2）


def test_migrate_task_migrate_file_with_all_retries_failed():
    """测试所有重试都失败的情况"""
    # 模拟依赖
    mock_obs_client = Mock()
    mock_oss_client = Mock()
    mock_migrate_logger = Mock()
    
    with patch('core.migrate_task.get_obs_client', return_value=mock_obs_client):
        with patch('core.migrate_task.get_oss_client', return_value=mock_oss_client):
            with patch('core.migrate_task.config_loader') as mock_config_loader:
                with patch('core.migrate_task.migrate_logger', mock_migrate_logger):
                    # 模拟配置
                    mock_config_loader.get_retry_config.return_value = {
                        'max_attempts': 3,
                        'interval': 0.1
                    }
                    
                    migrate_task = MigrateTask()
                    
                    # 模拟OBS下载成功
                    mock_obs_client.get_object.return_value = b'test content'
                    
                    # 模拟所有上传都失败
                    mock_oss_client.upload_file.return_value = (False, 'Upload failed')
                    mock_oss_client.get_target_path.return_value = 'file.txt'
                    
                    # 执行迁移任务
                    file_info = {'key': 'file.txt', 'size': 1024, 'etag': 'etag123'}
                    result = migrate_task.migrate_file(file_info)
                    
                    # 验证结果
                    assert result['status'] == 'failed'
                    assert 'error_msg' in result
                    assert result['error_msg'] == 'Upload failed'
                    
                    # 验证重试次数
                    assert mock_oss_client.upload_file.call_count == 3
                    
                    # 验证日志记录
                    mock_migrate_logger.log_file_migrate.assert_called_once()


def test_migrate_task_migrate_file_stream():
    """测试流式上传大文件"""
    # 模拟依赖
    mock_obs_client = Mock()
    mock_oss_client = Mock()
    mock_migrate_logger = Mock()
    
    with patch('core.migrate_task.get_obs_client', return_value=mock_obs_client):
        with patch('core.migrate_task.get_oss_client', return_value=mock_oss_client):
            with patch('core.migrate_task.config_loader') as mock_config_loader:
                with patch('core.migrate_task.migrate_logger', mock_migrate_logger):
                    # 模拟配置
                    mock_config_loader.get_retry_config.return_value = {
                        'max_attempts': 3,
                        'interval': 1
                    }
                    
                    migrate_task = MigrateTask()
                    
                    # 模拟OBS下载流
                    mock_stream = Mock()
                    mock_stream.read.side_effect = [b'part1', b'part2', b'']  # 分块读取

                    mock_response = Mock()
                    mock_response.status = 200
                    mock_response.body = Mock(response=mock_stream)
                    mock_obs_client.get_object_stream.return_value = mock_response
                    
                    # 模拟OSS流式上传成功
                    mock_oss_client.upload_file_stream.return_value = (True, None)
                    mock_oss_client.get_target_path.return_value = 'large_file.bin'
                    
                    # 执行迁移任务
                    file_info = {'key': 'large_file.bin', 'size': 1024 * 1024 * 20, 'etag': 'etag123'}  # 20MB
                    result = migrate_task.migrate_file_stream(file_info)
                    
                    # 验证结果
                    assert result['status'] == 'success'
                    assert result['obs_path'] == 'large_file.bin'
                    
                    # 验证方法调用
                    mock_obs_client.get_object_stream.assert_called_once()
                    mock_oss_client.upload_file_stream.assert_called_once()
