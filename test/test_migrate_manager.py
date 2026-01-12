#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试迁移管理器模块
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import threading
from core.migrate_manager import MigrateManager


def test_migrate_manager_initialization():
    """测试迁移管理器初始化"""
    # 模拟依赖
    mock_obs_client = Mock()
    mock_migrate_task = Mock()
    
    with patch('core.migrate_manager.get_obs_client', return_value=mock_obs_client):
        with patch('core.migrate_manager.MigrateTask', return_value=mock_migrate_task):
            with patch('core.migrate_manager.config_loader') as mock_config_loader:
                # 模拟配置
                mock_config_loader.get_concurrency_config.return_value = {'thread_count': 5}
                mock_config_loader.get_migrate_config.return_value = {'progress_interval': 5}
                
                # 创建迁移管理器实例
                migrate_manager = MigrateManager()
                
                # 验证初始化参数
                assert migrate_manager.obs_client == mock_obs_client
                assert migrate_manager.migrate_task == mock_migrate_task
                assert migrate_manager.thread_count == 5
                assert hasattr(migrate_manager, 'task_queue')
                assert hasattr(migrate_manager, 'threads')
                assert migrate_manager.total_files == 0
                assert migrate_manager.processed_files == 0


def test_migrate_manager_init_workers():
    """测试初始化工作线程"""
    # 模拟依赖
    mock_obs_client = Mock()
    mock_oss_client = Mock()
    mock_migrate_logger = Mock()
    mock_migrate_task = Mock()
    mock_config = {
        'concurrency': {
            'thread_count': 3
        },
        'obs': {
            'bucket_name': 'obs-bucket',
            'prefix': '',
            'exclude_suffixes': []
        }
    }
    
    with patch('core.migrate_manager.MigrateTask', return_value=mock_migrate_task):
        with patch('core.migrate_manager.threading.Thread') as mock_thread:
            migrate_manager = MigrateManager(
                mock_obs_client,
                mock_oss_client,
                mock_migrate_logger,
                mock_config
            )
            
            # 调用初始化工作线程方法
            migrate_manager.init_workers()
            
            # 验证线程创建
            assert mock_thread.call_count == 3
            for i in range(3):
                mock_thread.assert_any_call(
                    target=migrate_manager.worker,
                    name=f"Worker-{i+1}"
                )


def test_migrate_manager_worker():
    """测试工作线程执行任务"""
    # 模拟依赖
    mock_obs_client = Mock()
    mock_oss_client = Mock()
    mock_migrate_logger = Mock()
    mock_migrate_task = Mock()
    mock_config = {
        'concurrency': {
            'thread_count': 1
        },
        'obs': {
            'bucket_name': 'obs-bucket',
            'prefix': '',
            'exclude_suffixes': []
        }
    }
    
    with patch('core.migrate_manager.MigrateTask', return_value=mock_migrate_task):
        migrate_manager = MigrateManager(
            mock_obs_client,
            mock_oss_client,
            mock_migrate_logger,
            mock_config
        )
        
        # 添加任务到队列
        file_info1 = {'key': 'file1.txt', 'size': 1024, 'etag': 'etag1'}
        file_info2 = {'key': 'file2.txt', 'size': 2048, 'etag': 'etag2'}
        
        migrate_manager.task_queue.put(file_info1)
        migrate_manager.task_queue.put(file_info2)
        migrate_manager.task_queue.put(None)  # 结束信号
        
        # 模拟任务执行结果
        mock_migrate_task.migrate_file.return_value = {
            'success': True,
            'file_key': 'file.txt'
        }
        
        # 执行工作线程
        migrate_manager.worker()
        
        # 验证任务执行
        assert mock_migrate_task.migrate_file.call_count == 2
        mock_migrate_task.migrate_file.assert_any_call(
            file_info1, 'obs-bucket', ''
        )
        mock_migrate_task.migrate_file.assert_any_call(
            file_info2, 'obs-bucket', ''
        )
        
        # 验证完成计数
        assert migrate_manager.completed_count == 2


def test_migrate_manager_monitor_progress():
    """测试进度监控"""
    # 模拟依赖
    mock_obs_client = Mock()
    mock_oss_client = Mock()
    mock_migrate_logger = Mock()
    mock_migrate_task = Mock()
    mock_config = {
        'concurrency': {
            'thread_count': 2
        },
        'obs': {
            'bucket_name': 'obs-bucket',
            'prefix': '',
            'exclude_suffixes': []
        }
    }
    
    with patch('core.migrate_manager.MigrateTask', return_value=mock_migrate_task):
        migrate_manager = MigrateManager(
            mock_obs_client,
            mock_oss_client,
            mock_migrate_logger,
            mock_config
        )
        
        # 设置总任务数
        migrate_manager.total_count = 10
        
        # 测试进度计算
        migrate_manager.completed_count = 0
        assert migrate_manager.get_progress() == (0, 10, 0.0)
        
        migrate_manager.completed_count = 3
        assert migrate_manager.get_progress() == (3, 10, 30.0)
        
        migrate_manager.completed_count = 10
        assert migrate_manager.get_progress() == (10, 10, 100.0)


def test_migrate_manager_run():
    """测试完整的迁移流程"""
    # 模拟依赖
    mock_obs_client = Mock()
    mock_oss_client = Mock()
    mock_migrate_logger = Mock()
    mock_migrate_task = Mock()
    mock_config = {
        'concurrency': {
            'thread_count': 2
        },
        'obs': {
            'bucket_name': 'obs-bucket',
            'prefix': 'prefix/',
            'exclude_suffixes': ['.log']
        }
    }
    
    with patch('core.migrate_manager.MigrateTask', return_value=mock_migrate_task):
        with patch('core.migrate_manager.threading.Thread') as mock_thread:
            with patch('core.migrate_manager.time.sleep') as mock_sleep:
                # 模拟OBS文件列表
                mock_obs_client.list_objects.return_value = [
                    {'key': 'prefix/file1.txt', 'size': 1024, 'etag': 'etag1'},
                    {'key': 'prefix/file2.txt', 'size': 2048, 'etag': 'etag2'},
                    {'key': 'prefix/file3.log', 'size': 512, 'etag': 'etag3'}  # 应该被排除
                ]
                
                # 模拟任务执行结果
                mock_migrate_task.migrate_file.return_value = {
                    'success': True,
                    'file_key': 'file.txt'
                }
                
                # 模拟线程实例
                mock_thread_instance = Mock()
                mock_thread.return_value = mock_thread_instance
                
                migrate_manager = MigrateManager(
                    mock_obs_client,
                    mock_oss_client,
                    mock_migrate_logger,
                    mock_config
                )
                
                # 执行迁移流程
                migrate_manager.run()
                
                # 验证文件列举
                mock_obs_client.list_objects.assert_called_once_with(
                    'obs-bucket', 'prefix/', ['.log']
                )
                
                # 验证任务队列中的任务数量（排除了.log文件）
                assert migrate_manager.task_queue.qsize() == 2 + 2  # 2个文件 + 2个结束信号
                
                # 验证线程创建和启动
                assert mock_thread.call_count == 2
                assert mock_thread_instance.start.call_count == 2
                
                # 验证进度监控（至少调用一次sleep）
                assert mock_sleep.call_count > 0
                
                # 验证报告生成
                mock_migrate_logger.generate_daily_report.assert_called_once()
