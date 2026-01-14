#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试迁移日志记录器模块
"""
import os
import json
import pytest
from datetime import datetime
from unittest.mock import Mock, patch, mock_open
from log.migrate_logger import MigrateLogger


def test_migrate_logger_initialization():
    """测试迁移日志记录器初始化"""
    with patch('log.migrate_logger.os.makedirs'):
        migrate_logger = MigrateLogger()
        
        assert migrate_logger.success_files == 0
        assert migrate_logger.failed_files == 0
        assert isinstance(migrate_logger.failed_list, list)
        assert hasattr(migrate_logger, 'lock')


def test_migrate_logger_log_file_migrate_success():
    """测试记录成功的文件迁移"""
    with patch('log.migrate_logger.os.makedirs'):
        with patch('log.migrate_logger.open', mock_open()) as mock_file:
            migrate_logger = MigrateLogger()
            
            # 记录成功迁移
            migrate_logger.log_file_migrate(
                obs_path='file.txt',
                oss_path='migrated/file.txt',
                file_size=1024,
                duration=1.0,
                status='success',
                error_msg=''  
            )
            
            # 验证计数更新
            assert migrate_logger.success_files == 1
            assert migrate_logger.failed_files == 0
            assert len(migrate_logger.failed_list) == 0
            
            # 验证文件写入
            mock_file().write.assert_called_once()


def test_migrate_logger_log_file_migrate_failure():
    """测试记录失败的文件迁移"""
    with patch('log.migrate_logger.os.makedirs'):
        with patch('log.migrate_logger.open', mock_open()) as mock_file:
            migrate_logger = MigrateLogger()
            
            # 记录失败迁移
            migrate_logger.log_file_migrate(
                obs_path='file.txt',
                oss_path='migrated/file.txt',
                file_size=1024,
                duration=1.0,
                status='failed',
                error_msg='Network error'  
            )
            
            # 验证计数更新
            assert migrate_logger.success_files == 0
            assert migrate_logger.failed_files == 1
            assert len(migrate_logger.failed_list) == 1
            assert migrate_logger.failed_list[0]['obs_path'] == 'file.txt'
            assert migrate_logger.failed_list[0]['error_msg'] == 'Network error'
            
            # 验证文件写入
            mock_file().write.assert_called_once()


def test_migrate_logger_generate_daily_report():
    """测试生成每日报告"""
    with patch('log.migrate_logger.os.makedirs'):
        with patch('log.migrate_logger.open', mock_open()) as mock_file:
            with patch('log.migrate_logger.datetime') as mock_datetime:
                # 设置模拟日期
                mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)
                mock_datetime.strftime = datetime.strftime
                mock_datetime.isoformat = datetime.isoformat
                
                migrate_logger = MigrateLogger()
                
                # 添加一些成功和失败的记录
                migrate_logger.success_files = 100
                migrate_logger.failed_files = 5
                migrate_logger.failed_list = [
                    {'obs_path': 'failed1.txt', 'error_msg': 'Error 1'},
                    {'obs_path': 'failed2.txt', 'error_msg': 'Error 2'}
                ]
                
                # 生成报告
                report = migrate_logger.generate_daily_report()
                
                # 验证报告内容
                assert report['success_files'] == 100
                assert report['failed_files'] == 5
                assert len(report['failed_list']) == 2
                
                # 验证文件写入至少被调用
                mock_file().write.assert_called()  # 只需要确认有写入操作


def test_migrate_logger_load_failed_list():
    """测试加载失败文件列表"""
    with patch('log.migrate_logger.os.makedirs'):
        migrate_logger = MigrateLogger()

        # 模拟失败文件列表内容
        mock_file_content = 'failed1.txt\tError 1\nfailed2.txt\tError 2'

        # 同时模拟文件存在和文件读取
        with patch('builtins.open', mock_open(read_data=mock_file_content)):
            with patch('os.path.exists', return_value=True):
                failed_files = migrate_logger.load_failed_list('2023-01-01')

                # 验证加载结果
                assert len(failed_files) == 2
                assert failed_files[0]['obs_bucket'] == 'unknown'  # 旧格式会被标记为unknown
                assert failed_files[0]['obs_path'] == 'failed1.txt'
                assert failed_files[0]['error_msg'] == 'Error 1'
                assert failed_files[1]['obs_bucket'] == 'unknown'
                assert failed_files[1]['obs_path'] == 'failed2.txt'
                assert failed_files[1]['error_msg'] == 'Error 2'


def test_migrate_logger_load_failed_list_file_not_found():
    """测试加载不存在的失败文件列表"""
    with patch('log.migrate_logger.os.makedirs'):
        migrate_logger = MigrateLogger()
        
        with patch('builtins.open', side_effect=FileNotFoundError):
            failed_files = migrate_logger.load_failed_list('2023-01-01')
            
            # 验证返回空列表
            assert failed_files == []


def test_migrate_logger_load_failed_list_invalid_json():
    """测试加载无效格式的失败文件列表"""
    with patch('log.migrate_logger.os.makedirs'):
        migrate_logger = MigrateLogger()

        # 同时模拟文件存在和文件读取
        with patch('builtins.open', mock_open(read_data='invalid format')):
            with patch('os.path.exists', return_value=True):
                failed_files = migrate_logger.load_failed_list('2023-01-01')
                assert len(failed_files) == 1
                assert failed_files[0]['obs_bucket'] == 'unknown'
                assert failed_files[0]['obs_path'] == 'invalid format'
                assert failed_files[0]['error_msg'] == '格式错误'






