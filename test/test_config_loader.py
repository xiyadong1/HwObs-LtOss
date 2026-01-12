#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试配置加载器模块
"""
import os
import yaml
import pytest
from unittest.mock import patch, mock_open
from config.config_loader import ConfigLoader


def test_config_loader_from_file():
    """测试从YAML文件加载配置"""
    # 模拟配置文件内容
    mock_config = {
        'concurrency': {
            'thread_count': 10,
            'chunk_size': 8192
        },
        'retry': {
            'max_attempts': 3,
            'interval': 5
        }
    }
    
    with patch('builtins.open', mock_open(read_data=yaml.dump(mock_config))):
        config_loader = ConfigLoader('config.yaml')
        
        # 测试获取嵌套配置
        assert config_loader.get('concurrency.thread_count') == 10
        assert config_loader.get('retry.max_attempts') == 3
        assert config_loader.get('non_existent.key', 'default') == 'default'


def test_config_loader_with_environment_variables():
    """测试从环境变量加载配置"""
    mock_config = {
        'obs': {
            'bucket_name': 'test-bucket',
            'endpoint': 'obs.cn-north-4.myhuaweicloud.com'
        },
        'oss': {
            'bucket_name': 'target-bucket',
            'endpoint': 'oss.cn-beijing.aliyuncs.com'
        }
    }
    
    # 设置环境变量
    with patch.dict(os.environ, {
        'OBS_ACCESS_KEY': 'test-obs-access-key',
        'OBS_SECRET_KEY': 'test-obs-secret-key',
        'OSS_ACCESS_KEY': 'test-oss-access-key',
        'OSS_SECRET_KEY': 'test-oss-secret-key'
    }):
        with patch('builtins.open', mock_open(read_data=yaml.dump(mock_config))):
            config_loader = ConfigLoader('config.yaml')
            
            # 测试获取OBS配置
            obs_config = config_loader.get_obs_config()
            assert obs_config['bucket_name'] == 'test-bucket'
            assert obs_config['access_key'] == 'test-obs-access-key'
            assert obs_config['secret_key'] == 'test-obs-secret-key'
            
            # 测试获取OSS配置
            oss_config = config_loader.get_oss_config()
            assert oss_config['bucket_name'] == 'target-bucket'
            assert oss_config['access_key'] == 'test-oss-access-key'
            assert oss_config['secret_key'] == 'test-oss-secret-key'


def test_config_loader_missing_environment_variables():
    """测试缺少环境变量时的异常处理"""
    mock_config = {
        'obs': {
            'bucket_name': 'test-bucket'
        }
    }
    
    # 清除环境变量
    with patch.dict(os.environ, {}, clear=True):
        with patch('builtins.open', mock_open(read_data=yaml.dump(mock_config))):
            with pytest.raises(ValueError, match="OBS_ACCESS_KEY environment variable is required"):
                config_loader = ConfigLoader('config.yaml')
                config_loader.get_obs_config()


def test_config_loader_invalid_yaml():
    """测试无效的YAML配置文件"""
    with patch('builtins.open', mock_open(read_data='invalid: yaml: [file')):
        with pytest.raises(yaml.YAMLError):
            ConfigLoader('config.yaml')


def test_config_loader_file_not_found():
    """测试配置文件不存在的情况"""
    with patch('builtins.open', side_effect=FileNotFoundError):
        with pytest.raises(FileNotFoundError):
            ConfigLoader('non_existent_file.yaml')
