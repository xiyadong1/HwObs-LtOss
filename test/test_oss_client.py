#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试OSS客户端模块
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from io import BytesIO
from core.oss_client import OSSClient, get_oss_client, global_oss_client

# 测试前清理全局实例
def setup_module():
    global global_oss_client
    global_oss_client = None

# 测试后清理全局实例
def teardown_module():
    global global_oss_client
    global_oss_client = None


def test_oss_client_initialization():
    """测试OSS客户端初始化"""
    oss_config = {
        'endpoint': 'oss.cn-beijing.aliyuncs.com',
        'access_key': 'test-access-key',
        'secret_key': 'test-secret-key',
        'bucket_name': 'test-bucket',
        'target_prefix': ''
    }
    
    concurrency_config = {
        'chunk_size': 8192
    }
    
    with (patch('core.oss_client.config_loader.get_oss_config', return_value=oss_config),
          patch('core.oss_client.config_loader.get_concurrency_config', return_value=concurrency_config),
          patch('core.oss_client.oss2.Auth') as mock_auth,
          patch('core.oss_client.oss2.Bucket') as mock_bucket):
        # 创建OSS客户端实例
        oss_client = OSSClient()
        
        # 验证auth和bucket是否正确初始化
        mock_auth.assert_called_once_with('test-access-key', 'test-secret-key')
        mock_bucket.assert_called_once_with(
            mock_auth.return_value,
            'oss.cn-beijing.aliyuncs.com',
            'test-bucket'
        )
        assert oss_client.bucket == mock_bucket.return_value


def test_oss_client_get_target_path():
    """测试OSS目标路径映射"""
    oss_config = {
        'endpoint': 'oss.cn-beijing.aliyuncs.com',
        'access_key': 'test-access-key',
        'secret_key': 'test-secret-key',
        'bucket_name': 'test-bucket',
        'target_prefix': ''
    }
    
    concurrency_config = {
        'chunk_size': 8192
    }
    
    with (patch('core.oss_client.config_loader.get_oss_config', return_value=oss_config),
          patch('core.oss_client.config_loader.get_concurrency_config', return_value=concurrency_config),
          patch('core.oss_client.oss2')):
        oss_client = OSSClient()
        
        # 测试基本路径映射
        assert oss_client.get_target_path('file.txt') == 'file.txt'
        assert oss_client.get_target_path('data/file.txt') == 'data/file.txt'
    
    # 测试带目标前缀的路径映射
    oss_config_with_prefix = {
        **oss_config,
        'target_prefix': 'migrated/'
    }
    
    with (patch('core.oss_client.config_loader.get_oss_config', return_value=oss_config_with_prefix),
          patch('core.oss_client.config_loader.get_concurrency_config', return_value=concurrency_config),
          patch('core.oss_client.oss2')):
        oss_client = OSSClient()
        
        # 测试带目标前缀的路径映射
        assert oss_client.get_target_path('file.txt') == 'migrated/file.txt'
        assert oss_client.get_target_path('data/file.txt') == 'migrated/data/file.txt'


def test_oss_client_upload_file():
    """测试OSS客户端上传小文件"""
    oss_config = {
        'endpoint': 'oss.cn-beijing.aliyuncs.com',
        'access_key': 'test-access-key',
        'secret_key': 'test-secret-key',
        'bucket_name': 'test-bucket',
        'target_prefix': ''
    }
    
    concurrency_config = {
        'chunk_size': 8192
    }
    
    with (patch('core.oss_client.config_loader.get_oss_config', return_value=oss_config),
          patch('core.oss_client.config_loader.get_concurrency_config', return_value=concurrency_config),
          patch('core.oss_client.oss2') as mock_oss2):
        mock_bucket = Mock()
        mock_oss2.Bucket.return_value = mock_bucket
        
        # 模拟上传成功
        mock_bucket.put_object.return_value = Mock(status=200, headers={'ETag': 'etag123'})
        
        oss_client = OSSClient()
        source_path = 'source.txt'
        target_path = 'target.txt'
        content = b'test content'
        
        # 测试上传文件
        result = oss_client.upload_file(source_path, target_path, content)
        
        assert result['success'] is True
        assert result['etag'] == 'etag123'
        
        # 验证put_object方法是否被正确调用
        mock_bucket.put_object.assert_called_once_with('target.txt', content)


def test_oss_client_upload_file_stream():
    """测试OSS客户端流式上传大文件"""
    oss_config = {
        'endpoint': 'oss.cn-beijing.aliyuncs.com',
        'access_key': 'test-access-key',
        'secret_key': 'test-secret-key',
        'bucket_name': 'test-bucket',
        'target_prefix': ''
    }
    
    concurrency_config = {
        'chunk_size': 8192
    }
    
    with (patch('core.oss_client.config_loader.get_oss_config', return_value=oss_config),
          patch('core.oss_client.config_loader.get_concurrency_config', return_value=concurrency_config),
          patch('core.oss_client.oss2') as mock_oss2):
        mock_bucket = Mock()
        mock_oss2.Bucket.return_value = mock_bucket
        
        # 模拟流式上传成功
        mock_result = Mock(status=200, headers={'ETag': 'etag456'})
        mock_bucket.put_object.return_value = mock_result
        
        oss_client = OSSClient()
        source_path = 'large_file.bin'
        target_path = 'large_file.bin'
        
        # 创建模拟文件流
        mock_stream = BytesIO(b'x' * 1024 * 1024 * 10)  # 10MB
        
        # 测试流式上传
        result = oss_client.upload_file_stream(source_path, target_path, mock_stream)
        
        assert result['success'] is True
        assert result['etag'] == 'etag456'
        
        # 验证put_object方法是否被正确调用
        mock_bucket.put_object.assert_called_once_with('large_file.bin', mock_stream)


def test_oss_client_upload_file_failure():
    """测试OSS客户端上传失败"""
    oss_config = {
        'endpoint': 'oss.cn-beijing.aliyuncs.com',
        'access_key': 'test-access-key',
        'secret_key': 'test-secret-key',
        'bucket_name': 'test-bucket',
        'target_prefix': ''
    }
    
    concurrency_config = {
        'chunk_size': 8192
    }
    
    with (patch('core.oss_client.config_loader.get_oss_config', return_value=oss_config),
          patch('core.oss_client.config_loader.get_concurrency_config', return_value=concurrency_config),
          patch('core.oss_client.oss2') as mock_oss2):
        mock_bucket = Mock()
        mock_oss2.Bucket.return_value = mock_bucket
        
        # 模拟上传失败
        mock_bucket.put_object.side_effect = Exception('Network error')
        
        oss_client = OSSClient()
        source_path = 'source.txt'
        target_path = 'target.txt'
        content = b'test content'
        
        # 测试上传文件失败
        result = oss_client.upload_file(source_path, target_path, content)
        
        assert result['success'] is False
        assert 'error' in result
        assert 'Network error' in result['error']


def test_oss_client_check_md5():
    """测试OSS客户端MD5校验"""
    oss_config = {
        'endpoint': 'oss.cn-beijing.aliyuncs.com',
        'access_key': 'test-access-key',
        'secret_key': 'test-secret-key',
        'bucket_name': 'test-bucket',
        'target_prefix': ''
    }
    
    concurrency_config = {
        'chunk_size': 8192
    }
    
    with (patch('core.oss_client.config_loader.get_oss_config', return_value=oss_config),
          patch('core.oss_client.config_loader.get_concurrency_config', return_value=concurrency_config),
          patch('core.oss_client.oss2') as mock_oss2):
        mock_bucket = Mock()
        mock_oss2.Bucket.return_value = mock_bucket
        
        # 模拟文件存在且MD5匹配
        mock_bucket.head_object.return_value = Mock(headers={'ETag': '"md5hash123"'})
        
        oss_client = OSSClient()
        target_path = 'file.txt'
        md5_hash = 'md5hash123'
        
        # 测试MD5校验通过
        result = oss_client.check_md5(target_path, md5_hash)
        assert result is True
        
        # 模拟文件存在但MD5不匹配
        mock_bucket.head_object.return_value = Mock(headers={'ETag': '"differentmd5"'})
        result = oss_client.check_md5(target_path, md5_hash)
        assert result is False
        
        # 模拟文件不存在
        mock_bucket.head_object.side_effect = mock_oss2.exceptions.NoSuchKey
        result = oss_client.check_md5(target_path, md5_hash)
        assert result is False


def test_oss_client_upload_file_with_md5_match():
    """测试MD5匹配时跳过上传"""
    oss_config = {
        'endpoint': 'oss.cn-beijing.aliyuncs.com',
        'access_key': 'test-access-key',
        'secret_key': 'test-secret-key',
        'bucket_name': 'test-bucket',
        'target_prefix': ''
    }
    
    concurrency_config = {
        'chunk_size': 8192
    }
    
    with (patch('core.oss_client.config_loader.get_oss_config', return_value=oss_config),
          patch('core.oss_client.config_loader.get_concurrency_config', return_value=concurrency_config),
          patch('core.oss_client.oss2') as mock_oss2):
        mock_bucket = Mock()
        mock_oss2.Bucket.return_value = mock_bucket
        
        # 模拟文件存在且MD5匹配
        mock_bucket.head_object.return_value = Mock(headers={'ETag': '"md5hash123"'})
        
        oss_client = OSSClient()
        source_path = 'file.txt'
        target_path = 'file.txt'
        content = b'test content'
        
        # 测试上传时MD5匹配，应该跳过上传
        result = oss_client.upload_file(source_path, target_path, content, md5_hash='md5hash123')
        
        assert result['success'] is True
        assert result['skipped'] is True
        assert result['message'] == 'File already exists with same MD5, skipping'
        
        # 验证put_object方法没有被调用
        mock_bucket.put_object.assert_not_called()
