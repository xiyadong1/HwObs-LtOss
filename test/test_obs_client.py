#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试OBS客户端模块
"""
import pytest
from unittest.mock import Mock, patch
from core.obs_client import OBSClient, get_obs_client, global_obs_client

# 测试前清理全局实例
def setup_module():
    global global_obs_client
    global_obs_client = None

# 测试后清理全局实例
def teardown_module():
    global global_obs_client
    global_obs_client = None


def test_obs_client_initialization():
    """测试OBS客户端初始化"""
    obs_config = {
        'endpoint': 'obs.cn-north-4.myhuaweicloud.com',
        'access_key': 'test-access-key',
        'secret_key': 'test-secret-key',
        'bucket_name': 'test-bucket',
        'prefix': '',
        'exclude_suffixes': []
    }
    
    with (patch('core.obs_client.config_loader.get_obs_config', return_value=obs_config),
          patch('obs.ObsClient') as mock_obs_client):
        # 创建OBS客户端实例
        obs_client = OBSClient()
        
        # 验证OBS客户端是否正确初始化
        mock_obs_client.assert_called_once_with(
            access_key_id='test-access-key',
            secret_access_key='test-secret-key',
            server='obs.cn-north-4.myhuaweicloud.com'
        )
        assert obs_client.client == mock_obs_client.return_value


def test_obs_client_list_objects():
    """测试OBS客户端列举对象功能"""
    obs_config = {
        'endpoint': 'obs.cn-north-4.myhuaweicloud.com',
        'access_key': 'test-access-key',
        'secret_key': 'test-secret-key',
        'bucket_name': 'test-bucket',
        'prefix': '',
        'exclude_suffixes': []
    }
    
    with (patch('core.obs_client.config_loader.get_obs_config', return_value=obs_config),
          patch('obs.ObsClient') as mock_obs_client_class):
        # 模拟OBS客户端的listObjects方法返回值
        mock_obs_client = Mock()
        mock_obs_client_class.return_value = mock_obs_client
        
        # 模拟第一页结果
        mock_obs_client.listObjects.return_value = Mock(
            status=200,
            body=Mock(
                contents=[
                    Mock(key='file1.txt', size=1024, etag='etag1'),
                    Mock(key='file2.log', size=2048, etag='etag2'),
                    Mock(key='dir1/file3.txt', size=3072, etag='etag3')
                ],
                nextMarker=None
            )
        )
        
        obs_client = OBSClient()
        
        # 测试列举所有对象
        objects = list(obs_client.list_objects())
        
        assert len(objects) == 3
        assert objects[0] == {'key': 'file1.txt', 'size': 1024, 'etag': 'etag1'}
        assert objects[1] == {'key': 'file2.log', 'size': 2048, 'etag': 'etag2'}
        assert objects[2] == {'key': 'dir1/file3.txt', 'size': 3072, 'etag': 'etag3'}
        
        # 验证listObjects方法是否被正确调用
        mock_obs_client.listObjects.assert_called_once_with(
            Bucket='test-bucket',
            Prefix='',
            Marker=None,
            MaxKeys=1000
        )


def test_obs_client_list_objects_with_prefix():
    """测试带前缀过滤的对象列举"""
    obs_config = {
        'endpoint': 'obs.cn-north-4.myhuaweicloud.com',
        'access_key': 'test-access-key',
        'secret_key': 'test-secret-key',
        'bucket_name': 'test-bucket',
        'prefix': 'dir1/',
        'exclude_suffixes': []
    }
    
    with (patch('core.obs_client.config_loader.get_obs_config', return_value=obs_config),
          patch('obs.ObsClient') as mock_obs_client_class):
        mock_obs_client = Mock()
        mock_obs_client_class.return_value = mock_obs_client
        
        mock_obs_client.listObjects.return_value = Mock(
            status=200,
            body=Mock(
                contents=[
                    Mock(key='dir1/file3.txt', size=3072, etag='etag3'),
                    Mock(key='dir1/file4.txt', size=4096, etag='etag4')
                ],
                nextMarker=None
            )
        )
        
        obs_client = OBSClient()
        
        # 测试带前缀过滤的列举
        objects = list(obs_client.list_objects())
        
        assert len(objects) == 2
        assert all(obj['key'].startswith('dir1/') for obj in objects)
        
        # 验证listObjects方法是否被正确调用
        mock_obs_client.listObjects.assert_called_once_with(
            Bucket='test-bucket',
            Prefix='dir1/',
            Marker=None,
            MaxKeys=1000
        )


def test_obs_client_list_objects_with_exclude_suffixes():
    """测试带后缀排除的对象列举"""
    obs_config = {
        'endpoint': 'obs.cn-north-4.myhuaweicloud.com',
        'access_key': 'test-access-key',
        'secret_key': 'test-secret-key',
        'bucket_name': 'test-bucket',
        'prefix': '',
        'exclude_suffixes': ['.log', '.log.gz']
    }
    
    with (patch('core.obs_client.config_loader.get_obs_config', return_value=obs_config),
          patch('obs.ObsClient') as mock_obs_client_class):
        mock_obs_client = Mock()
        mock_obs_client_class.return_value = mock_obs_client
        
        mock_obs_client.listObjects.return_value = Mock(
            status=200,
            body=Mock(
                contents=[
                    Mock(key='file1.txt', size=1024, etag='etag1'),
                    Mock(key='file2.log', size=2048, etag='etag2'),
                    Mock(key='file3.log.gz', size=3072, etag='etag3')
                ],
                nextMarker=None
            )
        )
        
        obs_client = OBSClient()
        
        # 测试带后缀排除的列举
        objects = list(obs_client.list_objects())
        
        assert len(objects) == 1
        assert objects[0]['key'] == 'file1.txt'  # 只有.txt文件被包含


def test_obs_client_list_objects_with_pagination():
    """测试分页列举对象"""
    obs_config = {
        'endpoint': 'obs.cn-north-4.myhuaweicloud.com',
        'access_key': 'test-access-key',
        'secret_key': 'test-secret-key',
        'bucket_name': 'test-bucket',
        'prefix': '',
        'exclude_suffixes': []
    }
    
    with (patch('core.obs_client.config_loader.get_obs_config', return_value=obs_config),
          patch('obs.ObsClient') as mock_obs_client_class):
        mock_obs_client = Mock()
        mock_obs_client_class.return_value = mock_obs_client
        
        # 模拟第一页和第二页结果
        mock_obs_client.listObjects.side_effect = [
            Mock(
                status=200,
                body=Mock(
                    contents=[
                        Mock(key='file1.txt', size=1024, etag='etag1'),
                        Mock(key='file2.txt', size=2048, etag='etag2')
                    ],
                    nextMarker='file2.txt'
                )
            ),
            Mock(
                status=200,
                body=Mock(
                    contents=[
                        Mock(key='file3.txt', size=3072, etag='etag3')
                    ],
                    nextMarker=None
                )
            )
        ]
        
        obs_client = OBSClient()
        
        # 测试分页列举
        objects = list(obs_client.list_objects())
        
        assert len(objects) == 3
        assert objects[0]['key'] == 'file1.txt'
        assert objects[1]['key'] == 'file2.txt'
        assert objects[2]['key'] == 'file3.txt'
        
        # 验证listObjects方法被调用了两次
        assert mock_obs_client.listObjects.call_count == 2
        mock_obs_client.listObjects.assert_any_call(
            Bucket='test-bucket',
            Prefix='',
            Marker=None,
            MaxKeys=1000
        )
        mock_obs_client.listObjects.assert_any_call(
            Bucket='test-bucket',
            Prefix='',
            Marker='file2.txt',
            MaxKeys=1000
        )


def test_obs_client_list_objects_failure():
    """测试列举对象失败的情况"""
    obs_config = {
        'endpoint': 'obs.cn-north-4.myhuaweicloud.com',
        'access_key': 'test-access-key',
        'secret_key': 'test-secret-key',
        'bucket_name': 'test-bucket',
        'prefix': '',
        'exclude_suffixes': []
    }
    
    with (patch('core.obs_client.config_loader.get_obs_config', return_value=obs_config),
          patch('obs.ObsClient') as mock_obs_client_class):
        mock_obs_client = Mock()
        mock_obs_client_class.return_value = mock_obs_client
        
        # 模拟API调用失败
        mock_obs_client.listObjects.return_value = Mock(
            status=403,
            errorMessage='Access Denied'
        )
        
        obs_client = OBSClient()
        
        # 测试列举对象失败时是否抛出异常
        with pytest.raises(Exception, match='OBS列举文件失败：Access Denied'):
            list(obs_client.list_objects())
