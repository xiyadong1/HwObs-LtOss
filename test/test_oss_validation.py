#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OSS客户端核心功能验证脚本
用于验证OSS客户端的主要功能是否正常工作
"""

import os
import sys
import tempfile
from io import BytesIO
from unittest.mock import Mock, patch

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.oss_client import OSSClient

def test_oss_client_core_functionality():
    """测试OSS客户端核心功能"""
    print("=== 开始验证OSS客户端核心功能 ===")
    
    # 配置模拟
    oss_config = {
        'endpoint': 'oss.cn-north-1.unicomcloud.com',
        'access_key': 'test-access-key',
        'secret_key': 'test-secret-key',
        'bucket_name': 'test-bucket',
        'target_prefix': 'migrated/'
    }
    
    concurrency_config = {
        'chunk_size': 5 * 1024 * 1024  # 5MB
    }
    
    with (patch('core.oss_client.config_loader.get_oss_config', return_value=oss_config),
          patch('core.oss_client.config_loader.get_concurrency_config', return_value=concurrency_config),
          patch('core.oss_client.oss2.Auth') as mock_auth,
          patch('core.oss_client.oss2.Bucket') as mock_bucket):
        
        # 1. 测试初始化
        print("\n1. 测试OSS客户端初始化...")
        oss_client = OSSClient()
        print("   ✓ 初始化成功")
        
        # 验证endpoint处理
        expected_endpoint = 'https://oss.cn-north-1.unicomcloud.com'
        if oss_client.endpoint == expected_endpoint:
            print(f"   ✓ Endpoint处理正确：{oss_client.endpoint}")
        else:
            print(f"   ✗ Endpoint处理错误，预期：{expected_endpoint}，实际：{oss_client.endpoint}")
        
        # 2. 测试路径映射
        print("\n2. 测试路径映射...")
        test_path = 'data/file.txt'
        expected_target_path = 'migrated/data/file.txt'
        actual_target_path = oss_client.get_target_path(test_path)
        if actual_target_path == expected_target_path:
            print(f"   ✓ 路径映射正确：{test_path} -> {actual_target_path}")
        else:
            print(f"   ✗ 路径映射错误，预期：{expected_target_path}，实际：{actual_target_path}")
        
        # 3. 测试小文件上传
        print("\n3. 测试小文件上传...")
        mock_bucket_instance = Mock()
        mock_bucket.return_value = mock_bucket_instance
        mock_bucket_instance.object_exists.return_value = False
        mock_bucket_instance.put_object.return_value = Mock(status=200, etag='"test-etag"')
        
        # 重新创建客户端以应用mock
        oss_client = OSSClient()
        
        content = b'test content'
        file_size = len(content)
        etag = 'test-etag'
        
        success, error = oss_client.upload_file('source.txt', content, file_size, etag)
        if success:
            print("   ✓ 小文件上传成功")
        else:
            print(f"   ✗ 小文件上传失败：{error}")
        
        # 4. 测试大文件分片上传
        print("\n4. 测试大文件分片上传...")
        mock_bucket_instance.object_exists.return_value = False
        mock_bucket_instance.init_multipart_upload.return_value = Mock(upload_id='test-upload-id')
        mock_bucket_instance.upload_part.return_value = Mock(status=200, etag='"part-etag-1"')
        mock_bucket_instance.complete_multipart_upload.return_value = Mock(status=200, etag='"big-file-etag"')
        
        # 大文件内容（大于chunk_size）
        big_content = b'x' * (6 * 1024 * 1024)  # 6MB
        big_file_size = len(big_content)
        big_etag = 'big-file-etag'
        
        success, error = oss_client.upload_file('big-file.bin', big_content, big_file_size, big_etag)
        if success:
            print("   ✓ 大文件分片上传成功")
        else:
            print(f"   ✗ 大文件分片上传失败：{error}")
        
        # 5. 测试流式上传
        print("\n5. 测试流式上传...")
        mock_bucket_instance.object_exists.return_value = False
        mock_bucket_instance.put_object.return_value = Mock(status=200, etag='"stream-etag"')
        
        stream_content = b'stream content'
        stream = BytesIO(stream_content)
        stream_size = len(stream_content)
        stream_etag = 'stream-etag'
        
        success, error = oss_client.upload_file_stream('stream-file.txt', stream, stream_size, stream_etag)
        if success:
            print("   ✓ 流式上传成功")
        else:
            print(f"   ✗ 流式上传失败：{error}")
        
        print("\n=== OSS客户端核心功能验证完成 ===")

if __name__ == '__main__':
    test_oss_client_core_functionality()
