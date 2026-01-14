#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基本OSS上传测试脚本
"""

import os
import sys
import hashlib
from core.oss_client import OSSClient
from log.logger import logger


def test_basic_upload():
    """测试基本文件上传功能"""
    logger.info("开始测试基本文件上传功能")
    
    # 创建测试文件
    test_content = b"Test content for basic upload"
    test_files = [
        "test_basic.txt",  # 简单文件名
        "中文测试.txt",     # 中文文件名
    ]
    
    # 初始化OSS客户端
    oss_client = OSSClient()
    
    try:
        # 计算文件MD5
        etag = hashlib.md5(test_content).hexdigest()
        file_size = len(test_content)
        
        for test_file in test_files:
            logger.info(f"\n测试上传文件：{test_file}")
            
            try:
                # 上传文件
                success, error_msg = oss_client.upload_file(
                    obs_path=test_file,
                    content=test_content,
                    file_size=file_size,
                    etag=etag
                )
                
                if success:
                    logger.info(f"✅ 上传成功：{test_file}")
                else:
                    logger.error(f"❌ 上传失败：{test_file}，错误：{error_msg}")
                    
            except Exception as e:
                logger.error(f"❌ 测试失败：{test_file}，异常：{str(e)}")
        
        logger.info("\n✅ 基本文件上传测试完成！")
        
    finally:
        # 关闭客户端
        oss_client.close()


if __name__ == "__main__":
    test_basic_upload()