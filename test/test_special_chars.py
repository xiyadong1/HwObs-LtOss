#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试包含特殊字符的文件名上传
"""

import os
import sys
import tempfile
import hashlib
from core.oss_client import OSSClient
from log.logger import logger
from config.config_loader import config_loader


def test_special_chars_upload():
    """测试包含特殊字符的文件名上传"""
    logger.info("开始测试特殊字符文件名上传功能")
    
    # 创建测试文件
    test_content = b"Test content for special characters"
    test_files = [
        "test_file.txt",
        "中文文件名.txt",
        "文件名称：包含冒号.txt",
        "文件名称 (包含括号).txt",
        "文件名称_包含下划线.txt",
        "文件名称-包含连字符.txt",
        "文件名称.包含点.txt",
        "文件名称/包含路径/分隔符.txt",
        "文件名称[包含方括号].txt",
        "文件名称{包含大括号}.txt",
        "文件名称包含空格.txt",
        "文件名称包含%百分号.txt",
        "文件名称包含&和符号.txt",
        "文件名称包含@at符号.txt",
        "文件名称包含#井号.txt",
        "文件名称包含$美元符.txt",
        "文件名称包含^脱字符.txt",
        "文件名称包含*星号.txt",
        "文件名称包含!感叹号.txt",
        "文件名称包含~波浪号.txt",
    ]
    
    # 初始化OSS客户端
    oss_client = OSSClient()
    
    try:
        # 计算文件MD5
        etag = hashlib.md5(test_content).hexdigest()
        file_size = len(test_content)
        
        success_count = 0
        failure_count = 0
        
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
                    success_count += 1
                else:
                    logger.error(f"❌ 上传失败：{test_file}，错误：{error_msg}")
                    failure_count += 1
                    
                # 检查文件是否存在
                exists = oss_client.object_exists(test_file)
                logger.info(f"🔍 文件存在检查：{exists}")
                
            except Exception as e:
                logger.error(f"❌ 测试失败：{test_file}，异常：{str(e)}")
                failure_count += 1
        
        # 打印测试结果
        logger.info(f"\n=== 测试结果 ===")
        logger.info(f"总测试数：{len(test_files)}")
        logger.info(f"成功数：{success_count}")
        logger.info(f"失败数：{failure_count}")
        
        if failure_count == 0:
            logger.info("🎉 所有测试用例都通过了！")
            return True
        else:
            logger.error("❌ 部分测试用例失败，请检查问题")
            return False
            
    finally:
        # 关闭客户端
        oss_client.close()


if __name__ == "__main__":
    test_special_chars_upload()
