#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试OBS客户端分页功能的脚本
"""

import sys
import os
# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.obs_client import OBSClient
from config.config_loader import config_loader
from log.logger import logger


def test_pagination():
    """
    测试OBS客户端分页功能
    """
    logger.info("开始测试OBS客户端分页功能...")
    
    # 获取桶映射配置
    bucket_mappings = config_loader.get_bucket_mappings()
    
    if not bucket_mappings:
        logger.error("未配置桶映射")
        return
    
    # 测试第一个桶映射
    bucket_mapping = bucket_mappings[0]
    logger.info(f"测试桶映射：{bucket_mapping['obs_bucket']}")
    
    # 创建OBS客户端
    obs_config = {
        'bucket_name': bucket_mapping['obs_bucket'],
        'prefix': bucket_mapping['obs_prefix'],
        'exclude_suffixes': bucket_mapping['exclude_suffixes']
    }
    
    obs_client = OBSClient(bucket_config=obs_config)
    
    try:
        # 逐个获取文件，记录数量
        file_count = 0
        marker_count = 0
        current_marker = None
        
        # 直接使用生成器，不转换为列表
        for file_info in obs_client.list_objects():
            file_count += 1
            
            # 每100个文件打印一次进度
            if file_count % 100 == 0:
                logger.info(f"已获取 {file_count} 个文件")
        
        logger.info(f"分页测试完成！共获取到 {file_count} 个文件")
        
        # 对比转换为列表的方式
        logger.info("\n测试转换为列表的方式...")
        files_list = list(obs_client.list_objects())
        logger.info(f"使用list()获取到 {len(files_list)} 个文件")
        
    except Exception as e:
        logger.error(f"分页测试失败：{str(e)}")
        raise
    finally:
        # 关闭客户端
        obs_client.close()


if __name__ == "__main__":
    test_pagination()
