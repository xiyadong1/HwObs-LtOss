#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试环境变量加载的脚本
"""

import os
from dotenv import load_dotenv

# 打印当前工作目录
print(f"当前工作目录: {os.getcwd()}")

# 直接读取.env文件内容
with open('.env', 'r', encoding='utf-8') as f:
    env_content = f.read()
    print(f".env文件内容:\n{env_content}")

# 尝试加载环境变量
load_dotenv()
print("\n加载后的环境变量:")
print(f"OBS_ACCESS_KEY: {os.getenv('OBS_ACCESS_KEY')}")
print(f"OSS_ACCESS_KEY: {os.getenv('OSS_ACCESS_KEY')}")
print(f"ALIYUN_ACCESS_KEY: {os.getenv('ALIYUN_ACCESS_KEY')}")
print(f"ALIYUN_SECRET_KEY: {os.getenv('ALIYUN_SECRET_KEY')}")
