#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基于boto3 SDK的OSS上传测试脚本
"""

import os
import sys
import boto3
from dotenv import load_dotenv

# 加载.env文件
load_dotenv()

# 配置参数
REGION = "obs-tj"
BUCKET_NAME = "hf-test"
ENDPOINT = "https://obs-tj.cucloud.cn"  # 包含协议
OBJECT_KEY = "test_boto3_upload.txt"

# 从环境变量获取AK/SK
ACCESS_KEY = os.getenv('OSS_ACCESS_KEY')
SECRET_KEY = os.getenv('OSS_SECRET_KEY')

# 验证参数
if not all([ACCESS_KEY, SECRET_KEY, ENDPOINT, BUCKET_NAME, REGION]):
    print("缺少必要参数!")
    print(f"ACCESS_KEY: {ACCESS_KEY}")
    print(f"SECRET_KEY: {SECRET_KEY}")
    print(f"ENDPOINT: {ENDPOINT}")
    print(f"BUCKET_NAME: {BUCKET_NAME}")
    print(f"REGION: {REGION}")
    sys.exit(1)

# 创建测试文件
TEST_FILE_PATH = "test_upload_content.txt"
with open(TEST_FILE_PATH, "w", encoding="utf-8") as f:
    f.write("这是一个使用boto3 SDK上传的测试文件。")

# 读取测试文件内容
with open(TEST_FILE_PATH, "rb") as f:
    file_content = f.read()
file_size = len(file_content)

print("=== 基于boto3 SDK的OSS上传测试 ===")
print(f"Region: {REGION}")
print(f"Bucket: {BUCKET_NAME}")
print(f"Endpoint: {ENDPOINT}")
print(f"Object Key: {OBJECT_KEY}")
print(f"测试文件: {TEST_FILE_PATH}")
print(f"文件大小: {file_size} 字节")
print(f"AK: {ACCESS_KEY[:10]}...")
print(f"SK: {SECRET_KEY[:10]}...")
print()

try:
    # 1. 创建boto3 S3客户端
    print("1. 创建boto3 S3客户端...")
    
    # 配置boto3使用s3v4签名
    s3_client = boto3.client(
        's3',
        region_name=REGION,
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=boto3.session.Config(signature_version='s3v4')  # 强制使用s3v4签名
    )
    
    print("   ✓ boto3 S3客户端创建成功")
    
    # 2. 测试桶是否存在
    print("2. 测试桶是否存在...")
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        print("   ✓ 桶存在")
    except Exception as e:
        print(f"   ⚠️  桶存在性检查失败: {str(e)}")
    
    # 3. 执行上传对象的请求
    print("3. 开始上传文件...")
    
    # 使用upload_file方法上传文件，这个方法会自动处理Content-Length等问题
    response = s3_client.upload_file(
        TEST_FILE_PATH,  # 本地文件路径
        BUCKET_NAME,     # 桶名
        OBJECT_KEY       # 对象键
    )
    
    print("   ✓ 文件上传成功!")
    
    # 验证上传结果
    print("   - 验证上传结果...")
    head_response = s3_client.head_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY)
    print(f"   - 文件大小: {head_response['ContentLength']} 字节")
    print(f"   - ETag: {head_response['ETag']}")
    
    # 4. 验证上传结果
    print("4. 验证上传结果...")
    
    # 下载文件
    download_response = s3_client.get_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY)
    downloaded_content = download_response['Body'].read()
    
    print(f"   ✓ 下载验证成功!")
    print(f"   - 下载文件大小: {len(downloaded_content)} 字节")
    print(f"   - 文件内容: {downloaded_content.decode('utf-8')}")
    
    # 5. 清理测试文件
    print("5. 清理测试文件...")
    
    # 删除OSS上的测试文件
    delete_response = s3_client.delete_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY)
    if delete_response['ResponseMetadata']['HTTPStatusCode'] == 204:
        print(f"   ✓ OSS上的测试文件已删除")
    
    # 删除本地测试文件
    os.remove(TEST_FILE_PATH)
    print(f"   ✓ 本地测试文件已删除")
    
    print("\n✅ 所有测试都成功了！")
    print("boto3 SDK上传功能正常工作")
    
except Exception as e:
    print(f"\n❌ 错误: {type(e).__name__}")
    print(f"   错误信息: {str(e)}")
    import traceback
    traceback.print_exc()
    
    # 清理本地测试文件
    if os.path.exists(TEST_FILE_PATH):
        os.remove(TEST_FILE_PATH)
        print(f"\n已清理本地测试文件")
