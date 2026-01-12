#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基于手动AWS4-HMAC-SHA256签名的OSS上传测试脚本
"""

import os
import sys
import datetime
import hashlib
import hmac
import requests
from dotenv import load_dotenv

# 加载.env文件
load_dotenv()

# 配置参数
REGION = "obs-tj"
BUCKET_NAME = "hf-test"
ENDPOINT = "obs-tj.cucloud.cn"  # 不包含协议
OBJECT_KEY = "test_manual_sign_upload.txt"

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
    f.write("这是一个使用手动AWS4-HMAC-SHA256签名上传的测试文件。")

# 读取测试文件内容
with open(TEST_FILE_PATH, "rb") as f:
    file_content = f.read()
file_size = len(file_content)

print("=== 基于手动AWS4-HMAC-SHA256签名的OSS上传测试 ===")
print(f"Region: {REGION}")
print(f"Bucket: {BUCKET_NAME}")
print(f"Endpoint: {ENDPOINT}")
print(f"Object Key: {OBJECT_KEY}")
print(f"测试文件: {TEST_FILE_PATH}")
print(f"文件大小: {file_size} 字节")
print(f"AK: {ACCESS_KEY[:10]}...")
print(f"SK: {SECRET_KEY[:10]}...")
print()

# AWS4-HMAC-SHA256签名实现
def sign(key, msg):
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

def get_signature_key(key, date_stamp, region_name, service_name):
    k_date = sign(('AWS4' + key).encode('utf-8'), date_stamp)
    k_region = sign(k_date, region_name)
    k_service = sign(k_region, service_name)
    k_signing = sign(k_service, 'aws4_request')
    return k_signing

def sha256_hash(content):
    if isinstance(content, str):
        content = content.encode('utf-8')
    return hashlib.sha256(content).hexdigest()

try:
    # 1. 准备请求参数
    print("1. 准备请求参数...")
    
    # 生成时间戳
    t = datetime.datetime.utcnow()
    amz_date = t.strftime('%Y%m%dT%H%M%SZ')
    date_stamp = t.strftime('%Y%m%d')
    
    # 请求方法
    method = 'PUT'
    
    # 请求路径
    canonical_uri = f"/{OBJECT_KEY}"
    
    # 查询参数（无）
    canonical_querystring = ''
    
    # 计算文件哈希
    payload_hash = sha256_hash(file_content)
    
    # 构建规范头
    host = f"{BUCKET_NAME}.{ENDPOINT}"
    canonical_headers = ''
    canonical_headers += f"host:{host}\n"
    canonical_headers += f"x-amz-content-sha256:{payload_hash}\n"
    canonical_headers += f"x-amz-date:{amz_date}\n"
    
    # 构建已签名头
    signed_headers = 'host;x-amz-content-sha256;x-amz-date'
    
    # 构建规范请求
    canonical_request = ''
    canonical_request += f"{method}\n"
    canonical_request += f"{canonical_uri}\n"
    canonical_request += f"{canonical_querystring}\n"
    canonical_request += f"{canonical_headers}\n"
    canonical_request += f"{signed_headers}\n"
    canonical_request += f"{payload_hash}"
    
    # 2. 生成字符串签名
    print("2. 生成字符串签名...")
    algorithm = 'AWS4-HMAC-SHA256'
    credential_scope = f"{date_stamp}/{REGION}/s3/aws4_request"
    string_to_sign = ''
    string_to_sign += f"{algorithm}\n"
    string_to_sign += f"{amz_date}\n"
    string_to_sign += f"{credential_scope}\n"
    string_to_sign += f"{sha256_hash(canonical_request)}"
    
    # 3. 生成签名密钥
    print("3. 生成签名密钥...")
    signing_key = get_signature_key(SECRET_KEY, date_stamp, REGION, 's3')
    
    # 4. 计算签名
    signature = hmac.new(signing_key, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()
    
    # 5. 构建认证头
    credential = f"{ACCESS_KEY}/{credential_scope}"
    authorization_header = f"{algorithm} Credential={credential}, SignedHeaders={signed_headers}, Signature={signature}"
    
    # 6. 构建请求头
    headers = {
        'Host': host,
        'x-amz-content-sha256': payload_hash,
        'x-amz-date': amz_date,
        'Authorization': authorization_header,
        'Content-Length': str(file_size)
    }
    
    # 7. 发送请求
    print("4. 发送上传请求...")
    url = f"https://{host}{canonical_uri}"
    print(f"   请求URL: {url}")
    print(f"   请求方法: {method}")
    print(f"   请求头: {headers}")
    print(f"   文件大小: {file_size} 字节")
    
    response = requests.put(url, data=file_content, headers=headers, timeout=30)
    
    print(f"\n5. 处理响应...")
    print(f"   状态码: {response.status_code}")
    print(f"   响应头: {dict(response.headers)}")
    print(f"   响应内容: {response.text}")
    
    if response.status_code == 200:
        print("\n✅ 上传成功！")
        
        # 验证上传结果
        print("6. 验证上传结果...")
        
        # 构建下载请求
        download_headers = {
            'Host': host,
            'x-amz-date': amz_date,
            'x-amz-content-sha256': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',  # 空内容哈希
            'Authorization': authorization_header
        }
        
        download_response = requests.get(url, headers=download_headers, timeout=30)
        
        if download_response.status_code == 200:
            downloaded_content = download_response.content
            print(f"   ✓ 下载验证成功!")
            print(f"   - 下载文件大小: {len(downloaded_content)} 字节")
            print(f"   - 文件内容: {downloaded_content.decode('utf-8')}")
            
            # 清理测试文件
            print("7. 清理测试文件...")
            
            # 删除OSS上的测试文件
            delete_headers = {
                'Host': host,
                'x-amz-date': amz_date,
                'x-amz-content-sha256': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',  # 空内容哈希
                'Authorization': authorization_header
            }
            
            delete_response = requests.delete(url, headers=delete_headers, timeout=30)
            if delete_response.status_code == 204:
                print(f"   ✓ OSS上的测试文件已删除")
            else:
                print(f"   ⚠️  删除OSS测试文件失败: {delete_response.status_code}")
            
            # 删除本地测试文件
            os.remove(TEST_FILE_PATH)
            print(f"   ✓ 本地测试文件已删除")
            
            print("\n✅ 所有测试都成功了！")
            print("手动AWS4-HMAC-SHA256签名上传功能正常工作")
        else:
            print(f"\n❌ 下载验证失败: {download_response.status_code}")
            
    else:
        print(f"\n❌ 上传失败！状态码: {response.status_code}")
        print(f"   错误信息: {response.text}")
        
except Exception as e:
    print(f"\n❌ 错误: {type(e).__name__}")
    print(f"   错误信息: {str(e)}")
    import traceback
    traceback.print_exc()
    
    # 清理本地测试文件
    if os.path.exists(TEST_FILE_PATH):
        os.remove(TEST_FILE_PATH)
        print(f"\n已清理本地测试文件")
