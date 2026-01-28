#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证迁移后的文件是否具有公共读写的 ACL 权限
"""

import logging
from botocore import session
from botocore.config import Config

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_oss_client(endpoint_url, ak, sk):
    """
    创建联通云OSS的S3客户端
    """
    try:
        sess = session.Session()
        client = sess.create_client(
            's3',
            use_ssl=True,
            verify=False,
            endpoint_url=endpoint_url,
            aws_access_key_id=ak,
            aws_secret_access_key=sk,
            config=Config(
                signature_version="s3v4",
                connect_timeout=30,
                read_timeout=30,
                s3={'addressing_style': 'path'}
            )
        )
        logger.info("OSS客户端创建成功")
        return client
    except Exception as e:
        logger.error(f"OSS客户端创建失败: {str(e)}")
        raise

def get_object_acl(client, bucket_name, object_key):
    """
    获取对象的ACL权限
    """
    try:
        response = client.get_object_acl(
            Bucket=bucket_name,
            Key=object_key
        )
        logger.info(f"成功获取对象 [{bucket_name}/{object_key}] 的ACL")
        return response
    except Exception as e:
        logger.error(f"获取对象ACL失败: {str(e)}")
        raise

def main():
    """
    主函数
    """
    try:
        # 配置参数
        OSS_ENDPOINT = "https://obs-tj.cucloud.cn"
        ACCESS_KEY = "7412D2DA2D9A4F0CB8ECD4173B68A2397607"
        SECRET_KEY = "A6170506D4614680AEB9AC143C0818F38675"
        BUCKET_NAME = "hf-test"
        
        # 要验证的对象列表
        object_keys = [
            "20250617_0a59a99bf3944f49a6c508e4b09b2587.jpg",
            "20250617_63cd8c0946a64807aec509a0a9c78753.jpg",
            "20250617_8a926f5df21b4cd48381c3231697ffac.jpg",
            "20250617_e2c51f43a88b4d16ab98a85fd2a9ef3f.jpg",
            "20250623_4d39163fac704991b12931b1046ba945.jpg"
        ]
        
        # 创建OSS客户端
        oss_client = create_oss_client(OSS_ENDPOINT, ACCESS_KEY, SECRET_KEY)
        
        # 验证每个对象的ACL
        for object_key in object_keys:
            try:
                acl_response = get_object_acl(oss_client, BUCKET_NAME, object_key)
                # 打印ACL信息
                logger.info(f"对象 [{object_key}] 的ACL信息:")
                for grant in acl_response.get('Grants', []):
                    logger.info(f"  - {grant.get('Grantee', {}).get('Type', '')}: {grant.get('Permission', '')}")
                logger.info(f"  - Owner: {acl_response.get('Owner', {}).get('DisplayName', '')}")
            except Exception as e:
                logger.error(f"验证对象 [{object_key}] 的ACL失败: {str(e)}")
                continue
        
    except Exception as e:
        logger.error(f"脚本执行失败: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()