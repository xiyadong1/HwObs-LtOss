import logging
# 修正botocore session的导入方式（核心修复点）
from botocore import session
from botocore.config import Config

# 配置日志，方便调试和查看执行结果
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_oss_client(endpoint_url, ak, sk):
    """
    创建联通云OSS的S3客户端
    :param endpoint_url: OSS服务的端点地址 (如 https://obs-tj.cucloud.cn)
    :param ak: Access Key ID
    :param sk: Secret Access Key
    :return: 配置好的S3客户端对象
    """
    try:
        # 修复session创建方式，兼容所有botocore版本
        sess = session.Session()
        # 配置客户端参数，适配联通云OSS（同步调整SSL配置）
        client = sess.create_client(
            's3',
            use_ssl=True,          # 你的端点是https，需开启SSL
            verify=False,          # 测试环境暂时忽略证书验证，生产环境建议改为True
            endpoint_url=endpoint_url,
            aws_access_key_id=ak,
            aws_secret_access_key=sk,
            config=Config(
                signature_version="s3v4",  # 联通云OSS推荐使用s3v4签名
                connect_timeout=30,        # 连接超时（秒）
                read_timeout=30,           # 读取超时（秒）
                s3={'addressing_style': 'path'}  # 路径模式访问（适配联通云OSS）
            )
        )
        logger.info("OSS客户端创建成功")
        return client
    except Exception as e:
        logger.error(f"OSS客户端创建失败: {str(e)}")
        raise

def set_object_acl(client, bucket_name, object_key, acl_policy):
    """
    设置OSS对象的ACL权限
    :param client: OSS客户端对象
    :param bucket_name: 桶名称
    :param object_key: 对象路径（如 "test/file.txt"）
    :param acl_policy: ACL策略（支持：private, public-read, public-read-write）
    :return: API响应结果
    """
    # 验证ACL策略的合法性
    valid_acls = ['private', 'public-read', 'public-read-write']
    if acl_policy not in valid_acls:
        raise ValueError(f"无效的ACL策略！支持的策略：{valid_acls}")

    try:
        # 调用PutObjectACL接口设置权限
        response = client.put_object_acl(
            ACL=acl_policy,
            Bucket=bucket_name,
            Key=object_key
        )
        logger.info(f"成功设置对象 [{bucket_name}/{object_key}] 的ACL为: {acl_policy}")
        logger.debug(f"API响应: {response}")
        return response
    except client.exceptions.NoSuchBucket:
        logger.error(f"桶 [{bucket_name}] 不存在")
        raise
    except client.exceptions.NoSuchKey:
        logger.error(f"对象 [{object_key}] 在桶 [{bucket_name}] 中不存在")
        raise
    except Exception as e:
        logger.error(f"设置对象ACL失败: {str(e)}")
        raise

def batch_set_object_acl(client, bucket_name, object_keys, acl_policy):
    """
    批量设置OSS对象的ACL权限
    :param client: OSS客户端对象
    :param bucket_name: 桶名称
    :param object_keys: 对象路径列表（如 ["test/file1.txt", "test/file2.jpg"]）
    :param acl_policy: ACL策略
    :return: 批量处理结果（字典：成功/失败的对象列表）
    """
    result = {
        "success": [],
        "failed": []
    }

    for obj_key in object_keys:
        try:
            set_object_acl(client, bucket_name, obj_key, acl_policy)
            result["success"].append(obj_key)
        except Exception as e:
            logger.error(f"处理对象 [{obj_key}] 失败: {str(e)}")
            result["failed"].append({
                "key": obj_key,
                "error": str(e)
            })

    logger.info(f"批量处理完成 - 成功: {len(result['success'])} 个, 失败: {len(result['failed'])} 个")
    return result

if __name__ == "__main__":
    # ===================== 配置参数（已保留你的实际配置）=====================
    OSS_ENDPOINT = "https://obs-tj.cucloud.cn"  # 联通云OSS的端点地址
    ACCESS_KEY = "7412D2DA2D9A4F0CB8ECD4173B68A2397607"            # 你的AK
    SECRET_KEY = "A6170506D4614680AEB9AC143C0818F38675"            # 你的SK
    BUCKET_NAME = "hf-test"          # 桶名称
    
    # 单个对象设置示例
    SINGLE_OBJECT_KEY = "20250617_0a59a99bf3944f49a6c508e4b09b2587.jpg"    # 要修改的对象路径
    TARGET_ACL = "public-read-write"                # 目标ACL策略
    
    # 批量对象设置示例（可选）
    # BATCH_OBJECT_KEYS = [
    #     "test/file1.txt",
    #     "test/images/photo.jpg",
    #     "docs/readme.md"
    # ]

    # ===================== 执行逻辑 =====================
    try:
        # 1. 创建OSS客户端
        oss_client = create_oss_client(OSS_ENDPOINT, ACCESS_KEY, SECRET_KEY)
        
        # 2. 单个对象设置ACL
        set_object_acl(oss_client, BUCKET_NAME, SINGLE_OBJECT_KEY, TARGET_ACL)
        
        # 3. 批量对象设置ACL（如需批量处理，取消下面注释）
        # batch_result = batch_set_object_acl(oss_client, BUCKET_NAME, BATCH_OBJECT_KEYS, TARGET_ACL)
        # print("批量处理结果:", batch_result)
        
    except Exception as e:
        logger.error(f"脚本执行失败: {str(e)}")
        exit(1)