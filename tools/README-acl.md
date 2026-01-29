# ACL 批量修改工具

这是一个用于批量修改联通云 OSS 桶内对象访问控制列表（ACL）的工具，支持递归处理所有对象并设置统一的权限策略。

## 功能特性

- **批量修改 ACL**：支持递归处理桶内所有对象的 ACL 设置
- **并发处理**：可配置并发线程数，提高处理效率
- **灵活配置**：通过配置文件设置目标 ACL 策略、处理前缀等参数
- **详细日志**：提供完整的执行日志和统计信息
- **错误处理**：捕获并记录失败的对象，便于后续处理

## 安装依赖

工具依赖以下 Python 库：
- botocore
- requests
- pyyaml
- python-dotenv

可以通过以下命令安装：

```bash
pip install botocore requests pyyaml python-dotenv
```

## 配置

工具支持两种配置方式：

### 1. 默认配置文件

使用项目根目录下的 `config/config.yaml` 配置文件，其中包含 ACL 工具的配置选项：

```yaml
# ACL 工具配置
acl:
  # 目标 ACL 策略（private, public-read, public-read-write）
  target_acl: "public-read-write"
  # 并发处理线程数
  thread_count: 10
  # 批量处理大小
  batch_size: 100
  # 是否递归处理所有对象
  recursive: true
  # 处理前缀（可选，为空则处理整个桶）
  prefix: ""
  # 排除的文件后缀列表（可选）
  exclude_suffixes: []

# 联通云OSS配置
oss:
  # OSS桶名
  bucket_name: "your-bucket-name"
  # OSS endpoint
  endpoint: "https://your-oss-endpoint"
  # 访问密钥（可选，也可以通过环境变量设置）
  access_key: "your-access-key"
  secret_key: "your-secret-key"
```

### 2. 单独的配置文件

可以使用单独的配置文件，支持多桶配置。配置文件格式如下：

```yaml
# 多桶配置示例
buckets:
  # 桶 1 配置
  - endpoint: "https://obs-tj.cucloud.cn"
    access_key: "your-access-key"
    secret_key: "your-secret-key"
    bucket_name: "bucket-1"
    target_acl: "public-read-write"
    thread_count: 10
    batch_size: 100
    recursive: true
    prefix: ""
    exclude_suffixes: []
  
  # 桶 2 配置
  - endpoint: "https://obs-tj.cucloud.cn"
    access_key: "your-access-key"
    secret_key: "your-secret-key"
    bucket_name: "bucket-2"
    target_acl: "public-read"
    thread_count: 15
    batch_size: 100
    recursive: true
    prefix: "images/"
    exclude_suffixes: [".log", ".tmp"]
```

访问密钥可以通过环境变量设置（推荐）：
- `OSS_ACCESS_KEY`：Access Key ID
- `OSS_SECRET_KEY`：Secret Access Key

也可以直接在配置文件中设置（不推荐用于生产环境）。

## 使用方法

### 基本语法

在项目根目录下执行：

```bash
# 使用默认配置文件
python tools/acl_tool.py

# 使用指定的配置文件
python tools/acl_tool.py --config tools/acl_config.yaml

# 或使用短选项
python tools/acl_tool.py -c tools/acl_config.yaml
```

### 执行流程

1. 工具加载配置文件中的 OSS 连接信息和 ACL 工具配置
2. 验证配置的有效性，包括 OSS 连接信息和目标 ACL 策略
3. 创建 OSS 客户端连接
4. 递归列出桶内所有符合条件的对象
5. 使用多线程并发处理对象的 ACL 设置
6. 输出详细的执行结果和统计信息

### 配置说明

| 配置项 | 类型 | 默认值 | 说明 |
|-------|------|-------|------|
| `endpoint` | string | - | OSS 服务端点 |
| `access_key` | string | - | Access Key ID |
| `secret_key` | string | - | Secret Access Key |
| `bucket_name` | string | - | 桶名称 |
| `target_acl` | string | "public-read-write" | 目标 ACL 策略，支持：private, public-read, public-read-write |
| `thread_count` | integer | 10 | 并发处理线程数 |
| `batch_size` | integer | 100 | 批量处理大小 |
| `recursive` | boolean | true | 是否递归处理所有对象 |
| `prefix` | string | "" | 处理前缀，为空则处理整个桶 |
| `exclude_suffixes` | list | [] | 排除的文件后缀列表 |

### 支持的 ACL 策略

| 策略名称 | 描述 |
|---------|------|  
| `private` | 私有读写，只有桶所有者可以访问 |
| `public-read` | 公共读私有写，任何人都可以读取，但只有桶所有者可以写入 |
| `public-read-write` | 公共读写，任何人都可以读取和写入 |

## 示例

### 示例1：使用默认配置文件

在 `config/config.yaml` 中设置：

```yaml
# ACL 工具配置
acl:
  target_acl: "public-read-write"
  prefix: ""
  recursive: true

# 联通云OSS配置
oss:
  bucket_name: "hf-test"
  endpoint: "https://obs-tj.cucloud.cn"
  access_key: "your-access-key"
  secret_key: "your-secret-key"
```

执行命令：

```bash
python tools/acl_tool.py
```

### 示例2：使用单独的配置文件（单桶）

创建 `tools/acl_config.yaml` 文件：

```yaml
# 单桶配置
oss:
  endpoint: "https://obs-tj.cucloud.cn"
  access_key: "your-access-key"
  secret_key: "your-secret-key"
  bucket_name: "hf-test"

acl:
  target_acl: "public-read"
  prefix: "images/"
  recursive: true
  exclude_suffixes: [".log"]
```

执行命令：

```bash
python tools/acl_tool.py --config tools/acl_config.yaml
```

### 示例3：使用单独的配置文件（多桶）

创建 `tools/multi_bucket_config.yaml` 文件：

```yaml
# 多桶配置
buckets:
  - endpoint: "https://obs-tj.cucloud.cn"
    access_key: "your-access-key"
    secret_key: "your-secret-key"
    bucket_name: "bucket-1"
    target_acl: "public-read-write"
    thread_count: 10
    recursive: true
  
  - endpoint: "https://obs-tj.cucloud.cn"
    access_key: "your-access-key"
    secret_key: "your-secret-key"
    bucket_name: "bucket-2"
    target_acl: "public-read"
    thread_count: 15
    prefix: "docs/"
    exclude_suffixes: [".tmp"]
```

执行命令：

```bash
python tools/acl_tool.py --config tools/multi_bucket_config.yaml
```

## 执行结果

工具执行完成后，会输出详细的统计信息：

### 单桶执行结果

```
=== ACL 批量修改完成 ===
总对象数: 19
成功修改: 19
失败修改: 0
耗时: 0.27 秒
平均速度: 71.25 对象/秒
```

### 多桶执行结果

```
=== 处理桶 1/2: bucket-1 ===
=== ACL 批量修改完成 ===
总对象数: 10
成功修改: 10
失败修改: 0
耗时: 0.15 秒
平均速度: 66.67 对象/秒

=== 处理桶 2/2: bucket-2 ===
=== ACL 批量修改完成 ===
总对象数: 15
成功修改: 15
失败修改: 0
耗时: 0.20 秒
平均速度: 75.00 对象/秒

=== 总体执行结果 ===
总桶数: 2
总对象数: 25
成功修改: 25
失败修改: 0
```

如果有失败的对象，会列出前 10 个失败的对象及其错误信息。

## 注意事项

1. 确保配置文件中的 OSS 访问信息正确无误
2. 递归操作时请谨慎，特别是设置为公共读写权限时，避免意外暴露敏感数据
3. 并发数不宜设置过大，建议根据网络状况和系统资源合理配置
4. 工具使用了 `botocore` 库来实现 OSS 客户端，这是 AWS SDK for Python 的核心库，与联通云 OSS 兼容
5. 工具默认忽略 SSL 证书验证，生产环境建议修改为 `verify=True`



## 日志

操作日志会输出到控制台，同时也会记录到项目的日志系统中。可以通过修改 `config/config.yaml` 中的日志配置来调整日志级别和输出方式：

```yaml
log:
  # 日志路径
  path: ./migrate_log/
  # 日志级别（DEBUG/INFO/WARNING/ERROR/CRITICAL）
  level: INFO
```