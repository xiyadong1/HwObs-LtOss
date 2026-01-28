# 联通云OSS操作工具

这是一个用于管理联通云OSS的命令行工具集，包含文件操作工具和ACL批量修改工具。

## 工具列表

1. **oss_tool.py**：文件操作工具，支持上传、下载、删除功能
2. **acl_tool.py**：ACL批量修改工具，支持批量修改对象访问控制列表

## 功能特性

- **上传文件**：支持单个文件上传和目录递归上传
- **下载文件**：支持单个文件下载和目录递归下载
- **删除文件**：支持单个文件删除和目录递归删除
- **批量修改 ACL**：支持递归处理桶内所有对象的 ACL 设置
- **并发操作**：可配置并发数，提高操作效率
- **进度日志**：详细记录操作过程和结果
- **灵活配置**：通过配置文件设置各种参数

## 安装依赖

工具依赖以下Python库：
- requests
- pyyaml
- python-dotenv
- botocore（仅ACL工具需要）

可以通过以下命令安装：

```bash
pip install requests pyyaml python-dotenv botocore
```

## 配置

工具支持自定义配置文件，配置文件格式如下：

```yaml
# 自定义OSS配置文件
oss:
  endpoint: "your-oss-endpoint"
  access_key: "your-access-key"
  secret_key: "your-secret-key"
  region: "your-region"
  
concurrency:
  chunk_size: 5242880  # 5MB
```

使用 `--config` 或 `-c` 参数指定配置文件路径：

```bash
python tools/oss_tool.py --config /path/to/config.yaml <action> [options]
```

## 使用方法

### 基本语法

```bash
python tools/oss_tool.py <action> [options]
```

### 操作类型

- `upload`：上传文件到OSS
- `download`：从OSS下载文件
- `delete`：删除OSS中的文件

### 上传文件

```bash
# 上传单个文件
python tools/oss_tool.py upload --bucket <bucket-name> --local-path <local-file> --oss-prefix <oss-prefix>

# 递归上传目录
python tools/oss_tool.py upload --bucket <bucket-name> --local-path <local-dir> --oss-prefix <oss-prefix> --recursive

# 配置并发数（默认5）
python tools/oss_tool.py upload --bucket <bucket-name> --local-path <local-file> --concurrency 10
```

**参数说明**：
- `--bucket`/`-b`：OSS桶名称
- `--local-path`/`-l`：本地文件或目录路径
- `--oss-prefix`/`-p`：OSS目标路径前缀
- `--recursive`/`-r`：递归上传目录
- `--concurrency`/`-n`：并发上传数量

### 下载文件

```bash
# 下载单个文件
python tools/oss_tool.py download --bucket <bucket-name> --oss-path <oss-file> --local-dir <local-dir>

# 递归下载目录
python tools/oss_tool.py download --bucket <bucket-name> --oss-path <oss-dir> --local-dir <local-dir> --recursive

# 配置并发数（默认5）
python tools/oss_tool.py download --bucket <bucket-name> --oss-path <oss-file> --local-dir <local-dir> --concurrency 10
```

**参数说明**：
- `--bucket`/`-b`：OSS桶名称
- `--oss-path`/`-o`：OSS文件或目录路径
- `--local-dir`/`-l`：本地目标目录
- `--recursive`/`-r`：递归下载目录
- `--concurrency`/`-n`：并发下载数量

### 删除文件

```bash
# 删除单个文件
python tools/oss_tool.py delete --bucket <bucket-name> --oss-path <oss-file>

# 递归删除目录
python tools/oss_tool.py delete --bucket <bucket-name> --oss-path <oss-dir> --recursive

# 配置并发数（默认10）
python tools/oss_tool.py delete --bucket <bucket-name> --oss-path <oss-file> --concurrency 20
```

**参数说明**：
- `--bucket`/`-b`：OSS桶名称
- `--oss-path`/`-o`：OSS文件或目录路径
- `--recursive`/`-r`：递归删除目录
- `--concurrency`/`-n`：并发删除数量

## 示例

### 示例1：上传单个文件

```bash
python tools/oss_tool.py upload --bucket my-bucket --local-path ./test.txt --oss-prefix data/
```

### 示例2：递归上传目录

```bash
python tools/oss_tool.py upload --bucket my-bucket --local-path ./data --oss-prefix backup/2026-01-14/ --recursive --concurrency 10
```

### 示例3：下载单个文件

```bash
python tools/oss_tool.py download --bucket my-bucket --oss-path data/test.txt --local-dir ./downloads/
```

### 示例4：递归下载目录

```bash
python tools/oss_tool.py download --bucket my-bucket --oss-path backup/2026-01-14/ --local-dir ./downloads/ --recursive
```

### 示例5：删除单个文件

```bash
python tools/oss_tool.py delete --bucket my-bucket --oss-path data/test.txt
```

### 示例6：递归删除目录

```bash
python tools/oss_tool.py delete --bucket my-bucket --oss-path backup/2026-01-14/ --recursive
```

### 示例7：删除桶中的所有文件

```bash
python tools/oss_tool.py --config tools/test_config.yaml delete --bucket hf-test --recursive
```

## 注意事项

1. 确保配置文件中的OSS访问信息正确无误
2. 递归操作时请谨慎，特别是删除操作，避免误删数据
3. 并发数不宜设置过大，建议根据网络状况和系统资源合理配置
4. 大文件上传功能基于现有OSSClient实现，支持自动MD5验证

## 日志

操作日志会记录在 `tools/` 目录下的 `oss_tool.log` 文件中，可以查看详细的操作过程和错误信息。

## ACL 工具使用说明

### 功能

ACL 工具 (`acl_tool.py`) 用于批量修改 OSS 桶内对象的访问控制列表（ACL），支持递归处理所有对象并设置统一的权限策略。

### 配置

ACL 工具使用项目根目录下的 `config/config.yaml` 配置文件，其中包含 ACL 工具的配置选项：

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
```

### 运行方法

在项目根目录下执行：

```bash
python tools/acl_tool.py
```

### 支持的 ACL 策略

- `private`：私有读写，只有桶所有者可以访问
- `public-read`：公共读私有写，任何人都可以读取，但只有桶所有者可以写入
- `public-read-write`：公共读写，任何人都可以读取和写入

### 执行结果

工具执行完成后，会输出详细的统计信息，包括总对象数、成功修改数、失败修改数和耗时等。

### 详细文档

请参考 `tools/README-acl.md` 文件获取更详细的 ACL 工具使用说明。
