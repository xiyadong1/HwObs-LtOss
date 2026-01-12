# 华为云OBS到联通云OSS批量迁移工具

## 项目概述

本项目是一个高可用的华为云OBS到联通云OSS的批量文件迁移工具，专门为生产环境大规模文件迁移设计，支持10万+文件、100TB+数据量的高效迁移。工具采用多线程并发处理，支持断点续传、流式迁移、MD5校验等特性，确保迁移过程的可靠性和高效性。

## 技术栈

- Python 3.10+
- 华为云OBS SDK (`esdk-obs-python==3.24.12`)
- 联通云OSS SDK (`oss2==2.18.1` - 兼容阿里云OSS SDK)
- 多线程并发处理
- python-dotenv (环境变量管理)
- PyYAML (配置文件解析)

## 项目结构

```
.
├── config/                 # 配置文件目录
│   ├── config.yaml         # 主配置文件
│   └── config_loader.py    # 配置加载模块
├── core/                   # 核心功能模块
│   ├── migrate_manager.py  # 迁移管理器
│   ├── migrate_task.py     # 迁移任务实现
│   ├── obs_client.py       # 华为云OBS客户端
│   └── oss_client.py       # 联通云OSS客户端
├── log/                    # 日志模块
│   ├── logger.py           # 通用日志器
│   └── migrate_logger.py   # 迁移专用日志器
├── migrate_log/            # 日志文件存储目录
├── script/                 # 执行脚本
│   ├── migrate.py          # 主迁移脚本
│   └── retry_failed.py     # 失败重试脚本
├── test/                   # 单元测试目录
├── .env.example            # 环境变量示例文件
├── requirements.txt        # 依赖列表
└── README.md               # 项目说明文档
```

## 核心功能

### 1. 多线程并发迁移
- 可配置并发线程数（默认50）
- 支持大文件分片上传（默认5MB/片）
- 优化的连接池配置，提高并发性能

### 2. 智能迁移策略
- 小文件：直接内存缓存上传
- 大文件：流式迁移（默认>50MB触发）
- 自动选择最优上传方式

### 3. 数据完整性保障
- MD5校验：上传前后验证文件完整性
- 断点续传：支持上传中断后的续传
- 失败重试：可配置重试次数和间隔

### 4. 灵活的过滤机制
- 支持按前缀过滤源文件
- 支持按后缀排除特定文件类型

### 5. 详细的日志和监控
- 实时进度显示
- 分级日志记录
- 迁移完成报告生成

### 6. 多桶并行迁移
- 支持同时迁移多个OBS-OSS桶对
- 桶级别的独立配置
- 灵活的前缀映射和过滤规则

## 安装步骤

### 1. 克隆项目
```bash
git clone <repository-url>
cd HwObs-LtOss
```

### 2. 安装依赖
```bash
pip install -r requirements.txt
```

## 配置方法

### 1. 环境变量配置

创建`.env`文件（基于`.env.example`模板），配置华为云OBS和联通云OSS的认证信息：

```bash
# 复制环境变量示例文件
cp .env.example .env

# 编辑.env文件，填写实际的AK/SK
vi .env
```

```dotenv
# 华为云OBS认证信息
OBS_ACCESS_KEY=your_obs_access_key_here
OBS_SECRET_KEY=your_obs_secret_key_here

# 联通云OSS认证信息
OSS_ACCESS_KEY=your_oss_access_key_here
OSS_SECRET_KEY=your_oss_secret_key_here
```

### 2. 主配置文件

编辑`config/config.yaml`文件，配置迁移参数：

#### 并发配置
```yaml
concurrency:
  thread_count: 50                # 并发线程数
  chunk_size: 5242880             # 分片大小（5MB）
  streaming_threshold: 52428800   # 流式迁移阈值（50MB）
```

#### 重试配置
```yaml
retry:
  max_attempts: 3                # 最大重试次数
  interval: 5                    # 重试间隔（秒）
```

#### 日志配置
```yaml
log:
  path: ./migrate_log/           # 日志存储路径
  level: INFO                    # 日志级别
  max_size: 100                  # 单个日志文件最大大小（MB）
  backup_count: 7                # 日志文件保留天数
```

#### 华为云OBS配置
```yaml
obs:
  bucket_name: "your-obs-bucket-name"  # OBS桶名
  endpoint: "obs.cn-north-1.myhuaweicloud.com"  # OBS端点
  prefix: ""                     # 文件前缀过滤（可选）
  exclude_suffixes: [".log", ".tmp"]  # 排除文件后缀（可选）
```

#### 联通云OSS配置
```yaml
oss:
  bucket_name: "your-oss-bucket-name"  # OSS桶名
  endpoint: "oss.cn-north-1.unicomcloud.com"  # OSS端点
  target_prefix: ""              # 目标路径前缀（可选）
  client:                        # OSS客户端配置
    connect_timeout: 30          # 连接超时（秒）
    connection_pool_size: 100    # 连接池大小
```

#### 多桶迁移配置（可选）

支持同时迁移多个OBS-OSS桶对，配置示例：

```yaml
bucket_mappings:
  - obs_bucket: "obs-bucket-1"
    oss_bucket: "oss-bucket-1"
    obs_prefix: "data/"
    oss_prefix: "migrated_data/"
    exclude_suffixes: [".log", ".tmp"]
  - obs_bucket: "obs-bucket-2"
    oss_bucket: "oss-bucket-2"
    obs_prefix: "backup/"
    oss_prefix: "oss_backup/"
    exclude_suffixes: []
```

**参数说明：**
- `obs_bucket`: 源华为云OBS桶名
- `oss_bucket`: 目标联通云OSS桶名
- `obs_prefix`: OBS源文件前缀（可选）
- `oss_prefix`: OSS目标文件前缀（可选）
- `exclude_suffixes`: 排除的文件后缀列表（可选）

## 使用方法

### 启动迁移任务

```bash
python script/migrate.py
```

### 查看迁移进度

迁移过程中，控制台会实时显示迁移进度：
```
迁移进度：1234/5678 (21.73%)
```

### 查看迁移日志

详细日志存储在`./migrate_log/`目录下，包含：
- 迁移任务日志
- 失败文件清单
- 每日汇总报告

## 失败重试

如果有文件迁移失败，可以使用重试脚本重新迁移失败的文件：

```bash
python script/retry_failed.py
```

## 技术细节

### 1. 流式迁移实现

- 大文件（>50MB）自动使用流式迁移
- 避免将整个文件加载到内存
- 分片读取并上传，降低内存占用

### 2. 连接优化

- 配置合理的连接超时时间
- 优化连接池大小，提高并发性能
- 自动处理连接异常和重试

### 3. 错误处理

- 详细的异常捕获和分类
- 针对不同错误类型的处理策略
- 完善的日志记录，便于故障排查

## 注意事项

1. **网络环境**：确保迁移服务器与华为云OBS、联通云OSS之间网络畅通
2. **权限配置**：确保配置的AK/SK具有足够的操作权限
3. **存储空间**：确保目标OSS桶有足够的存储空间
4. **性能调优**：根据服务器配置调整并发线程数和分片大小
5. **日志管理**：定期清理日志文件，避免占用过多磁盘空间

## 故障排除

### 1. 认证失败

**错误信息**：`权限不足：AccessDenied`

**解决方法**：
- 检查.env文件中的AK/SK是否正确
- 确保AK/SK具有足够的操作权限

### 2. 网络连接问题

**错误信息**：`网络请求错误：RequestError`

**解决方法**：
- 检查网络连接是否正常
- 调整connect_timeout参数
- 检查防火墙设置

### 3. 桶不存在

**错误信息**：`桶不存在：NoSuchBucket`

**解决方法**：
- 检查配置文件中的桶名是否正确
- 确保桶已在对应云服务中创建

### 4. 内存不足

**错误信息**：`MemoryError`

**解决方法**：
- 降低并发线程数
- 减小chunk_size参数
- 确保流式迁移阈值配置合理

## 性能优化建议

1. **并发线程数**：根据服务器CPU核数和网络带宽调整，建议值：20-100
2. **分片大小**：小文件多的场景可适当减小，大文件多的场景可适当增大
3. **流式迁移阈值**：根据服务器内存大小调整，建议值：50MB-1GB
4. **连接池大小**：建议与并发线程数保持一致或略大

## 版本更新日志

### v1.1.0
- 新增多桶并行迁移功能
- 支持同时迁移多个OBS-OSS桶对
- 桶级别的独立配置和过滤规则
- 增强的配置灵活性

### v1.0.0
- 初始版本发布
- 支持华为云OBS到联通云OSS的批量迁移
- 多线程并发处理
- 流式迁移和分片上传
- MD5校验和断点续传

## 许可证

MIT License

## 贡献

欢迎提交Issue和Pull Request！

## 联系方式

如有问题或建议，请通过以下方式联系：
- Email: your-email@example.com
- GitHub: <repository-url>
"# HwObs-LtOss" 
