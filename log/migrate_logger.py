#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
迁移日志记录模块
负责记录迁移任务的详细信息和生成汇总报告
"""

import os
import json
import threading
from datetime import datetime
from collections import defaultdict
from log.logger import logger as base_logger
from config.config_loader import config_loader


class MigrateLogger:
    """迁移日志记录器类"""
    
    def __init__(self):
        """
        初始化迁移日志记录器
        """
        self.log_path = config_loader.get('log.path', './migrate_log/')
        self.today = datetime.now().strftime('%Y-%m-%d')
        
        # 确保日志目录存在
        os.makedirs(self.log_path, exist_ok=True)
        
        # 全局统计信息
        self.total_files = 0
        self.success_files = 0
        self.failed_files = 0
        self.failed_list = []
        
        # 按桶统计信息
        self.bucket_stats = defaultdict(lambda: {
            'total': 0,
            'success': 0,
            'failed': 0
        })
        
        # 线程锁
        self.lock = threading.Lock()
    
    def log_file_migrate(self, obs_path, oss_path, file_size, duration, status, error_msg="", obs_bucket=None):
        """
        记录单个文件的迁移信息
        
        Args:
            obs_path (str): 华为云OBS文件路径
            oss_path (str): 联通云OSS文件路径
            file_size (int): 文件大小（字节）
            duration (float): 迁移耗时（秒）
            status (str): 迁移状态（success/failed）
            error_msg (str): 错误信息（如果失败）
            obs_bucket (str): OBS桶名称
        """
        # 提取桶名称
        if not obs_bucket:
            # 从路径中提取桶名称（假设格式为 bucket_name/object_path）
            obs_bucket = obs_path.split('/')[0] if obs_path else "unknown"
        
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "obs_bucket": obs_bucket,
            "obs_path": obs_path,
            "oss_path": oss_path,
            "file_size": file_size,
            "duration": duration,
            "status": status,
            "error_msg": error_msg
        }
        
        # 写入按小时分割的日志文件
        hour = datetime.now().strftime('%H')
        log_file = os.path.join(self.log_path, f'migrate_{self.today}_{hour}.jsonl')
        
        with self.lock:
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
            
            # 更新全局统计信息
            if status == "success":
                self.success_files += 1
            else:
                self.failed_files += 1
                self.failed_list.append({
                    "obs_path": obs_path,
                    "error_msg": error_msg
                })
            
            # 更新按桶统计信息
            self.bucket_stats[obs_bucket]['total'] += 1
            if status == "success":
                self.bucket_stats[obs_bucket]['success'] += 1
            else:
                self.bucket_stats[obs_bucket]['failed'] += 1
    
    def update_total_files(self, total, obs_bucket=None):
        """
        更新总文件数
        
        Args:
            total (int): 总文件数
            obs_bucket (str): OBS桶名称（可选）
        """
        with self.lock:
            self.total_files = total
            
            # 如果提供了桶名称，更新对应桶的总文件数
            if obs_bucket:
                self.bucket_stats[obs_bucket]['total'] = total
    
    def get_progress(self):
        """
        获取迁移进度
        
        Returns:
            dict: 包含总文件数、已完成文件数和百分比的字典
        """
        with self.lock:
            completed = self.success_files + self.failed_files
            percentage = (completed / self.total_files * 100) if self.total_files > 0 else 0
            return {
                "total": self.total_files,
                "completed": completed,
                "percentage": round(percentage, 2),
                "success": self.success_files,
                "failed": self.failed_files
            }
    
    def generate_daily_report(self):
        """
        生成每日汇总报告
        """
        # 转换bucket_stats为普通字典，以便JSON序列化
        bucket_stats_dict = dict(self.bucket_stats)
        
        report = {
            "date": self.today,
            "total_files": self.total_files,
            "success_files": self.success_files,
            "failed_files": self.failed_files,
            "failed_list": self.failed_list,
            "bucket_stats": bucket_stats_dict,
            "generated_at": datetime.now().isoformat()
        }
        
        report_file = os.path.join(self.log_path, f'report_{self.today}.json')
        
        with self.lock:
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
        
        # 生成失败文件清单（用于重试）
        if self.failed_files > 0:
            failed_file = os.path.join(self.log_path, f'failed_{self.today}.txt')
            with open(failed_file, 'w', encoding='utf-8') as f:
                for item in self.failed_list:
                    f.write(f"{item['obs_path']}\t{item['error_msg']}\n")
        
        return report
    
    def load_failed_list(self, date=None):
        """
        加载指定日期的失败文件清单
        
        Args:
            date (str): 日期，格式为YYYY-MM-DD
            
        Returns:
            list: 失败文件列表
        """
        if not date:
            date = self.today
        
        failed_file = os.path.join(self.log_path, f'failed_{date}.txt')
        failed_list = []
        
        if os.path.exists(failed_file):
            with open(failed_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        obs_path, error_msg = line.strip().split('\t', 1)
                        failed_list.append({
                            "obs_path": obs_path,
                            "error_msg": error_msg
                        })
        
        return failed_list


# 单例模式
migrate_logger = MigrateLogger()