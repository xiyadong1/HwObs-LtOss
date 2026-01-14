#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试迁移工具的并行执行功能
"""

import sys
import os
import time
import threading
import queue
import random

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from log.logger import logger


def simulate_migrate_task(task_id, sleep_time):
    """
    模拟迁移任务，通过sleep时间来模拟任务执行时间
    
    Args:
        task_id (int): 任务ID
        sleep_time (float): 模拟任务执行时间（秒）
    """
    start_time = time.time()
    logger.info(f"任务 {task_id} 开始执行，预计耗时 {sleep_time:.2f} 秒")
    
    # 模拟任务执行
    time.sleep(sleep_time)
    
    end_time = time.time()
    actual_time = end_time - start_time
    logger.info(f"任务 {task_id} 执行完成，实际耗时 {actual_time:.2f} 秒")
    
    return task_id, actual_time


def worker(queue, results, exit_flag):
    """
    工作线程函数
    
    Args:
        queue (Queue): 任务队列
        results (list): 结果列表
        exit_flag (threading.Event): 退出标志
    """
    while not exit_flag.is_set():
        try:
            # 从队列获取任务
            task_info = queue.get(timeout=1)
            
            if task_info is None:
                # 收到终止信号
                break
            
            task_id = task_info['task_id']
            sleep_time = task_info['sleep_time']
            
            # 执行任务
            result = simulate_migrate_task(task_id, sleep_time)
            
            # 保存结果
            with threading.Lock():
                results.append(result)
            
            # 标记任务完成
            queue.task_done()
            
        except queue.Empty:
            # 队列为空，继续检查退出标志
            continue
        except Exception as e:
            logger.error(f"工作线程异常：{str(e)}")
            queue.task_done()


def test_parallel_execution(thread_count=50, task_count=100):
    """
    测试并行执行功能
    
    Args:
        thread_count (int): 线程数量
        task_count (int): 任务数量
    """
    logger.info(f"开始测试并行执行功能：线程数={thread_count}，任务数={task_count}")
    
    # 创建任务队列
    task_queue = queue.Queue()
    
    # 创建结果列表
    results = []
    
    # 创建退出标志
    exit_flag = threading.Event()
    
    # 启动工作线程
    threads = []
    for _ in range(thread_count):
        thread = threading.Thread(target=worker, args=(task_queue, results, exit_flag))
        thread.daemon = True
        thread.start()
        threads.append(thread)
    
    # 生成测试任务
    total_expected_time = 0
    for i in range(task_count):
        # 随机生成任务执行时间（0.1-2秒）
        sleep_time = random.uniform(0.1, 2.0)
        total_expected_time += sleep_time
        
        task_info = {
            'task_id': i + 1,
            'sleep_time': sleep_time
        }
        
        task_queue.put(task_info)
    
    logger.info(f"所有任务已添加到队列，总预期耗时：{total_expected_time:.2f} 秒")
    logger.info(f"如果完全并行执行，预计耗时：{max(task['sleep_time'] for task in task_queue.queue):.2f} 秒")
    
    # 记录开始时间
    start_time = time.time()
    
    # 等待所有任务完成
    logger.info("等待所有任务完成...")
    task_queue.join()
    
    # 发送终止信号
    exit_flag.set()
    
    # 等待所有线程退出
    for thread in threads:
        thread.join(timeout=5)
    
    # 记录结束时间
    end_time = time.time()
    total_actual_time = end_time - start_time
    
    logger.info(f"\n测试完成！")
    logger.info(f"总任务数：{task_count}")
    logger.info(f"总预期耗时：{total_expected_time:.2f} 秒")
    logger.info(f"实际执行时间：{total_actual_time:.2f} 秒")
    logger.info(f"并行效率：{(total_expected_time / total_actual_time):.2f}x")
    
    # 检查是否所有任务都完成
    if len(results) == task_count:
        logger.info("所有任务都已成功完成！")
    else:
        logger.error(f"部分任务未完成：已完成 {len(results)}/{task_count}")
    
    return {
        'task_count': task_count,
        'thread_count': thread_count,
        'total_expected_time': total_expected_time,
        'total_actual_time': total_actual_time,
        'parallel_efficiency': total_expected_time / total_actual_time,
        'completed_tasks': len(results)
    }


if __name__ == "__main__":
    # 测试不同线程数量的效果
    for thread_count in [1, 10, 20, 50]:
        logger.info(f"\n{'='*60}")
        logger.info(f"测试线程数：{thread_count}")
        logger.info('='*60)
        
        result = test_parallel_execution(thread_count=thread_count, task_count=100)
        
        logger.info(f"\n测试结果：")
        logger.info(f"线程数：{result['thread_count']}")
        logger.info(f"任务数：{result['task_count']}")
        logger.info(f"总预期耗时：{result['total_expected_time']:.2f} 秒")
        logger.info(f"实际执行时间：{result['total_actual_time']:.2f} 秒")
        logger.info(f"并行效率：{result['parallel_efficiency']:.2f}x")
        logger.info(f"已完成任务：{result['completed_tasks']}")
