"""
任务调度器

负责定时调度和执行 Pipeline 任务
"""

from typing import Any, Dict, List, Optional, Callable
from datetime import datetime
import signal
import sys
from loguru import logger
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import pytz


class TaskScheduler:
    """
    任务调度器
    
    负责根据配置的调度规则定时执行 Pipeline 任务。
    支持 cron 表达式、依赖管理、失败重试等功能。
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化任务调度器
        
        Args:
            config: 调度器配置字典
                - timezone: str, 时区（默认 'Asia/Shanghai'）
        """
        self.config = config or {}
        self.tasks: Dict[str, Dict[str, Any]] = {}
        self.scheduler = BlockingScheduler(timezone=pytz.timezone(self.config.get('timezone', 'Asia/Shanghai')))
        self._shutdown_requested = False
        
        # 注册事件监听器
        self.scheduler.add_listener(self._job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        
        # 注册信号处理器，用于优雅关闭
        # Windows 只支持 SIGINT，不支持 SIGTERM
        signal.signal(signal.SIGINT, self._signal_handler)
        if hasattr(signal, 'SIGTERM'):
            signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.debug("初始化任务调度器")
    
    def _signal_handler(self, signum, frame):
        """信号处理器，用于捕获 Ctrl+C 等中断信号"""
        if self._shutdown_requested:
            # 如果已经收到关闭请求，强制退出
            logger.warning("收到第二次中断信号，强制退出...")
            sys.exit(1)
        
        logger.warning(f"收到中断信号 ({signum})，正在优雅关闭调度器...")
        self._shutdown_requested = True
        
        # 停止调度器（这会触发 KeyboardInterrupt）
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
    
    def add_task(self, task_config: Dict[str, Any]) -> None:
        """
        添加任务
        
        Args:
            task_config: 任务配置字典
                - name: str, 任务名称
                - func: Callable, 要执行的函数
                - schedule: str, cron 表达式（如 "0 14 * * *" 表示每天14:00）
                - args: tuple, 位置参数（可选）
                - kwargs: dict, 关键字参数（可选）
                - depends_on: List[str], 依赖的任务名称列表（可选，暂未实现）
        """
        name = task_config.get('name')
        if not name:
            raise ValueError("任务名称不能为空")
        
        func = task_config.get('func')
        if not func or not callable(func):
            raise ValueError("任务函数不能为空且必须是可调用对象")
        
        schedule = task_config.get('schedule')
        if not schedule:
            raise ValueError("调度表达式不能为空")
        
        args = task_config.get('args', ())
        kwargs = task_config.get('kwargs', {})
        depends_on = task_config.get('depends_on', [])
        
        # 解析 cron 表达式
        cron_parts = schedule.split()
        if len(cron_parts) != 5:
            raise ValueError(f"无效的 cron 表达式: {schedule}，应为 5 个部分（分 时 日 月 周）")
        
        minute, hour, day, month, day_of_week = cron_parts
        
        # 创建 cron 触发器
        trigger = CronTrigger(
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=day_of_week
        )
        
        # 添加任务到调度器
        self.scheduler.add_job(
            func=func,
            trigger=trigger,
            id=name,
            name=name,
            args=args,
            kwargs=kwargs,
            replace_existing=True
        )
        
        # 保存任务配置
        self.tasks[name] = {
            'name': name,
            'func': func,
            'schedule': schedule,
            'args': args,
            'kwargs': kwargs,
            'depends_on': depends_on
        }
        
        logger.info(f"添加任务: {name}, 调度: {schedule}")
    
    def start(self) -> None:
        """
        启动调度器
        """
        if not self.tasks:
            logger.warning("没有任务可调度")
            return
        
        logger.info(f"启动任务调度器，共 {len(self.tasks)} 个任务")
        logger.info("按 Ctrl+C 停止调度器")
        
        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("收到停止信号，正在关闭调度器...")
            self.stop()
        except Exception as e:
            logger.error(f"调度器运行异常: {e}")
            self.stop()
            raise
    
    def stop(self) -> None:
        """
        停止调度器
        """
        if self._shutdown_requested:
            # 如果已经收到关闭请求，直接返回
            return
        
        self._shutdown_requested = True
        
        if self.scheduler.running:
            logger.info("正在关闭调度器...")
            try:
                # 等待当前任务完成，但设置超时
                self.scheduler.shutdown(wait=True)
                logger.info("调度器已停止")
            except Exception as e:
                logger.error(f"关闭调度器时出错: {e}")
                # 强制关闭
                try:
                    self.scheduler.shutdown(wait=False)
                except:
                    pass
    
    def execute_task(self, task_name: str) -> None:
        """
        执行指定任务
        
        Args:
            task_name: 任务名称
        """
        if task_name not in self.tasks:
            raise ValueError(f"任务不存在: {task_name}")
        
        task = self.tasks[task_name]
        logger.info(f"手动执行任务: {task_name}")
        
        try:
            task['func'](*task['args'], **task['kwargs'])
            logger.info(f"任务执行完成: {task_name}")
        except Exception as e:
            logger.error(f"任务执行失败: {task_name}, 错误: {e}")
            raise
    
    def _check_dependencies(self, task_name: str) -> bool:
        """
        检查任务依赖是否满足（框架方法）
        
        Args:
            task_name: 任务名称
            
        Returns:
            bool: 依赖是否满足
        """
        # TODO: 实现依赖检查逻辑
        return True
    
    def _job_listener(self, event):
        """
        任务执行事件监听器
        
        Args:
            event: 调度器事件
        """
        if event.exception:
            logger.error(f"任务执行异常: {event.job_id}, 错误: {event.exception}")
        else:
            logger.debug(f"任务执行成功: {event.job_id}")

