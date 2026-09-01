"""
Claude-Queue Background Task Manager (clauq_btm)

IMPORTANT: For distributed execution (Celery), executors must be pre-registered
at application initialization time. The same module that registers executors
must be imported by both API servers and Celery workers.

Setup Option 1:
    clauq = ClauqBTM(redis_url='redis://localhost:6379/0')

    task_manager = clauq.setup(executors={
        'process_data': {'executor': process_fn, 'post_execution': callback_fn},
    })

    # For Celery workers (only after setup is complete)
    celery_app = clauq.celery_app

Setup Option 2:
    clauq = ClauqBTM(redis_url='redis://localhost:6379/0')

    clauq.register_executor("process_data", process_fn)

    task_manager = clauq.setup()


Setup Option 3:
    clauq = ClauqBTM(redis_url='redis://localhost:6379/0')

    @clauq.exectuor_registry.register_executor("process_data")
    def process_fn(task: ClauqBTMTask) -> Any:
        return "Hello, world!"

    task_manager = clauq.setup()
"""

from assistant_gateway.clauq_btm.events import TaskEvent, TaskEventType
from assistant_gateway.clauq_btm.executor_registry import (
    ExecutorConfig,
    ExecutorFunc,
    ExecutorRegistry,
    PostExecutionFunc,
)
from assistant_gateway.clauq_btm.instance import (
    BackgroundTasksUnavailableError,
    ClauqBTM,
    ClauqBTMConfig,
    ClauqBTMSetupError,
    SetupState,
)
from assistant_gateway.clauq_btm.queue_manager import (
    CeleryQueueManager,
    QueueInfo,
)
from assistant_gateway.clauq_btm.schemas import ClauqBTMTask, TaskStatus
from assistant_gateway.clauq_btm.task_manager import BTMTaskManager

__all__ = [
    "ClauqBTMTask",
    "TaskStatus",
    "TaskEvent",
    "TaskEventType",
    "BTMTaskManager",
    "ExecutorRegistry",
    "ExecutorConfig",
    "ExecutorFunc",
    "PostExecutionFunc",
    "QueueInfo",
    "CeleryQueueManager",
    "BackgroundTasksUnavailableError",
    "ClauqBTM",
    "ClauqBTMConfig",
    "ClauqBTMSetupError",
    "SetupState",
]
