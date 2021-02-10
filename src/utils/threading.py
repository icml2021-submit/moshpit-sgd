import os
from concurrent.futures import Future, ThreadPoolExecutor
from src.utils import get_logger

logger = get_logger(__name__)
EXECUTOR_PID, GLOBAL_EXECUTOR = None, None


def run_in_background(func: callable, *args, **kwargs) -> Future:
    global EXECUTOR_PID, GLOBAL_EXECUTOR
    if os.getpid() != EXECUTOR_PID:
        GLOBAL_EXECUTOR = ThreadPoolExecutor(max_workers=1000)
        EXECUTOR_PID = os.getpid()
    return GLOBAL_EXECUTOR.submit(func, *args, **kwargs)


def increase_file_limit(new_soft=2 ** 15, new_hard=2 ** 15):
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        new_soft = max(soft, new_soft)
        new_hard = max(hard, new_hard)
        logger.info(f"Increasing file limit: soft {soft}=>{new_soft}, hard {hard}=>{new_hard}")
        return resource.setrlimit(resource.RLIMIT_NOFILE, (new_soft, new_hard))
    except Exception as e:
        logger.warning(f"Failed to increase file limit: {e}")
