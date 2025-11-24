import asyncio
import os
import sys
import time
from pathlib import Path

import pandas as pd
from distribution import distribution_server
from logger import log
from utils.time_util import get_current_time


async def opcua_reading_coroutine(dis: distribution_server):
    while True:
        time_start = time.time()
        await dis.opcua_device_read_task()
        time_using = time.time() - time_start
        # log.info(f'Task reading timing: {time_using:.4f}s')
        time_using = 0.01 if time_using > 0.8 else 0.81 - time_using
        await asyncio.sleep(time_using)

async def opcua_manager_coroutine(dis: distribution_server):
    while True:
        time_start = time.time()
        await dis.opcua_device_manage_task()
        time_using = time.time() - time_start
        # if time_using > 0.1:
        #     log.info(f'Task opcua manager timing: {time_using:.4f}s')
        time_using = 0.01 if time_using > 1.0 else 1.01 - time_using
        # restart_process(dis)
        await asyncio.sleep(time_using)

# 定时发布模组的连接状态
async def modules_connection_state_coroutine(dis: distribution_server):
    while True:
        time_start = time.time()
        await dis.modules_connection_state_task()
        time_using = time.time() - time_start
        # if time_using > 0.1:
        #     log.info(f'Task opcua manager timing: {time_using:.4f}s')
        time_using = 0.01 if time_using > 2.0 else 2.01 - time_using
        await asyncio.sleep(time_using)

async def request_coroutine(dis: distribution_server):
    while True:
        time_start = time.time()
        await dis.request_task()
        time_using = time.time() - time_start
        # if time_using > 0.1:
        #     log.info(f'Task request timing: {time_using:.4f}s')
        time_using = 0.01 if time_using > 0.5 else 0.51 - time_using
        await asyncio.sleep(time_using)


async def timed_clear_coroutine(dis: distribution_server):
    while True:
        time_start = time.time()
        await dis.timed_clear_task()
        time_using = time.time() - time_start
        # if time_using > 0.1:
        #     log.info(f'Task timed clear timing: {time_using:.4f}s')
        time_using = 0.01 if time_using > 0.2 else 0.21 - time_using
        await asyncio.sleep(time_using)


async def main():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    # 读取 config
    if getattr(sys, 'frozen', False):
        # 打包后exe所在目录
        base_dir = Path(sys.executable).parent
    else:
        base_dir = Path(__file__).parent

    config_dir = base_dir / "config files"
    config_dir.mkdir(exist_ok=True)

    # create distribution server
    async with distribution_server() as distribution:
        await distribution.initialize(config_dir)

        # multi coroutine
        reading_task = asyncio.create_task(opcua_reading_coroutine(distribution))
        manager_task = asyncio.create_task(opcua_manager_coroutine(distribution))
        if distribution.is_local:
            request_task = asyncio.create_task(request_coroutine(distribution))
        timed_clear_task = asyncio.create_task(timed_clear_coroutine(distribution))
        asyncio.create_task(modules_connection_state_coroutine(distribution))

        while True:
            await distribution.mqtt_handler()
            await asyncio.sleep(0.02)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("程序被用户中断，正在清理资源并退出...")
        log.info("程序被用户中断，正在清理资源并退出...")
        sys.exit(0)