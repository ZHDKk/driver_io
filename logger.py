import logging
import os
from datetime import datetime
import re
from logging.handlers import TimedRotatingFileHandler

# driver logging init.
log = logging.getLogger('drv_io')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log.setLevel(logging.INFO)

# create console handler with a higher log level
consoler_handler = logging.StreamHandler()
consoler_handler.setFormatter(formatter)
consoler_handler.setLevel(logging.WARNING)
log.addHandler(consoler_handler)

# create a rotating file handler
# time_rotating_file_handler = TimedRotatingFileHandler(filename=f'./logs/drv', encoding='utf-8',
#                                                       when='midnight', interval=1, backupCount=30)
# time_rotating_file_handler.suffix = '%Y-%m-%d-%H-%M-%S.log'
# time_rotating_file_handler.extMatch = re.compile('^\\d{4}-\\d{2}-\\d{2}-\\d{2}-\\d{2}-\\d{2}(\\.\\w+)?$', re.ASCII)
# time_rotating_file_handler.setLevel(logging.INFO)
# time_rotating_file_handler.setFormatter(formatter)
# log.addHandler(time_rotating_file_handler)

# create file handler
# 确保日志目录存在
log_dir = './logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# 创建了一个时间滚动文件处理器（TimedRotatingFileHandler），用于将日志消息写入到文件中，并在指定的时间点滚动日志文件。这里设置为每天午夜（when='midnight'）滚动一次（interval=1
# ），并保留30个备份文件（backupCount=30）。
time_rotating_file_handler = TimedRotatingFileHandler(filename=f'{log_dir}/drv_io_', encoding='utf-8',
                                                      when='midnight', interval=1, backupCount=30)
time_rotating_file_handler.suffix = '%Y-%m-%d.log'
time_rotating_file_handler.extMatch = re.compile('^\\d{4}-\\d{2}-\\d{2}-\\d{2}-\\d{2}-\\d{2}(\\.\\w+)?$', re.ASCII)
time_rotating_file_handler.setLevel(logging.INFO)
time_rotating_file_handler.setFormatter(formatter)
log.addHandler(time_rotating_file_handler)

# driver logging end.
