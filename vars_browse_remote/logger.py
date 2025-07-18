import logging
import os
import re
from logging.handlers import TimedRotatingFileHandler

log = logging.getLogger('drv_vbr')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log.setLevel(logging.INFO)

# 控制台日志配置
consoler_handler = logging.StreamHandler()
consoler_handler.setFormatter(formatter)
consoler_handler.setLevel(logging.WARNING)
log.addHandler(consoler_handler)

# 确保日志目录存在
log_dir = './logs'
os.makedirs(log_dir, exist_ok=True)  # 更安全的目录创建方式

# 创建时间滚动处理器 - 使用自定义文件名格式
time_rotating_file_handler = TimedRotatingFileHandler(
    filename=os.path.join(log_dir, 'drv_vbr.log'),  # 基础日志文件名
    encoding='utf-8',
    when='midnight',
    interval=1,
    backupCount=30
)

# 自定义后缀和匹配规则 - 实现 drv_io_xxxx-xx-xx.log 格式
time_rotating_file_handler.suffix = '%Y-%m-%d'  # 只包含日期部分
time_rotating_file_handler.namer = lambda name: name.replace('.log', '') + '.log'
time_rotating_file_handler.extMatch = re.compile(r'^\d{4}-\d{2}-\d{2}$', re.ASCII)  # 匹配日期格式

time_rotating_file_handler.setLevel(logging.INFO)
time_rotating_file_handler.setFormatter(formatter)
log.addHandler(time_rotating_file_handler)