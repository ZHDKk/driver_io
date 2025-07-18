import logging
import os
import re
from logging.handlers import TimedRotatingFileHandler


# 改进的命名函数
def custom_namer(default_name):
    dir_name, file_name = os.path.split(default_name)
    # 匹配默认格式：基础文件名.日期
    match = re.match(r'^(.*?)\.(\d{4}-\d{2}-\d{2})$', file_name)
    if match:
        base_part = match.group(1)  # "drv_io.log"
        date_part = match.group(2)  # "2025-07-18"

        # 提取主文件名（不含扩展名）
        main_name = os.path.splitext(base_part)[0]  # "drv_io"
        new_name = f"{main_name}_{date_part}.log"
        return os.path.join(dir_name, new_name)
    return default_name


# 自定义处理器（解决清理问题）
class CustomTimedRotatingFileHandler(TimedRotatingFileHandler):
    def getFilesToDelete(self):
        """
        重写此方法以正确识别自定义文件格式
        """
        # 获取目录中的所有文件
        dir_name, base_file = os.path.split(self.baseFilename)
        file_names = os.listdir(dir_name)

        # 构建匹配模式
        prefix = os.path.splitext(os.path.basename(self.baseFilename))[0]  # "drv_io"
        pattern = re.compile(rf"^{prefix}_(\d{{4}}-\d{{2}}-\d{{2}})\.log$")

        # 收集所有匹配的备份文件
        backups = []
        for file_name in file_names:
            mo = pattern.match(file_name)
            if mo:
                date_str = mo.group(1)
                # 将文件名转换为 (mtime, 文件名) 元组
                path = os.path.join(dir_name, file_name)
                backups.append((os.path.getmtime(path), path))

        # 按修改时间排序（最旧的在前面）
        backups.sort(key=lambda x: x[0])

        # 计算需要删除的数量
        if len(backups) <= self.backupCount:
            return []
        return [path for (_, path) in backups[:len(backups) - self.backupCount]]


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
os.makedirs(log_dir, exist_ok=True)

# 创建自定义时间滚动处理器
time_rotating_file_handler = CustomTimedRotatingFileHandler(
    filename=os.path.join(log_dir, 'drv_vbr.log'),
    encoding='utf-8',
    when='midnight',
    interval=1,
    backupCount=30
)

# 配置后缀和匹配规则
time_rotating_file_handler.suffix = '%Y-%m-%d'
time_rotating_file_handler.extMatch = re.compile(r'^\d{4}-\d{2}-\d{2}$', re.ASCII)
time_rotating_file_handler.namer = custom_namer

time_rotating_file_handler.setLevel(logging.INFO)
time_rotating_file_handler.setFormatter(formatter)
log.addHandler(time_rotating_file_handler)