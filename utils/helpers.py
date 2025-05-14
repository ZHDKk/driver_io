# utils/helpers.py
import json
import re
from decimal import Decimal, ROUND_HALF_UP

from logger import log

# 定义颜色映射
LOG_COLOR_MAP = {
    "INFO": "lightgreen",
    "WRITE": "white",
    "ERROR": "red",
    "WARNING": "orange",
    "DEFAULT": "lightblue"
}


def format_log_message(module_name, message):
    return f"[{module_name}] {message}"


def load_config_file(config_file_path):
    """
    load config file of distribution
    """
    # read config file
    config = {}
    try:
        with open(config_file_path, 'r', encoding='utf-8') as file:
            config = json.load(file)

    except FileNotFoundError:
        log.warning('Failure to find ./config files/driver config.json.')
        return
    except json.JSONDecodeError:
        log.warning('Failure to parse ./config files/driver config.json.')
        return
    except Exception as e:
        log.warning('Failure to read ./config files/driver config.json.')
        return
    return config


def key_to_module_names(config):
    module_names = []
    if config is not None:
        # 从 Opcua 字典中提取所有 key
        for key in config['Opcua']:
            module_names.append(key)

        for key in config:
            if key == 'Mqtt' or key == 'DB':
                module_names.append(key)
    return module_names


def get_log_color(message):
    """根据日志内容返回对应的颜色"""
    for keyword, color in LOG_COLOR_MAP.items():
        if keyword in message:
            return color
    return LOG_COLOR_MAP['DEFAULT']  # 默认颜色


def data_type_from_string(dtype):
    """
        定义一个函数来将 dataType 字符串转换为 Python 类型
    """
    dtype_map = {
        "unknown": None,
        "null": None,
        "Null": None,
        "bool": bool,
        "sbyte": int,
        "byte": int,
        "int16": int,
        "uint16": int,
        "int32": int,
        "uint32": int,
        "int64": int,
        "uint64": int,
        "float": float,
        "double": float,
        "string": str,
        "bytes": bytes,
        "datetime": int,
        "guid": str,
        "structure": dict,
    }
    return dtype_map.get(dtype, None)


def count_decimal_places(number):
    # 将数字转换为 Decimal
    num_dec = Decimal(f"{number:.16f}")

    # 去掉尾部的零
    num_dec = num_dec.quantize(Decimal('1'), rounding=ROUND_HALF_UP)

    # 去掉整数部分，获取小数部分
    fractional_part = num_dec - num_dec.to_integral_value()

    # 如果小数部分不为零，计算其长度
    if fractional_part != 0:
        return len(str(fractional_part).split('.')[1])
    else:
        return 0  # 没有小数部分


def round_half_up(value, precision):
    """
    实现浮点数高精度四舍五入
    :param value: 要四舍五入的数值，可以是浮点数或字符串形式的数值
    :param precision: 保留的小数位数
    :return: 四舍五入后的结果
    """
    decimal_value = Decimal(value)  # 转为 Decimal 类型
    rounded_value = decimal_value.quantize(Decimal(f'1.{"0" * precision}'), rounding=ROUND_HALF_UP)
    return float(rounded_value)  # 返回 float 类型结果


def generate_paths(data):
    paths = []
    for module in data:
        base_path = module['base_path']
        module_name = module['module_name']
        for sub_module in module['sub_modules']:
            sub_module_name = sub_module['sub_module']
            for sub_path in sub_module['sub_paths']:
                if sub_path['is_enable']:
                    full_path = f"{base_path}{sub_module_name}_{module_name}/{sub_path['name']}"
                    paths.append(full_path)
    return paths


def format2code_str(input_string):
    """
        把blockId_index_category_code格式的数据转成code
    """
    return "_".join(input_string.split("_")[-2:])


def code2format_str(blockId, index, category, code):
    """
        把code格式的数据转成blockId_index_category_code
    """
    return f'{blockId}_{index}_{category}_{code}'

def node_path2id(input_path: str) -> str:
    """
    把 OPC UA 的节点路径转成 NodeId 字符串：
      - 纯数字段作为上一个名称的索引：[数字]
      - 其余任何包含字母、下划线或数字的段，整体当名称，不做索引
    """
    parts = [p for p in input_path.split('/') if p]
    processed = []
    for part in parts:
        if part.isdigit() and processed and processed[-1][1] is None:
            name, _ = processed.pop()
            processed.append((name, part))
        else:
            processed.append((part, None))
    segments = []
    for name, idx in processed:
        if idx is not None:
            segments.append(f'"{name}"[{idx}]')
        else:
            segments.append(f'"{name}"')
    return f'ns=3;s={".".join(segments)}'

def is_target_format(name):
    """
    判断节点名称是否符合目标格式，例如 "2_10_xx"、"1_1_xx"、"B_2_10_xx"
    - 示例有效格式："2_10_xx"、"B_2_10_xx"、"B_1_1-xYz"
    """
    try:
        # 修改后的正则表达式：
        pattern = r"^(B_)?\d+_\d+_[A-Za-z0-9_-]+$"
        return re.match(pattern, name) is not None
    except Exception as e:
        # 如果需要记录警告可以取消注释下面这行
        log.warning(f"目标节点格式不符合规范：{e}")
        return False

def save_config_file(file_path, data):
    try:
        # 打开文件并保存数据，使用 'w' 模式覆盖原文件
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        return False