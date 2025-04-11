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


def node_path2id(input_path):
    """
         把NodePath转成解析NodeId
    """
    # 去掉开头的斜杠并分割字符串
    parts = input_path.lstrip('/').split('/')
    # 为每个部分添加引号
    quoted_parts = [f'"{part}"' for part in parts]
    # 用点号连接各部分
    joined_parts = '.'.join(quoted_parts)
    # 组合成最终结果
    return f'ns=3;s={joined_parts}'

def node_path2id2(input_str):
    parts = [p for p in input_str.split('/') if p]
    processed = []

    for part in parts:
        # 如果是数字且前一个元素可追加索引
        if part.isdigit() and processed and processed[-1][1] is None:
            prev_name, _ = processed.pop()
            processed.append((prev_name, part))
        else:
            processed.append((part, None))

    # 构建带索引的路径字符串
    path = []
    for name, index in processed:
        if index is not None:
            path.append(f'"{name}"[{index}]')
        else:
            path.append(f'"{name}"')

    return f'ns=3;s={".".join(path)}'

def node_path2id3(input_str):
    """
         把NodePath转成解析NodeId
    """
    # 分割路径并过滤空字符串
    parts = input_str.split('/')
    parts = [p for p in parts if p]

    if not parts:
        return 'ns=3;s=""'  # 处理空路径的情况

    # 检查最后一个元素是否为数字
    index = None
    if parts[-1].isdigit():
        index = parts[-1]
        path_parts = parts[:-1]
    else:
        path_parts = parts

    # 用双引号包裹每个部分并用点连接
    quoted_parts = [f'"{part}"' for part in path_parts]
    result = '.'.join(quoted_parts)

    # 添加索引（如果存在）
    if index is not None:
        result += f'[{index}]'

    return f'ns=3;s={result}'

def node_path2id4(input_str):
    parts = [p for p in input_str.split('/') if p]
    processed = []

    for part in parts:
        if part.isdigit() and processed and processed[-1][1] is None:
            prev_name, _ = processed.pop()
            processed.append((prev_name, part))
        else:
            processed.append((part, None))

    path = []
    for name, index in processed:
        if index is not None:
            path.append(f'"{name}"[{index}]')
        else:
            path.append(f'"{name}"')

    return f'ns=3;s={".".join(path)}'

def is_target_format(name):
    """
        判断节点名称是否符合目标格式，例如 "2_10_xx"、"1_1_xx"、"2_15_xx"，
        最后只支持大小写字母和数字的组合
    """
    try:
        pattern = r"^\d+_\d+_[A-Za-z0-9_-]+$"
        return re.match(pattern, name)
    except Exception as e:
        log.warning(f"目标节点格式不符合1_1_xxx：{e}")
        return None

def save_config_file(file_path, data):
    try:
        # 打开文件并保存数据，使用 'w' 模式覆盖原文件
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        return False