# utils/helpers.py
import json
import random
import re
import string
import uuid
from decimal import Decimal, ROUND_HALF_UP

from asyncua import ua

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
        log.warning(f'Failure to find {config_file_path}')
        return
    except json.JSONDecodeError:
        log.warning(f'Failure to parse {config_file_path}')
        return
    except Exception as e:
        log.warning(f'Failure to read {config_file_path}:{e}')
        return
    return config


def upsert_config_file(config_file_path, data):
    """
    upsert config file of distribution
    """
    # read config file
    config = {}
    try:
        with open(config_file_path, 'w') as f:
            json.dump(data, f, indent=4)

    except FileNotFoundError:
        log.warning(f'Failure to find {config_file_path}')
        return
    except json.JSONDecodeError:
        log.warning(f'Failure to parse {config_file_path}')
        return
    except Exception as e:
        log.warning(f'Failure to read {config_file_path}:{e}')
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


def generate_random_string(length=8):
    """
    生成一个8位的字母数字组合随机数
    """
    # 定义可以使用的字符集：字母和数字
    characters = string.ascii_letters + string.digits
    # 从字符集中随机选择字符并生成字符串
    random_string = ''.join(random.choice(characters) for _ in range(length))
    return random_string


def check_string_regex(s):
    """
    检测字符串中只包含字母、数字和_，没有特殊字符正则
    """
    pattern = r'^[a-zA-Z0-9_]+$'
    return bool(re.match(pattern, s))


def datatype_str2_num(datatype_str):
    # 数据类型字符串到枚举值的映射
    type_mapping = {
        'null': ua.VariantType.Null,
        'bool': ua.VariantType.Boolean,
        'sbyte': ua.VariantType.SByte,
        'byte': ua.VariantType.Byte,
        'int16': ua.VariantType.Int16,
        'uint16': ua.VariantType.UInt16,
        'int32': ua.VariantType.Int32,
        'uint32': ua.VariantType.UInt32,
        'int64': ua.VariantType.Int64,
        'uint64': ua.VariantType.UInt64,
        'float': ua.VariantType.Float,
        'double': ua.VariantType.Double,
        'string': ua.VariantType.String,
        'datetime': ua.VariantType.DateTime,
        'guid': ua.VariantType.Guid,
        'bytes': ua.VariantType.ByteString,
        'structure': ua.VariantType.ExtensionObject
    }
    return type_mapping.get(datatype_str, None)

def name_2path(path, name):
    """
    add name to path
    """
    str_tmp = path + '/' + name
    return str_tmp

def json_from_list(datas: dict):
    """
    pack json frame, input dict format: {'module':{}, 'list':[{},{}...]}
    """
    try:
        module = datas['module']
        datas.update(module)
        datas.pop('module')

        result = {"id": str(uuid.uuid4()), "ask": False, "data": datas}
        result = json.dumps(result)
        # pprint.pprint(res_json)
        return result
    except:
        log.warning(f'Failure to pack json frame {datas}.')
        return {}

def save_config_file(file_path, data):
    try:
        # 打开文件并保存数据，使用 'w' 模式覆盖原文件
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        return False
