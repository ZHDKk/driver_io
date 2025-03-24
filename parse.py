import json
import pprint
import uuid

import snap7.util
from bigtree import print_tree, find_attr, find_path, tree_to_nested_dict, tree_to_dict, find_children
from logger import log
from asyncua import ua
from snap7.types import *
from snap7.util import *
from bigtree import Node as BTNode

from utils.helpers import data_type_from_string, round_half_up, code2format_str


def bytes_2_ua_data(datas: bytearray, byte_index: int, bit_index: int, var_type: ua.VariantType):
    """
    convert bytes to opcua data with type
    """
    match var_type:
        case ua.VariantType.Null:
            return None
        case ua.VariantType.Boolean:
            return snap7.util.get_bool(datas, byte_index, bit_index)
        case ua.VariantType.SByte:
            return snap7.util.get_sint(datas, byte_index)
        case ua.VariantType.Byte:
            return snap7.util.get_byte(datas, byte_index)
        case ua.VariantType.Int16:
            return snap7.util.get_int(datas, byte_index)
        case ua.VariantType.UInt16:
            return snap7.util.get_uint(datas, byte_index)
        case ua.VariantType.Int32:
            return snap7.util.get_dint(datas, byte_index)
        case ua.VariantType.UInt32:
            return snap7.util.get_udint(datas, byte_index)
        case ua.VariantType.Int64:
            return None
        case ua.VariantType.UInt64:
            return snap7.util.get_dword(datas, byte_index)
        case ua.VariantType.Float:
            return snap7.util.get_real(datas, byte_index)
        case ua.VariantType.Double:
            return snap7.util.get_lreal(datas, byte_index)
        case ua.VariantType.String:
            return snap7.util.get_string(datas, byte_index)
        case ua.VariantType.ByteString:
            return None
        case ua.VariantType.DateTime:
            return snap7.util.get_dt(datas, byte_index)
        case ua.VariantType.Guid:
            return None
        case ua.VariantType.ExtensionObject:
            return None


def nested_dict_2list(nested_dict: dict, res: list, time_ms, parent_key=None, sep='_'):
    """
    convert nested dict to list, 'code' include tree path name
    """
    for key, value in nested_dict.items():
        if isinstance(value, dict):  # value is dict
            nested_dict_2list(value, res, time_ms, parent_key=key if parent_key is None else parent_key + sep + key,
                              sep=sep)
        else:  # value is single variable
            data_type = type(value).__name__
            if data_type == 'str':
                data_type = 'string'
            res.append({'code': key if parent_key is None else parent_key + sep + key, 'value': value,
                        'dataType': data_type, 'arrLen': 0, 'time': time_ms})


def tree_to_list(node, t2l, rtime):
    """
    convert tree to list
    """
    # 2024/10/23 新增节点不是children时处理
    if node.children:
        for child in node.children:
            if child.children:  # child's data type is array, recursion
                tree_to_list(child, t2l, rtime)
            else:  # child's data type is single variable, add to list
                t2l.append({"code": child.code, "value": child.value, "dataType": child.DataTypeString,
                            "arrLen": child.ArrayDimensions, "time": rtime})
    else:
        t2l.append({"code": node.code, "value": node.value, "dataType": node.DataTypeString,
                    "arrLen": node.ArrayDimensions, "time": rtime})


def data_to_list(node, t2l, rtime, dev):
    """
    convert data to list
    """
    value_dict = node.get('value')
    if value_dict:
        leaf_keys = extract_leaf_keys_with_path(value_dict)
        for key in leaf_keys:
            if key.startswith('_'):
                child = dev.code_to_node.get(
                    code2format_str(node['blockId'], node['index'], node['category'],
                                    node['code']) + '_' + key[1:])
                t2l.append({"code": child["code"], "value": child["value"], "dataType": child["DataTypeString"],
                            "arrLen": child["ArrayDimensions"], "time": rtime})
            else:
                child = dev.code_to_node.get(
                    code2format_str(node['blockId'], node['index'], node['category'], node['code']) + '_' + key)
                t2l.append({"code": child["code"], "value": child["value"], "dataType": child["DataTypeString"],
                            "arrLen": child["ArrayDimensions"], "time": rtime})


def extract_leaf_keys_with_path(dictionary, current_path=""):
    result = []
    if isinstance(dictionary, dict):
        for key, value in dictionary.items():
            new_path = f"{current_path}_{key}" if current_path else key
            if isinstance(value, (dict, list)):
                result.extend(extract_leaf_keys_with_path(value, new_path))
            else:
                result.append(new_path)
    elif isinstance(dictionary, list):
        for index, item in enumerate(dictionary):
            new_path = f"{current_path}_{index}" if current_path else str(index)
            if isinstance(item, (dict, list)):
                result.extend(extract_leaf_keys_with_path(item, new_path))
            else:
                result.append(new_path)
    return result


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


def json_from_tree(node, current_time):
    # print(node)
    # tree to directory
    t2d = tree_to_dict(node, attr_dict={"code": "code", "value": "value", "DataTypeString": "dataType",
                                        "ArrayDimensions": "arrLen"})
    # filter single variable and add to list
    d2l = []
    for n in t2d:
        m = t2d[n]
        if m['dataType'] == 'Null' or m['dataType'] == 'structure' and m['arrLen'] != 0:
            continue
        if type(m['value']) == list or type(m['value']) == dict:
            continue

        del m['name']
        m['time'] = current_time
        d2l.append(m)
    # pprint.pprint(d2l)

    # create self-define frame and convert to json
    result = {"id": str(uuid.uuid4()), "ask": False, 'success': True, 'message': 'OK',
              "data": {"blockId": node.blockId, "index": node.index, "category": node.category, "list": d2l}}

    result = json.dumps(result)
    # pprint.pprint(res_json)
    return result


def json_msg_pack(blockId, index, category, code, cmd):
    # create self-define frame and convert to json
    result = {"id": str(uuid.uuid4()), "ask": False,
              "msg": {"blockId": blockId, "index": index, "category": category,
                      "list": [{"code": code, "cmd": cmd, "time": int(time.time() * 1000)}]}}
    # pprint.pprint(result)
    res_json = json.dumps(result)
    # pprint.pprint(res_json)
    return res_json


def json_from_nested_dict(dict_datas: dict):
    # create self-define frame and convert to json
    try:
        list_data = []
        nested_dict_2list(dict_datas, list_data, int(time.time() * 1000))
        # pprint.pprint(list_data)

        result = {"id": str(uuid.uuid4()), "ask": False,
                  "data": {"blockId": 100, "index": 100, "category": "Driver", 'list': list_data}}
        result = json.dumps(result)
        pprint.pprint(result)
        return result
    except:
        print('Failure to pack json frame.')
        return None


def array_parse(dev, node, list_node, value, M2O, M2O_list, O2M, O2M_list, rtime, msg: list):
    """
    parse array structure of tree, update node[n].value with value[n], add to sending buffer M2O_list or O2M_list.
    """
    # print("array source:", node.ArrayDimensions, type(value), value)
    if type(value) is not list:
        msg.append(f'Failure to {dev.name}{node.NodePath}[{node.ArrayDimensions}] is array, '
                   f'but value type is {type(value)}.')
        return value

    if len(value) != node.ArrayDimensions:
        msg.append(f'Failure to match array length, {node.ArrayDimensions}!={len(value)}.')
        return value

    for n in range(node.ArrayDimensions):
        # find array[n] node
        try:
            child = find_attr(node, "NodePath", node.NodePath + '/' + str(n))  # array element
        except:
            msg.append(f'Failure to find {dev.name}{node.NodePath}/{n} in variable list.')
            continue

        # verification node, value and datatype
        if child is None:
            msg.append(f'Failure to find {dev.name}{node.NodePath}/{n} in variable list.')
            continue

        if value[n] is None:
            msg.append(f'Failure to find {dev.name}{node.NodePath}/{n} = {value[n]}, Null value.')
            continue

        value_type = type(value[n])
        if type(child.DataType) is str:
            child.DataType = int(child.DataType)
        child_type = ua.VariantType(child.DataType)

        # value_type_name = value_type.name
        # if value_type_name == 'str':
        #     value_type_name = 'string'
        # if value_type_name != child.DataTypeString:
        #     msg.append(f'Failure to match {dev.name}{node.NodePath}/{n} data type,'
        #                f'{value_type_name},{child.DataTypeString}')
        #     continue

        if child.ArrayDimensions > 0:  # and value_type is list, child's data type is array, recursion
            value[n] = array_parse(dev, child, list_node, value[n],
                                   M2O, M2O_list, O2M, O2M_list, rtime, msg)
        elif child_type in [ua.VariantType.ExtensionObject]:  # child's data type is structure, recursion
            value[n] = struct_parse(dev, child, list_node,
                                    value[n] if type(value[n]) is dict else value[n].__dict__,
                                    M2O, M2O_list, O2M, O2M_list, rtime, msg)
        elif O2M_list is not None and (O2M is True or child.value != value[n]):  # opcua2mqtt
            #  2024/11/22  增加高精度浮点运算
            if child.DataTypeString == "float" or child.DataTypeString == "double":
                precision = child.DecimalPoint
                # print(precision)
                value[n] = round_half_up(value[n], precision)
                # if o2m_value != 0.0:
                #     print(f"{child.NodePath}:{o2m_value}")
            O2M_list.append({"code": child.code, "value": value[n], "dataType": child.DataTypeString,
                             "arrLen": child.ArrayDimensions, "time": rtime})  # child's data type is single variable
        elif M2O_list is not None and (M2O is True or child.value != value[n]):  # mqtt2opcua
            if dev.link_type == 'opcua':
                value_type = type(value[n])
                original_type = data_type_from_string(child.DataTypeString)
                if (isinstance(value[n], int) and isinstance(original_type, type) and original_type is float) or \
                        (isinstance(value[n], int) and hasattr(original_type,
                                                                 '__name__') and original_type.__name__ == 'float'):
                    pass
                elif original_type != value_type:
                    msg.append(
                        f'Write Data Type Error, Please check: ({child.NodeID}, datetype:{child.DataTypeString}, value:{value[n]}), '
                        f'the value should be of type {original_type.__name__}, not {value_type.__name__}')
                    return
                M2O_list.append({'node_id': child.NodeID, 'datatype': child.DataType, 'value': value[n]})
            elif dev.link_type == 's7':
                M2O_list.append({'s7_db': child.s7_db, 's7_start': child.s7_start, 's7_bit': child.s7_bit,
                                 's7_size': child.s7_size, 'value': value[n]})
        child.value = value[n]  # update to node
    return value


def struct_parse(dev, node, list_node, value: dict, M2O, M2O_list, O2M, O2M_list, rtime, msg: list):
    """
    parse structure of tree, update node[key].value with value[key], add to sending buffer M2O_list or O2M_list.
    """
    # print("structure source:", type(value), value, M2O, O2M)
    if type(value) is not dict:
        msg.append(f'Failure to {dev.name}{node.NodePath} is structure, but value type is {type(value)}.')
        return value

    for key in value:
        try:
            if key.startswith('_'):
                child = find_attr(node, "NodePath", node.NodePath + '/' + key[1:])  # structure element
            else:
                child = find_attr(node, "NodePath", node.NodePath + '/' + key)  # structure element
        except:
            msg.append(f'Failure to find {dev.name}.{node.NodePath}/{key} in variable list.')
            continue

        # verification node, value and datatype
        if child is None:
            msg.append(f'Failure to find {dev.name}{node.NodePath}/{key} in variable list.')
            continue

        if value[key] is None:
            msg.append(f'{dev.name}{node.NodePath}/{key} Structure is not readable = {value[key]}, Null value.')
            continue

        value_type = type(value[key])
        if type(child.DataType) is str:
            child.DataType = int(child.DataType)
        child_type = ua.VariantType(child.DataType)

        # value_type_name = value_type.name
        # if value_type_name == 'str':
        #     value_type.name = 'string'
        # if value_type_name != child.DataTypeString:
        #     print(f'Failure to match {dev.name}{node.NodePath}/{str(int(0) + n)} data type,'
        #           f'{value_type_name},{child.DataTypeString}')
        #     continue

        if child.ArrayDimensions > 0:  # and value_type is list, child's data type is array, recursion
            value[key] = array_parse(dev, child, list_node, value[key],
                                     M2O, M2O_list, O2M, O2M_list, rtime, msg)
        elif child_type in [ua.VariantType.ExtensionObject]:  # child's data type is structure, recursion
            value[key] = struct_parse(dev, child, list_node,
                                      value[key] if type(value[key]) is dict else value[key].__dict__,
                                      M2O, M2O_list, O2M, O2M_list, rtime, msg)
        elif O2M_list is not None and (O2M is True or child.value != value[key]):  # opcua2mqtt
            #  2024/11/22  增加高精度浮点运算
            if child.DataTypeString == "float" or child.DataTypeString == "double":
                precision = child.DecimalPoint
                value[key] = round_half_up( value[key], precision)
                # if o2m_value != 0.0:
                #     print(f"{child.NodePath}:{o2m_value}")
            O2M_list.append({"code": child.code, "value":  value[key], "dataType": child.DataTypeString,
                             "arrLen": child.ArrayDimensions, "time": rtime})
        elif M2O_list is not None and (M2O is True or child.value != value[key]):  # mqtt2opcua
            if dev.link_type == 'opcua':
                value_type = type(value[key])
                original_type = data_type_from_string(child.DataTypeString)
                if (isinstance(value[key], int) and isinstance(original_type, type) and original_type is float) or \
                        (isinstance(value[key], int) and hasattr(original_type,
                                                            '__name__') and original_type.__name__ == 'float'):
                    pass
                elif original_type != value_type:
                    msg.append(f'Write Data Type Error, Please check: ({child.NodeID}, datetype:{child.DataTypeString}, value:{value[key]}), '
                               f'the value should be of type {original_type.__name__}, not {value_type.__name__}')
                    return
                M2O_list.append({'node_id': child.NodeID, 'datatype': child.DataType, 'value': value[key]})
            elif dev.link_type == 's7':
                M2O_list.append({'s7_db': child.s7_db, 's7_start': child.s7_start, 's7_bit': child.s7_bit,
                                 's7_size': child.s7_size, 'value': value[key]})
        child.value = value[key]
    return value


def datas_parse(dev, node, list_node, value, M2O, M2O_list, O2M, O2M_list, rtime, msg):
    """
    recursive parse structure data
    """
    if value is None:
        return
        # msg.append(f'{dev.name}{node.NodePath} Structure is not readable = {value}, Null value.')
    else:
        # node datatype and value type, recursive parse structure data
        value_type = type(value)
        if value_type == 'str':
            value_type = 'string'
        if type(node.DataType) is str:
            node.DataType = int(node.DataType)
        node_type = ua.VariantType(node.DataType)

        if node.ArrayDimensions > 0:  # and value_type is list, data type is array
            value = array_parse(dev, node, list_node, value,
                                M2O, M2O_list, O2M, O2M_list, rtime, msg)
        elif node_type in [ua.VariantType.ExtensionObject]:  # data type of node is structure
            value = struct_parse(dev, node, list_node, value if value_type is dict else value.__dict__,
                                 M2O, M2O_list, O2M, O2M_list, rtime, msg)
        elif O2M_list is not None and (O2M is True or node.value != value):  # opcua2mqtt
            if node.DataTypeString == "float" or node.DataTypeString == "double":
                precision = node.DecimalPoint
                # print(precision)
                value = round_half_up(value, precision)
            O2M_list.append({"code": node.code, "value": value, "dataType": node.DataTypeString,
                             "arrLen": node.ArrayDimensions, "time": rtime})
        elif M2O_list is not None and (M2O is True or node.value != value):  # mqtt2opcua
            if dev.link_type == 'opcua':
                value_type = type(value)
                original_type = data_type_from_string(node.DataTypeString)
                if (isinstance(value, int) and isinstance(original_type, type) and original_type is float) or \
                        (isinstance(value, int) and hasattr(original_type,
                                                                 '__name__') and original_type.__name__ == 'float'):
                    pass
                elif original_type != value_type:
                    msg.append(
                        f'Write Data Type Error, Please check: ({node.NodeID}, datetype:{node.DataTypeString}, value:{value}), '
                        f'the value should be of type {original_type.__name__}, not {value_type.__name__}')
                    return
                M2O_list.append({'node_id': node.NodeID, 'datatype': node.DataType, 'value': value})
            elif dev.link_type == 's7':
                M2O_list.append({'s7_db': node.s7_db, 's7_start': node.s7_start, 's7_bit': node.s7_bit,
                                 's7_size': node.s7_size, 'datatype': node.DataTypeString, 'value': value})

        # update variable value to node and list
        node.value = value
        list_node['value'] = value


def array_parse_o2m(dev, list_node, value, O2M, O2M_list, rtime, msg: list):
    """
    parse array structure of tree, update node[n].value with value[n], add to sending buffer M2O_list or O2M_list.
    """
    # print("array source:", node.ArrayDimensions, type(value), value)
    if type(value) is not list:
        msg.append(f'Failure to {dev.name}{list_node["NodePath"]}[{list_node["ArrayDimensions"]}] is array, '
                   f'but value type is {type(value)}.')
        return value

    if len(value) != list_node["ArrayDimensions"]:
        msg.append(f'Failure to match array length, {list_node["ArrayDimensions"]}!={len(value)}.')
        return value

    for n in range(list_node["ArrayDimensions"]):
        # find array[n] node
        try:
            list_child = dev.code_to_node.get(code2format_str(list_node['blockId'], list_node['index'], list_node['category'], list_node['code']) + '_' + str(n))
        except:
            msg.append(f'Failure to find {dev.name}{list_node["NodePath"]}/{n} in variable list.')
            continue

        # verification node, value and datatype
        if list_child is None:
            msg.append(f'Failure to find {dev.name}{list_node["NodePath"]}/{n} in variable list.')
            continue

        if value[n] is None:
            msg.append(f'Failure to find {dev.name}{list_node["NodePath"]}/{n} = {value[n]}, Null value.')
            continue

        value_type = type(value[n])
        if type(list_child["DataType"]) is str:
            list_child["DataType"] = int(list_child["DataType"])
        child_type = ua.VariantType(list_child["DataType"])

        if list_child["ArrayDimensions"] > 0:  # and value_type is list, child's data type is array, recursion
            value[n] = array_parse_o2m(dev, list_child, value[n], O2M, O2M_list, rtime, msg)
        elif child_type in [ua.VariantType.ExtensionObject]:  # child's data type is structure, recursion
            value[n] = struct_parse_o2m(dev, list_child, value[n] if type(value[n]) is dict else value[n].__dict__, O2M,
                                        O2M_list, rtime, msg)
        elif O2M_list is not None and (O2M is True or list_child["value"] != value[n]):  # opcua2mqtt
            #  2024/11/22  增加高精度浮点运算
            if list_child["DataTypeString"] == "float" or list_child["DataTypeString"] == "double":
                precision = list_child["DecimalPoint"]
                # print(precision)
                value[n] = round_half_up(value[n], precision)
                # if o2m_value != 0.0:
                #     print(f"{child.NodePath}:{o2m_value}")
            O2M_list.append({"code": list_child["code"], "value": value[n], "dataType": list_child["DataTypeString"],"arrLen": list_child["ArrayDimensions"], "time": rtime})  # child's data type is single variable
        list_child["value"] = value[n]  # update to node
    return value


def struct_parse_o2m(dev, list_node, value: dict, O2M, O2M_list, rtime, msg: list):
    """
    parse structure of tree, update node[key].value with value[key], add to sending buffer M2O_list or O2M_list.
    """
    # print("structure source:", type(value), value, M2O, O2M)
    if type(value) is not dict:
        msg.append(f'Failure to {dev.name}{list_node["NodePath"]} is structure, but value type is {type(value)}.')
        return value

    for key in value:
        try:
            if key.startswith('_'):
                list_child = dev.code_to_node.get(
                    code2format_str(list_node['blockId'], list_node['index'], list_node['category'],
                                    list_node['code']) + '_' + key[1:])
            else:
                list_child = dev.code_to_node.get(code2format_str(list_node['blockId'], list_node['index'], list_node['category'], list_node['code']) + '_' + key)
        except:
            msg.append(f'Failure to find {dev.name}.{list_node["NodePath"]}/{key} in variable list.')
            continue
        # verification node, value and datatype
        if list_child is None:
            msg.append(f'Failure to find {dev.name}{list_node["NodePath"]}/{key} in variable list.')
            continue

        if value[key] is None:
            msg.append(f'{dev.name}{list_node["NodePath"]}/{key} Structure is not readable = {value[key]}, Null value.')
            continue

        try:
            value_type = type(value[key])
            if type(list_child["DataType"]) is str:
                list_child["DataType"] = int(list_child["DataType"])
            child_type = ua.VariantType(list_child["DataType"])

            if list_child["ArrayDimensions"] > 0:  # and value_type is list, child's data type is array, recursion
                value[key] = array_parse_o2m(dev, list_child, value[key], O2M, O2M_list, rtime, msg)
            elif child_type in [ua.VariantType.ExtensionObject]:  # child's data type is structure, recursion
                value[key] = struct_parse_o2m(dev, list_child,
                                              value[key] if type(value[key]) is dict else value[key].__dict__, O2M,
                                              O2M_list, rtime, msg)
            elif O2M_list is not None and (O2M is True or list_child["value"] != value[key]):  # opcua2mqtt
                #  2024/11/22  增加高精度浮点运算
                if list_child["DataTypeString"] == "float" or list_child["DataTypeString"] == "double":
                    precision = list_child["DecimalPoint"]
                    value[key] = round_half_up(value[key], precision)
                    # if o2m_value != 0.0:
                    #     print(f"{child.NodePath}:{o2m_value}")
                O2M_list.append(
                    {"code": list_child["code"], "value": value[key], "dataType": list_child["DataTypeString"],
                     "arrLen": list_child["ArrayDimensions"], "time": rtime})
            list_child["value"] = value[key]
            # print(list_child)
        except Exception as e:
            msg.append(f'{dev.name}{list_node["NodePath"]}/{key} 可能有重复Code，请排查')
    return value

def datas_parse_o2m(dev, list_node, value, O2M, O2M_list, rtime, msg):
    """
    recursive parse structure data
    """
    if value is None:
        return
        # msg.append(f'{dev.name}{node.NodePath} Structure is not readable = {value}, Null value.')
    else:
        # node datatype and value type, recursive parse structure data
        value_type = type(value)
        if value_type == 'str':
            value_type = 'string'
        if type(list_node['DataType']) is str:
            list_node['DataType'] = int(list_node['DataType'])
        node_type = ua.VariantType(list_node['DataType'])

        if list_node['ArrayDimensions'] > 0:  # and value_type is list, data type is array
            value = array_parse_o2m(dev, list_node, value, O2M, O2M_list, rtime, msg)
        elif node_type in [ua.VariantType.ExtensionObject]:  # data type of node is structure
            value = struct_parse_o2m(dev, list_node, value if value_type is dict else value.__dict__,
                                     O2M, O2M_list, rtime, msg)
        elif O2M_list is not None and (O2M is True or list_node['value'] != value):  # opcua2mqtt
            if list_node['DataTypeString'] == "float" or list_node['DataTypeString'] == "double":
                precision = list_node['DecimalPoint']
                # print(precision)
                value = round_half_up(value, precision)
            O2M_list.append({"code": list_node['code'], "value": value, "dataType": list_node['DataTypeString'],
                             "arrLen": list_node['ArrayDimensions'], "time": rtime})

        # update variable value to node and list
        list_node['value'] = value


def array_parse_m2o(dev, list_node, value, M2O, M2O_list, rtime, msg: list):
    """
    parse array structure of tree, update node[n].value with value[n], add to sending buffer M2O_list or O2M_list.
    """
    # print("array source:", node.ArrayDimensions, type(value), value)
    if type(value) is not list:
        msg.append(f'Failure to {dev.name}{list_node["NodePath"]}[{list_node["ArrayDimensions"]}] is array, '
                   f'but value type is {type(value)}.')
        return value

    if len(value) != list_node["ArrayDimensions"]:
        msg.append(f'Failure to match array length, {list_node["ArrayDimensions"]}!={len(value)}.')
        return value

    for n in range(list_node["ArrayDimensions"]):
        # find array[n] node
        try:
            list_child = dev.code_to_node.get(code2format_str(list_node['blockId'], list_node['index'], list_node['category'], list_node['code']) + '_' + str(n))
        except:
            msg.append(f'Failure to find {dev.name}{list_node["NodePath"]}/{n} in variable list.')
            continue

        # verification node, value and datatype
        if list_child is None:
            msg.append(f'Failure to find {dev.name}{list_node["NodePath"]}/{n} in variable list.')
            continue

        if value[n] is None:
            msg.append(f'Failure to find {dev.name}{list_node["NodePath"]}/{n} = {value[n]}, Null value.')
            continue

        value_type = type(value[n])
        if type(list_child['DataType']) is str:
            list_child['DataType'] = int(list_child['DataType'])
        child_type = ua.VariantType(list_child['DataType'])

        # value_type_name = value_type.name
        # if value_type_name == 'str':
        #     value_type_name = 'string'
        # if value_type_name != child.DataTypeString:
        #     msg.append(f'Failure to match {dev.name}{node.NodePath}/{n} data type,'
        #                f'{value_type_name},{child.DataTypeString}')
        #     continue

        if list_child['ArrayDimensions'] > 0:  # and value_type is list, child's data type is array, recursion
            value[n] = array_parse_m2o(dev, list_child, value[n],
                                   M2O, M2O_list, rtime, msg)
        elif child_type in [ua.VariantType.ExtensionObject]:  # child's data type is structure, recursion
            value[n] = struct_parse_m2o(dev, list_child,
                                    value[n] if type(value[n]) is dict else value[n].__dict__,
                                    M2O, M2O_list, rtime, msg)
        elif M2O_list is not None and (M2O is True or list_child['value'] != value[n]):  # mqtt2opcua
            if dev.link_type == 'opcua':
                value_type = type(value[n])
                original_type = data_type_from_string(list_child['DataTypeString'])
                if (isinstance(value[n], int) and isinstance(original_type, type) and original_type is float) or \
                        (isinstance(value[n], int) and hasattr(original_type,
                                                                 '__name__') and original_type.__name__ == 'float'):
                    pass
                elif original_type != value_type:
                    msg.append(
                        f'Write Data Type Error, Please check: ({list_child["NodeID"]}, datetype:{list_child["DataTypeString"]}, value:{value[n]}), '
                        f'the value should be of type {original_type.__name__}, not {value_type.__name__}')
                    return
                M2O_list.append({'node_id': list_child["NodeID"], 'datatype': list_child["DataType"], 'value': value[n]})
            elif dev.link_type == 's7':
                M2O_list.append(
                    {'s7_db': list_child["s7_db"], 's7_start': list_child["s7_start"], 's7_bit': list_child["s7_bit"],
                     's7_size': list_child["s7_size"], 'value': value[n]})
    return value


def struct_parse_m2o(dev, list_node, value: dict, M2O, M2O_list, rtime, msg: list):
    """
    parse structure of tree, update node[key].value with value[key], add to sending buffer M2O_list or O2M_list.
    """
    # print("structure source:", type(value), value, M2O, O2M)
    if type(value) is not dict:
        msg.append(f'Failure to {dev.name}{list_node["NodePath"]} is structure, but value type is {type(value)}.')
        return value

    for key in value:
        try:
            if key.startswith('_'):
                list_child = dev.code_to_node.get(code2format_str(list_node['blockId'], list_node['index'], list_node['category'], list_node['code']) + '_' + key[1:])
            else:
                list_child = dev.code_to_node.get(code2format_str(list_node['blockId'], list_node['index'], list_node['category'], list_node['code']) + '_' + key)
        except:
            msg.append(f'Failure to find {dev.name}.{list_node["NodePath"]}/{key} in variable list.')
            continue

        # verification node, value and datatype
        if list_child is None:
            msg.append(f'Failure to find {dev.name}{list_node["NodePath"]}/{key} in variable list.')
            continue

        if value[key] is None:
            msg.append(f'{dev.name}{list_node["NodePath"]}/{key} Structure is not readable = {value[key]}, Null value.')
            continue

        value_type = type(value[key])
        if type(list_child["DataType"]) is str:
            list_child["DataType"] = int(list_child["DataType"])
        child_type = ua.VariantType(list_child["DataType"])

        # value_type_name = value_type.name
        # if value_type_name == 'str':
        #     value_type.name = 'string'
        # if value_type_name != child.DataTypeString:
        #     print(f'Failure to match {dev.name}{node.NodePath}/{str(int(0) + n)} data type,'
        #           f'{value_type_name},{child.DataTypeString}')
        #     continue

        if list_child["ArrayDimensions"] > 0:  # and value_type is list, child's data type is array, recursion
            value[key] = array_parse_m2o(dev, list_child, value[key], M2O, M2O_list, rtime, msg)
        elif child_type in [ua.VariantType.ExtensionObject]:  # child's data type is structure, recursion
            value[key] = struct_parse_m2o(dev, list_child, value[key] if type(value[key]) is dict else value[key].__dict__, M2O, M2O_list, rtime, msg)
        elif M2O_list is not None and (M2O is True or list_child["value"] != value[key]):  # mqtt2opcua
            if dev.link_type == 'opcua':
                value_type = type(value[key])
                original_type = data_type_from_string(list_child['DataTypeString'])
                if (isinstance(value[key], int) and isinstance(original_type, type) and original_type is float) or \
                        (isinstance(value[key], int) and hasattr(original_type,
                                                            '__name__') and original_type.__name__ == 'float'):
                    pass
                elif original_type != value_type:
                    msg.append(f'Write Data Type Error, Please check: ({list_child["NodeID"]}, datetype:{list_child["DataTypeString"]}, value:{value[key]}), '
                               f'the value should be of type {original_type.__name__}, not {value_type.__name__}')
                    return
                M2O_list.append({'node_id': list_child["NodeID"], 'datatype': list_child["DataType"], 'value': value[key]})
            elif dev.link_type == 's7':
                M2O_list.append({'s7_db': list_child["s7_db"], 's7_start': list_child["s7_start"], 's7_bit': list_child["s7_bit"],
                                 's7_size': list_child["s7_size"], 'value': value[key]})
    return value


def datas_parse_m2o(dev, list_node, value, M2O, M2O_list, rtime, msg):
    """
    recursive parse structure data
    """
    if value is None:
        return
        # msg.append(f'{dev.name}{node.NodePath} Structure is not readable = {value}, Null value.')
    else:
        # node datatype and value type, recursive parse structure data
        value_type = type(value)
        if value_type == 'str':
            value_type = 'string'
        if type(list_node['DataType']) is str:
            list_node['DataType'] = int(list_node['DataType'])
        node_type = ua.VariantType(list_node['DataType'])

        if list_node['ArrayDimensions'] > 0:  # and value_type is list, data type is array
            value = array_parse_m2o(dev, list_node, value,
                                M2O, M2O_list, rtime, msg)
        elif node_type in [ua.VariantType.ExtensionObject]:  # data type of node is structure
            value = struct_parse_m2o(dev, list_node, value if value_type is dict else value.__dict__,
                                 M2O, M2O_list, rtime, msg)
        elif M2O_list is not None and (M2O is True or list_node['value'] != value):  # mqtt2opcua
            if dev.link_type == 'opcua':
                value_type = type(value)
                original_type = data_type_from_string(list_node['DataTypeString'])
                if (isinstance(value, int) and isinstance(original_type, type) and original_type is float) or \
                        (isinstance(value, int) and hasattr(original_type,
                                                                 '__name__') and original_type.__name__ == 'float'):
                    pass
                elif original_type != value_type:
                    msg.append(
                        f'Write Data Type Error, Please check: ({list_node["NodeID"]}, datetype:{list_node["DataTypeString"]}, value:{value}), '
                        f'the value should be of type {original_type.__name__}, not {value_type.__name__}')
                    return
                M2O_list.append({'node_id': list_node["NodeID"], 'datatype': list_node["DataType"], 'value': value})
            elif dev.link_type == 's7':
                M2O_list.append({'s7_db': list_node["s7_db"], 's7_start': list_node["s7_start"], 's7_bit': list_node["s7_bit"],
                                 's7_size': list_node["s7_size"], 'datatype': list_node["DataTypeString"], 'value': value})

        # update variable value to node and list
        list_node['value'] = value


def s7_array_parse(dev, list_node, datas, offset, M2O, M2O_list, O2M, O2M_list, rtime, msg: list):
    """
    parse array structure of tree, update node[n].value with value[n], add to sending buffer M2O_list or O2M_list.
    """
    # print("array source:", node.ArrayDimensions, type(value), value)
    value = []
    if type(datas) is not bytearray:
        msg.append(f'Failure to {dev.name}{list_node["NodePath"]} is bytearray, but datas type is {type(datas)}.')
        return value

    for n in range(list_node["ArrayDimensions"]):
        # find array[n] node
        try:
            list_child = dev.code_to_node.get(
                code2format_str(list_node['blockId'], list_node['index'], list_node['category'],
                                list_node['code']) + '_' + str(n))
        except:
            msg.append(f'Failure to find {dev.name}{list_node["NodePath"]}/{n} in variable list.')
            continue

        # verification node, value and datatype
        if list_child is None:
            msg.append(f'Failure to find {dev.name}{list_node["NodePath"]}/{n} in variable list.')
            continue

        # datas length verification
        if list_child["s7_start"] + list_child["s7_size"] > len(datas):
            msg.append(f'Failure to find {dev.name}{list_node["NodePath"]}/{n} address '
                       f'{list_child["s7_start"]}+{list_child["s7_size"]}>{len(datas)}(datas)')
            continue

        if type(list_child["DataType"]) is str:
            list_child["DataType"] = int(list_child["DataType"])
        child_type = ua.VariantType(list_child["DataType"])

        if list_child["ArrayDimensions"] > 0:  # child's data type is array, recursion
            list_child["value"] = s7_array_parse(dev, list_child, datas, offset,
                                                 M2O, M2O_list, O2M, O2M_list, rtime, msg)
        elif list_child["DataTypeString"] == 'structure':  # child's data type is structure, recursion
            list_child["value"] = s7_struct_parse(dev, list_child, datas, offset,
                                                  M2O, M2O_list, O2M, O2M_list, rtime, msg)
        else:  # child's data type is single variable
            try:
                value_t = bytes_2_ua_data(datas, list_child["s7_start"] - offset, list_child["s7_bit"], child_type)  # bytes to value
                if O2M_list is not None and (O2M is True or list_child["value"] != value_t):  # s7 2 client
                    O2M_list.append({"code": list_child["code"], "value": value_t, "dataType": list_child["DataTypeString"],
                                     "arrLen": list_child["ArrayDimensions"], "time": rtime})
            except:
                value_t = None
                msg.append(f'Failure to get {dev.name}{list_child["NodePath"]} value from s7 data, address/size:'
                           f'{list_child["s7_start"]}/{list_child["s7_size"]}, datas:{datas}.')
            list_child["value"] = value_t  # update to node
        value.append(list_child["value"])  # update to node
    return value


def s7_struct_parse(dev, list_node, datas, offset, M2O, M2O_list, O2M, O2M_list, rtime, msg: list):
    """
    parse structure of tree, update node[key].value with value[key], add to sending buffer M2O_list or O2M_list.
    """
    # print("structure source:", type(value), value, M2O, O2M)
    value = {}
    if type(datas) is not bytearray:
        msg.append(f'Failure to {dev.name}{list_node["NodePath"]} is bytearray, but datas type is {type(datas)}.')
        return value
    # print_tree(node)
    value_dict = list_node.get('value')
    if value_dict:
        leaf_keys = extract_leaf_keys_with_path(value_dict)
        for key in leaf_keys:
            if key.startswith('_'):
                child = dev.code_to_node.get(
                    code2format_str(list_node['blockId'], list_node['index'], list_node['category'],
                                    list_node['code']) + '_' + key[1:])
                if type(child["DataType"]) is str:
                    child["DataType"] = int(child["DataType"])
                child_type = ua.VariantType(child["DataType"])
                if child["ArrayDimensions"] > 0:  # child's data type is array, recursion
                    child["value"] = s7_array_parse(dev, child, datas, offset,
                                                    M2O, M2O_list, O2M, O2M_list, rtime, msg)
                elif child.DataTypeString == 'structure':  # child's data type is structure, recursion
                    child["value"] = s7_struct_parse(dev, child, datas, offset,
                                                    M2O, M2O_list, O2M, O2M_list, rtime, msg)
                else:
                    # print(child.name, child_type, offset, child.s7_start, child.s7_bit, len(datas))
                    try:
                        value_t = bytes_2_ua_data(datas, child["s7_start"] - offset, child["s7_bit"],
                                                  child_type)  # data to value
                        if O2M_list is not None and (O2M is True or child["value"] != value_t):  # opcua2mqtt
                            O2M_list.append({"code": child["code"], "value": value_t, "dataType": child["DataTypeString"],
                                             "arrLen": child["ArrayDimensions"], "time": rtime})
                    except:
                        value_t = None
                        msg.append(f'Failure to get {dev.name}{child["NodePath"]} value from s7 data, offset/address/size:'
                                   f'{offset},{child["s7_start"]}/{child["s7_size"]}, datas:{datas}.')
                    child["value"] = value_t  # update to node
                value[child["name"]] = child["value"]  # update to node
    return value


def s7_datas_parse(dev, list_node, datas, M2O, M2O_list, O2M, O2M_list, rtime, msg):
    """
    recursive parse structure data
    """
    # print("s7_datas_parse:", node.name, node.s7_start, node.s7_size, len(datas), node.DataTypeString)
    if datas is None:
        msg.append(f'Failure to find {dev.name}{list_node["NodePath"]} = {datas}, Null value.')
    else:
        # node datatype, recursive parse structure data
        if type(list_node["DataType"]) is str:
            list_node["DataType"] = int(list_node["DataType"])
        node_type = ua.VariantType(list_node["DataType"])

        if list_node["ArrayDimensions"] > 0:  # and value_type is list, data type is array
            value = s7_array_parse(dev, list_node, datas, list_node["s7_start"],
                                   M2O, M2O_list, O2M, O2M_list, rtime, msg)
        elif node_type in [ua.VariantType.ExtensionObject]:  # data type of node is structure
            value = s7_struct_parse(dev, list_node, datas, list_node["s7_start"],
                                    M2O, M2O_list, O2M, O2M_list, rtime, msg)
        elif list_node["s7_size"] <= len(datas):
            try:
                value = bytes_2_ua_data(datas, 0, list_node["s7_bit"], node_type)  # data to value
                if O2M_list is not None and (O2M is True or list_node["value"] != value):
                    O2M_list.append({"code": list_node['code'], "value": value, "dataType": list_node['DataTypeString'],
                                     "arrLen": list_node['ArrayDimensions'], "time": rtime})
                elif M2O_list is not None and (M2O is True or list_node['value'] != value):
                    M2O_list.append({'node_id': list_node['NodeID'], 'datatype': list_node['DataType'], 'value': value})
            except:
                value = None
                msg.append(f'Failure to get {dev.name}{list_node["NodePath"]} value from s7 data, address/size:'
                           f'{list_node["s7_start"]}/{list_node["s7_size"]}, datas:{datas}.')
        else:
            value = None
            msg.append(f'Failure to find {dev.name}{list_node["NodePath"]} address '
                       f'{list_node["s7_start"]}+{list_node["s7_size"]}>{len(datas)}(datas)')

        # update variable value to node and list
        list_node['value'] = value
        # print_tree(node, attr_list=['name', 'value', 'code', 'DataTypeString', 'ArrayDimensions', 'NodeID'])
