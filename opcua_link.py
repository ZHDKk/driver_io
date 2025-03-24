import asyncio
import logging

import pprint
from datetime import datetime
import time
from asyncua import Client, Node, ua
from asyncua.ua.ua_binary import struct_from_binary

from logger import log
from utils.helpers import count_decimal_places, generate_paths
from utils.time_util import get_current_time


def path_to_node_id(path):
    return str.replace(path, '/', '.')


def node_id_to_path(node_id):
    str_tmp = '/' + str.replace(node_id, '.', '/').replace('"', '').replace('[', '/').replace(']', '/')
    str_tmp = str.replace(str_tmp, '//', '/').replace('//', '/')
    if str_tmp[-1] == '/':
        str_tmp = str_tmp[:-1]
    return str_tmp


def path_2name(path):
    str_tmp = str.replace(path, '/', '_')
    return str_tmp[1:]


def name_2path(path, name):
    """
    add name to path
    """
    str_tmp = path + '/' + name
    return str_tmp


def path_2info(path: str):
    """
    convert path to information, '/0_1_MC/Basic/ID'->{"blockId": 0, "index": 1, "category": 'MC', "name":Basic_ID}
    """
    str_tmp = path
    result = {"category": '', "blockId": 0, "index": 0, "name": str.replace(str_tmp[1:], '/', '_')}

    str_tmp = path
    for n in range(len(str_tmp)):
        # '/'  '/' split
        try:
            index = str_tmp.index('/')  # header
            str_tmp = str_tmp[index + 1:]
        except ValueError:
            return result  # none header

        try:
            index = str_tmp.index('/')  # section end
            rest = str_tmp[:index]
            name = str_tmp[index + 1:]
        except ValueError:
            rest = str_tmp[:]  # none header
            name = rest

        # print(str_tmp)
        # print(rest)

        # '_'  '_' split
        try:
            index = rest.index('_')
            section_1 = rest[:index]
            rest = rest[index + 1:]
        except ValueError:
            continue  # none split

        try:
            index = rest.index('_')
            section_2 = rest[:index]
            rest = rest[index + 1:]
        except ValueError:
            continue  # none split

        # print(section_1, section_2, rest)

        # identify string
        if section_1.isdigit() and section_2.isdigit() and len(rest) > 0:
            result["blockId"] = int(section_1)
            result["index"] = int(section_2)
            result["category"] = rest
            result["name"] = name.replace('/', '_')
            return result
        else:
            continue

    return result


def ua_data_type_to_string(var_type: ua.VariantType):
    """
    opcua data type to string
    """
    var_type_str = 'unknown'
    match var_type:
        case ua.VariantType.Null:
            var_type_str = 'null'
        case ua.VariantType.Boolean:
            var_type_str = 'bool'
        case ua.VariantType.SByte:
            var_type_str = 'sbyte'
        case ua.VariantType.Byte:
            var_type_str = 'byte'
        case ua.VariantType.Int16:
            var_type_str = 'int16'
        case ua.VariantType.UInt16:
            var_type_str = 'uint16'
        case ua.VariantType.Int32:
            var_type_str = 'int32'
        case ua.VariantType.UInt32:
            var_type_str = 'uint32'
        case ua.VariantType.Int64:
            var_type_str = 'int64'
        case ua.VariantType.UInt64:
            var_type_str = 'uint64'
        case ua.VariantType.Float:
            var_type_str = 'float'
        case ua.VariantType.Double:
            var_type_str = 'double'
        case ua.VariantType.String:
            var_type_str = 'string'
        case ua.VariantType.ByteString:
            var_type_str = 'bytes'
        case ua.VariantType.DateTime:
            var_type_str = 'datetime'
        case ua.VariantType.Guid:
            var_type_str = 'guid'
        case ua.VariantType.ExtensionObject:
            var_type_str = 'structure'
    return var_type_str


def ua_data_type_size(var_type: ua.VariantType):
    """
    opcua data type to string
    """
    var_type_size = 0
    match var_type:
        case ua.VariantType.Null:
            var_type_size = 0
        case ua.VariantType.Boolean:
            var_type_size = 1
        case ua.VariantType.SByte:
            var_type_size = 1
        case ua.VariantType.Byte:
            var_type_size = 1
        case ua.VariantType.Int16:
            var_type_size = 2
        case ua.VariantType.UInt16:
            var_type_size = 2
        case ua.VariantType.Int32:
            var_type_size = 4
        case ua.VariantType.UInt32:
            var_type_size = 4
        case ua.VariantType.Int64:
            var_type_size = 8
        case ua.VariantType.UInt64:
            var_type_size = 8
        case ua.VariantType.Float:
            var_type_size = 4
        case ua.VariantType.Double:
            var_type_size = 8
        case ua.VariantType.String:
            var_type_size = 256
        case ua.VariantType.ByteString:
            var_type_size = 0
        case ua.VariantType.DateTime:
            var_type_size = 0
        case ua.VariantType.Guid:
            var_type_size = 0
        case ua.VariantType.ExtensionObject:
            var_type_size = 0
    return var_type_size


def convert_ua_data(vType, value):
    match ua.VariantType(vType):
        case ua.VariantType.Boolean:
            return bool(value)
        case ua.VariantType.SByte:
            return int(value)
        case ua.VariantType.Byte:
            return int(value)
        case ua.VariantType.Int16:
            return int(value)
        case ua.VariantType.UInt16:
            return int(value)
        case ua.VariantType.Int32:
            return int(value)
        case ua.VariantType.UInt32:
            return int(value)
        case ua.VariantType.Float:
            return float(value)
        case ua.VariantType.Double:
            return float(value)
        case ua.VariantType.String:
            return str(value)


class SubHandler(object):
    """
    Subscription Handler. To receive events from server for a subscription
    data_change and event methods are called directly from receiving thread.
    Do not do expensive, slow or network operation there. Create another
    thread if you need to do such a thing
    """

    def __init__(self, opcua_name, collection_handler):
        self.opcua_name = opcua_name
        self.collection_handler = collection_handler  # return data of subscription handle

    def datachange_notification(self, node: Node, val, data):
        """
        called for every data change notification from server
        """
        # collection datas to data source of system
        if self.collection_handler is not None:
            self.collection_handler(self.opcua_name, node.nodeid.to_string(), val)
        else:
            print('Do not register data collection method!')
        # print(current_time, node.nodeid.NamespaceIndex, node.nodeid.Identifier, val)

    def event_notification(self, event):
        print("New event", event)


class opcua_linker(object):
    """
    opc ua linker, link opc ua server, browse nodes, subscription and write function
    """

    def __init__(self, config):
        """
        opcua client init
        """
        self.uri = config['uri']
        self.main_node = config['main_node']
        self.timeout = config['timeout']
        self.watchdog_interval = config['watchdog_interval']

        self.client = Client(self.uri, timeout=self.timeout, watchdog_intervall=self.watchdog_interval)

        self.subscription = None
        self.rw_failure_count = 0  # read write failure count
        self.last_linking_time = 0  # last reading variables or connecting time
        self.linking = False

        self.retry_write = 0  # 记录重写次数
        self.retry_write_max = 5  # 重写次数上限
        self.read_value = None
        self.var_obj_read_flag = 0
        self.write_variable_count = 10

    async def new_client(self):
        self.client = Client(self.uri, timeout=self.timeout, watchdog_intervall=self.watchdog_interval)

    async def link(self):
        """
        link to opcua server
        """
        try:
            await self.client.connect()
            res = await self.client.load_data_type_definitions(overwrite_existing=True)
            # print(res)
            self.rw_failure_count = 0
            self.last_linking_time = int(time.time() * 1000)
            # print(f'link to {self.uri}:{self.linking}')
            # log.info(f'link to {self.uri}:{self.linking}')
            await asyncio.sleep(1)
            self.linking = True
            return True
        except:
            self.linking = False
            log.warning(f'Failure to link to {self.uri}.')
            return False

    async def unlink(self):
        """
        unlink to opcua server
        """
        try:
            self.linking = False
            # await asyncio.sleep(1)
            # self.rw_failure_count = 0
            await self.client.disconnect()
            print(f'Unlink to {self.uri}:{self.linking}')
            log.info(f'Unlink to {self.uri}:{self.linking}')
            return True
        except:
            log.warning(f'Failure to unlink to {self.uri}.')
            return False

    async def get_link_state(self):
        """
        check link state
        """
        if self.rw_failure_count > 5:
            if self.linking is True:
                await self.unlink()
                self.linking = False
                self.rw_failure_count = 0
                log.warning(f'unlink opcua {self.uri} {self.linking}')

        # time_current = int(time.time() * 1000)
        # if time_current - self.last_linking_time > 5000:
        #     if self.linking is True:
        #         await self.unlink()
        #         self.linking = False
        #         # print(f'unlink opcua {self.uri} {self.linking}')
        #         log.warning(f'unlink opcua {self.uri} {self.linking}')
        # else:
        #     self.linking = True
        # print(time_current - self.last_linking_time, 'ms', self.uri, self.linking)
        # link_result = await self.client.check_connection()
        return self.linking

    async def subscription_variables(self, nodes):
        """
        subscription variables of nodes.
        """
        for n in nodes:
            # print(n)
            try:
                await self.subscription.subscribe_data_change(self.client.get_node(n))
            except:
                log.warning(f'Failure to subscribe {n}')

    async def write_multi_variables(self, variables, timeout=0.1, batch_size=500):
        """
        将变量分批次写入 OPC UA 服务器。
        :param variables: 要写入的变量列表，每个元素是一个字典，包含 'node_id', 'value', 和 'datatype'。
        :param batch_size: 每批写入的变量数量。
        :param timeout: 每批写入操作的超时时间。
        """
        print(f"{get_current_time()} 写入变量总数量：{len(variables)}")
        log.info(f"写入变量总数量：{len(variables)}")
        timeout = 8  # 给一个默认超时时间8s
        if len(variables) < batch_size:
            try:
                self.retry_write = 0  # 重写复位
                state = await self.write_variables(variables, timeout, 1, 1)
                return state
            except Exception as e:
                return False
        else:
            try:
                total_batches = (len(variables) + batch_size - 1) // batch_size  # 计算总批次数
                tasks = []
                success = False
                for i in range(total_batches):
                    batch = variables[i * batch_size:(i + 1) * batch_size]
                    # tasks.append(self.write_variables(batch, timeout, batch_num=i + 1, total_batches=total_batches))
                    self.retry_write = 0  # 重写复位
                    success = await self.write_variables(batch, timeout, i + 1, total_batches)
                    if success:
                        continue
                    else:
                        break
                # success_all = await asyncio.gather(*tasks)
                # print(success_all)
                # return all(success_all)  # 如果 success_all 中所有元素都是 True，则返回 True；否则返回 False
                return success
            except Exception as e:
                return False

    async def write_variables(self, variables, timeout, batch_num, total_batches):
        """
        将多个变量写入 OPC UA 服务器。
        :param variables: 要写入的变量列表，每个元素是一个字典，包含 'node_id', 'value', 和 'datatype'。
        :param timeout: 写入操作的超时时间。
        :param batch_num: 当前批次号，用于日志记录。
        :param total_batches: 总批次数，用于日志记录。
        """
        self.write_variable_count = len(variables)
        write_state_fail_docs = []  # 记录写入失败的状态描述
        try:
            request = ua.WriteRequest()
            for v in variables:
                attr = ua.WriteValue()
                attr.NodeId = ua.NodeId.from_string(v['node_id'])
                attr.AttributeId = ua.AttributeIds.Value
                attr.Value = ua.DataValue(ua.Variant(v['value'], ua.VariantType(v['datatype'])))
                request.Parameters.NodesToWrite.append(attr)
            # print(request)

            start_time = int(time.time() * 1000)
            data = await self.client.uaclient.protocol.send_request(request)
            response = struct_from_binary(ua.WriteResponse, data)
            response.ResponseHeader.ServiceResult.check()
            write_states = response.Results
            # write_states = await self.client.uaclient.write_attributes(nodes, values, ua.AttributeIds.Value)
            write_time = int(time.time() * 1000) - start_time
            self.last_linking_time = int(time.time() * 1000)

            # 检查写入结果
            for i, write_state in enumerate(write_states):
                if write_state.value != 0:  # 只记录无法写入的变量
                    fail_msg = (f"---->>Node ID: {variables[i]['node_id']}，"
                                f"写入返回的状态码: {write_state.value}，描述: {write_state.doc}<<----")
                    write_state_fail_docs.append(fail_msg)

            # 统一日志输出
            if self.write_variable_count < 5:
                message_prefix = f"{get_current_time()} 第{batch_num}/{total_batches}批次，变量：{variables}"
            else:
                message_prefix = f"{get_current_time()} 第{batch_num}/{total_batches}批次，总数量：{self.write_variable_count}"

            if not write_state_fail_docs:
                success_message = f"{message_prefix} 通过OPCUA写入成功，耗时 {write_time}ms"
                print(success_message)
                log.info(success_message)
                return True
            else:
                fail_message = f"{message_prefix} ，结果：{write_state_fail_docs}无法通过OPCUA写入"
                log.warning(fail_message)
                return False

        except asyncio.TimeoutError:
            if self.write_variable_count < 5:
                message_prefix = f"第{batch_num}/{total_batches}批次，变量：{variables}"
            else:
                message_prefix = f"第{batch_num}/{total_batches}批次，总数量：{self.write_variable_count}"
            log.warning(f"{message_prefix}，写入{timeout}s超时")
        except Exception as e:
            if self.write_variable_count < 5:
                message_prefix = f"第{batch_num}/{total_batches}批次，变量：{variables}"
            else:
                message_prefix = f"第{batch_num}/{total_batches}批次，总数量：{self.write_variable_count}"
            log.warning(f"{message_prefix}，写入失败：{variables}，错误信息：{e}")

        # 失败重试逻辑
        if self.retry_write < self.retry_write_max:
            self.retry_write += 1
            retry_message = f"{get_current_time()} 第{batch_num}/{total_batches}批次，尝试第{self.retry_write}次重写"
            print(retry_message)
            log.info(retry_message)
            return await self.write_variables(variables, timeout, batch_num, total_batches)
        else:
            if self.write_variable_count < 5:
                log.warning(f"最终失败：无法通过 OPC UA 写入 {variables}")
            else:
                log.warning(f"最终失败：无法通过 OPC UA 写入 {len(variables)}")
            return False

    # async def write_variables(self, variables, timeout, batch_num, total_batches):
    #     """
    #     将多个变量写入 OPC UA 服务器。
    #     :param variables: 要写入的变量列表，每个元素是一个字典，包含 'node_id', 'value', 和 'datatype'。
    #     :param timeout: 写入操作的超时时间。
    #     :param batch_num: 当前批次号，用于日志记录。
    #     :param total_batches: 总批次数，用于日志记录。
    #     """
    #     nodes = []
    #     values = []
    #     write_state_values = []  # 通过opcua写入变量返回的状态码
    #     write_state_fail_docs = []  # 通过opcua写入变量失败的返回的状态描述
    #     try:
    #         for v in variables:
    #             nodes.append(ua.NodeId.from_string(v['node_id']))
    #             values.append(ua.DataValue(ua.Variant(v['value'], ua.VariantType(v['datatype']))))
    #
    #         stat_t = int(time.time() * 1000)
    #         # 根据批次大小动态调整 timeout，按批次大小线性增加 每增加一个变量，就额外增加一个固定的时间
    #         # timeout = timeout + (len(values) * 0.002)
    #         # write_states = await asyncio.wait_for(self.client.uaclient.write_attributes(nodes, values, ua.AttributeIds.Value), timeout)
    #         # print(values)
    #         write_states = await self.client.uaclient.write_attributes(nodes, values, ua.AttributeIds.Value)
    #
    #         write_time = int(time.time() * 1000) - stat_t
    #         self.last_linking_time = int(time.time() * 1000)
    #
    #         for i, write_state in enumerate(write_states):
    #             write_state_values.append(write_state.value)  # 写入变量返回的状态码
    #             if write_state.value != 0:  # 写入失败的变量整理
    #                 fail_msg = f"---->>通过opcua写入失败，返回的状态码：{write_state.value}，描述：{write_state.doc}，node_id：{variables[i]}<<----"
    #                 # print(f"{get_current_time()}:{fail_msg}")
    #                 # log.warning(fail_msg)
    #                 write_state_fail_docs.append(fail_msg)  # 写入变量返回的状态描述
    #
    #         if all(value == 0 for value in write_state_values):
    #             # 如果所有元素都是0表示所有元素都写入成功
    #             if len(variables) < 5:
    #                 print(
    #                     f'{get_current_time()}第{batch_num}/{total_batches}批次，总数量：{len(variables)} 变量:{variables}通过opcua写入成功，消耗{write_time}ms')
    #                 log.info(
    #                     f'第{batch_num}/{total_batches}批次，总数量：{len(variables)} 变量:{variables}通过opcua写入成功，消耗{write_time}ms')
    #             else:
    #                 print(
    #                     f'{get_current_time()}第{batch_num}/{total_batches}批次，总数量：{len(variables)} 变量,通过opcua写入成功，消耗{write_time}ms')
    #                 log.info(
    #                     f'第{batch_num}/{total_batches}批次，总数量：{len(variables)} 变量,通过opcua写入成功，消耗{write_time}ms')
    #             return True
    #         else:
    #             if len(variables) < 5:
    #                 log.warning(
    #                     f'第{batch_num}/{total_batches}批次，总数量：{len(variables)} 变量:{variables}通过opcua写入失败：{write_state_fail_docs}')
    #             else:
    #                 log.warning(f'第{batch_num}/{total_batches}批次，总数量：{len(variables)} 变量,通过opcua写入失败：{write_state_fail_docs}')
    #             return False
    #
    #     except Exception as e:
    #         if len(variables) < 5:
    #             log.warning(
    #                 f'第{batch_num}/{total_batches}批次，总数量：{len(variables)} 变量:{variables}通过opcua写入失败')
    #         else:
    #             log.warning(f'第{batch_num}/{total_batches}批次，总数量：{len(variables)} 变量,通过opcua写入失败')
    #         # 这包数据如果写入失败，则重新尝试写入
    #         if self.retry_write < self.retry_write_max:
    #             self.retry_write += 1
    #             print(f'{get_current_time()}第{batch_num}/{total_batches}批次，尝试第{self.retry_write}次重写')
    #             log.info(f'第{batch_num}/{total_batches}批次，尝试第{self.retry_write}次重写')
    #             state = await self.write_variables(variables, timeout, batch_num, total_batches)
    #             return state
    #         else:
    #             log.warning(f'Failure to write {variables} via opcua. because {e}')
    #             return False

    # async def write_multi_variables(self, variables, timeout=0.1):
    #     """
    #     Write multiple variables to opcua server
    #     """
    #     nodes = []
    #     values = []
    #     try:
    #         for v in variables:
    #             nodes.append(ua.NodeId.from_string(v['node_id']))
    #             values.append(ua.DataValue(ua.Variant(v['value'], ua.VariantType(v['datatype']))))
    #         await asyncio.wait_for(self.client.uaclient.write_attributes(nodes, values, ua.AttributeIds.Value), timeout)
    #         self.last_linking_time = int(time.time() * 1000)
    #         log.info(f'Success to Write {variables} variables via opcua.')
    #         return True
    #     except:
    #         log.warning(f'Failure to write {variables} via opcua.')
    #         # print(nodes)
    #         # print(values)
    #         return False

    async def read_multi_variables(self, node_id, timeout=0.2):
        """
        Read multiple variables from opcua server
        """
        result = []

        try:
            nodes = []
            for n in node_id:
                nodes.append(ua.NodeId.from_string(n))
            value = await asyncio.wait_for(self.client.uaclient.read_attributes(nodes, ua.AttributeIds.Value), timeout)
            # print(value)

            for v in value:
                result.append(v.Value.Value)
            # pprint.pprint('',result[0])

            self.last_linking_time = int(time.time() * 1000)
            if self.rw_failure_count > 2:
                self.rw_failure_count -= 2
            else:
                self.rw_failure_count = 0
            return result
        except:
            self.rw_failure_count += 1
            result = []
            log.warning(f'Failure to read opcua: {node_id}, timeout is {timeout}.')
            return result

    async def check_write_result(self, nodes):
        """
        check wrote result
        """
        node_id = []
        value = []
        for n in nodes:
            node_id.append(n['node_id'])
            value.append(n['value'])

        # read wrote value
        datas = await self.read_multi_variables(node_id, timeout=0.2 + len(node_id) * 0.05)
        if not datas:
            log.warning(f'Failure to read opcua {self.uri}, check wrote {node_id}.')
            return False
        # print(nodes)
        # print(node_id)
        # print(value)
        # print(data)

        # check write result
        if value == datas:
            log.info(f'success to check wrote {node_id} {value} to {self.uri}.')
            return True
        else:
            log.warning(f'Failure to write check wrote {node_id} to {self.uri}, {datas}!={value}.')
            return False

