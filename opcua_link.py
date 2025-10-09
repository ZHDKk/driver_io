import asyncio
import logging
import math

import pprint
from datetime import datetime
import time
from asyncua import Client, Node, ua
from asyncua.ua.ua_binary import struct_from_binary

from logger import log
from utils.helpers import count_decimal_places, generate_paths, is_target_format
from utils.time_util import get_current_time

# 在文件开头添加容差比较相关的函数和常量

# 浮点数比较容差
FLOAT_ABSOLUTE_TOLERANCE = 1e-6
FLOAT_RELATIVE_TOLERANCE = 1e-5


def is_float_type(datatype):
    """判断是否为浮点数类型"""
    return datatype in [ua.VariantType.Float, ua.VariantType.Double]


def are_values_equal(expected, actual, datatype, absolute_tolerance=None, relative_tolerance=None):
    """
    容差比较函数，支持浮点数精度处理
    :param expected: 期望值
    :param actual: 实际值
    :param datatype: 数据类型
    :param absolute_tolerance: 绝对容差
    :param relative_tolerance: 相对容差
    :return: 是否在容差范围内相等
    """
    if absolute_tolerance is None:
        absolute_tolerance = FLOAT_ABSOLUTE_TOLERANCE
    if relative_tolerance is None:
        relative_tolerance = FLOAT_RELATIVE_TOLERANCE

    # 处理None值
    if expected is None and actual is None:
        return True
    if expected is None or actual is None:
        return False

    # 处理字符串类型
    if isinstance(expected, str) or isinstance(actual, str):
        return str(expected) == str(actual)

    # 处理布尔类型
    if isinstance(expected, bool) or isinstance(actual, bool):
        return bool(expected) == bool(actual)

    # 处理整数类型
    if isinstance(expected, int) and isinstance(actual, int):
        return expected == actual

    # 处理浮点数类型的精度问题
    if is_float_type(datatype):
        # 如果两个值都是NaN
        if math.isnan(expected) and math.isnan(actual):
            return True

        # 如果其中一个值是NaN
        if math.isnan(expected) or math.isnan(actual):
            return False

        # 如果两个值都是无穷大
        if math.isinf(expected) and math.isinf(actual):
            return (expected > 0) == (actual > 0)

        # 绝对容差比较
        if abs(expected - actual) <= absolute_tolerance:
            return True

        # 相对容差比较（避免除以0）
        if expected == 0 or actual == 0:
            return abs(expected - actual) <= absolute_tolerance

        # 计算相对误差
        relative_error = abs(expected - actual) / max(abs(expected), abs(actual))
        return relative_error <= relative_tolerance

    # 默认精确比较
    return expected == actual


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
    改进路径解析，优先提取最深层符合格式的模块信息
    """
    parts = path.split('/')
    for part in reversed(parts):  # 从后向前查找第一个有效模块
        if not part:
            continue
        clean_part = part.replace("B_", "") if part.startswith("B_") else part
        segments = clean_part.split('_')
        if len(segments) >= 3 and segments[0].isdigit() and segments[1].isdigit():
            block_id = int(segments[0])
            index = int(segments[1])
            category = '_'.join(segments[2:])
            return {
                "blockId": block_id,
                "index": index,
                "category": category,
                "name": '_'.join(parts[parts.index(part) + 1:])  # 提取后续路径作为 name
            }
    # 默认返回（当路径中无有效模块时）
    return {"blockId": 0, "index": 0, "category": "Unknown", "name": path[1:].replace('/', '_')}


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

        self.retry_write_max = 5  # 重写次数上限
        self.read_value = None
        self.var_obj_read_flag = 0
        self.write_variable_count = 10

        # 新增配置参数
        self.base_timeout = 2  # 基础超时时间（秒）
        self.max_timeout = 30  # 最大超时时间（秒）
        self.read_retry_max = 3  # 读取最大重试次数
        self.write_verification_enabled = False  # 默认禁用写入验证
        self.verification_retry_max = 3  # 验证失败后的最大重写次数
        self.adaptive_batch_size = True  # 是否启用自适应批次大小
        self.min_batch_size = 50  # 最小批次大小
        self.max_batch_size = 400  # 最大批次大小

        # 容差配置
        self.float_absolute_tolerance = FLOAT_ABSOLUTE_TOLERANCE
        self.float_relative_tolerance = FLOAT_RELATIVE_TOLERANCE

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

        # 自适应批次大小
        if self.adaptive_batch_size:
            batch_size = self._calculate_adaptive_batch_size(len(variables))

        # 动态计算超时时间
        calculated_timeout = self._calculate_timeout(len(variables))
        timeout = max(timeout, calculated_timeout)  # 使用较大的超时值

        if len(variables) <= batch_size:
            try:
                state = await self.write_variables(variables, timeout, 1, 1)

                # 写入验证
                if state and self.write_verification_enabled:
                    verification_result, failed_variables = await self._verify_write_result_with_retry(variables)
                    if not verification_result and failed_variables:
                        log.warning(
                            f"写入验证失败，{len(failed_variables)}个变量需要重写: {[v['node_id'] for v in failed_variables]}")

                        # 对验证失败的变量进行重写
                        rewrite_success = await self._rewrite_failed_variables(failed_variables, timeout)
                        if rewrite_success:
                            log.info(f"重写成功: {len(failed_variables)}个变量")
                            return True
                        else:
                            log.error(f"重写失败: {len(failed_variables)}个变量")
                            return False
                    elif verification_result:
                        log.info(f"写入验证成功: {len(variables)}个变量")
                        return True
                    else:
                        log.error(f"写入验证失败且无法获取失败变量列表")
                        return False

                return state
            except Exception as e:
                log.error(f"写入单批次异常: {e}")
                return False
        else:
            try:
                total_batches = (len(variables) + batch_size - 1) // batch_size  # 计算总批次数
                success_all = True
                all_failed_variables = []  # 收集所有批次的失败变量

                for i in range(total_batches):
                    batch = variables[i * batch_size:(i + 1) * batch_size]
                    batch_success = await self.write_variables(batch, timeout, i + 1, total_batches)

                    # 验证写入结果
                    if batch_success and self.write_verification_enabled:
                        verification_result, failed_variables = await self._verify_write_result_with_retry(batch)
                        if not verification_result and failed_variables:
                            log.warning(f"批次{i + 1}写入验证失败，{len(failed_variables)}个变量需要重写")
                            all_failed_variables.extend(failed_variables)
                            batch_success = False

                    if not batch_success:
                        success_all = False

                # 如果有验证失败的变量，尝试重写
                if all_failed_variables:
                    log.warning(f"总共{len(all_failed_variables)}个变量验证失败，尝试重写")
                    rewrite_success = await self._rewrite_failed_variables(all_failed_variables, timeout)
                    if rewrite_success:
                        log.info(f"重写成功: {len(all_failed_variables)}个变量")
                        return True
                    else:
                        log.error(f"重写失败: {len(all_failed_variables)}个变量")
                        return False

                return success_all
            except Exception as e:
                log.error(f"写入多批次异常: {e}")
                return False

    async def _rewrite_failed_variables(self, failed_variables, timeout, retry_count=0):
        """
        重写验证失败的变量
        :param failed_variables: 验证失败的变量列表
        :param timeout: 超时时间
        :param retry_count: 当前重试次数
        """
        if retry_count >= self.verification_retry_max:
            log.error(f"重写达到最大次数{self.verification_retry_max}，放弃重写{len(failed_variables)}个变量")
            return False

        log.info(f"第{retry_count + 1}次重写{len(failed_variables)}个验证失败的变量")

        # 重写失败的变量
        rewrite_success = await self.write_variables(failed_variables, timeout, 1, 1, 0)

        if rewrite_success:
            # 验证重写结果
            await asyncio.sleep(0.1)  # 给PLC更多处理时间
            verification_result, still_failed_variables = await self._verify_write_result_with_retry(failed_variables)

            if verification_result:
                log.info(f"重写验证成功: {len(failed_variables)}个变量")
                return True
            elif still_failed_variables:
                log.warning(f"重写后仍有{len(still_failed_variables)}个变量验证失败")

                # 指数退避后再次重写
                retry_delay = 0.2 * (2 ** retry_count)
                await asyncio.sleep(retry_delay)

                return await self._rewrite_failed_variables(still_failed_variables, timeout, retry_count + 1)
            else:
                log.error(f"重写验证无法确定结果")
                return False
        else:
            log.warning(f"重写操作失败")
            return False

    async def write_variables(self, variables, timeout, batch_num, total_batches, retry_count=0):
        """
        将多个变量写入 OPC UA 服务器。
        :param variables: 要写入的变量列表，每个元素是一个字典，包含 'node_id', 'value', 和 'datatype'。
        :param timeout: 写入操作的超时时间。
        :param batch_num: 当前批次号，用于日志记录。
        :param total_batches: 总批次数，用于日志记录。
        :param retry_count: 当前重试次数（内部使用）
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

            start_time = int(time.time() * 1000)
            data = await self.client.uaclient.protocol.send_request(request)
            response = struct_from_binary(ua.WriteResponse, data)
            response.ResponseHeader.ServiceResult.check()
            write_states = response.Results
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
                success_message = f"{message_prefix} 通过OPCUA写入成功，耗时 {write_time}ms, {self.uri}"
                print(success_message)
                log.info(success_message)

                # 关键写入后添加短暂延迟，确保PLC处理
                if self.write_variable_count > 0:
                    await asyncio.sleep(0.01)  # 10ms延迟

                return True
            else:
                fail_message = f"{message_prefix} ，结果：{write_state_fail_docs}无法通过OPCUA写入, {self.uri}"
                log.warning(fail_message)
                return False

        except asyncio.TimeoutError:
            error_type = "超时"
            error_detail = f"写入{timeout}s超时"
        except ua.UaError as e:
            error_type = "OPC UA错误"
            error_detail = f"UA错误: {e}"
        except ConnectionError as e:
            error_type = "连接错误"
            error_detail = f"连接异常: {e}"
        except Exception as e:
            error_type = "未知错误"
            error_detail = f"异常: {e}"

        # 错误日志
        if self.write_variable_count < 5:
            message_prefix = f"第{batch_num}/{total_batches}批次，变量：{variables}"
        else:
            message_prefix = f"第{batch_num}/{total_batches}批次，总数量：{self.write_variable_count}"

        log.warning(f"{message_prefix}，{error_type}: {error_detail}, {self.uri}")

        # 失败重试逻辑
        if retry_count < self.retry_write_max:
            retry_count += 1
            retry_delay = 0.1 * (2 ** retry_count)  # 指数退避策略
            retry_message = f"{get_current_time()} 第{batch_num}/{total_batches}批次，尝试第{retry_count}次重写，等待{retry_delay:.2f}s后重试, {self.uri}"
            print(retry_message)
            log.info(retry_message)

            await asyncio.sleep(retry_delay)
            return await self.write_variables(variables, timeout, batch_num, total_batches, retry_count)
        else:
            if self.write_variable_count < 5:
                log.warning(f"最终失败：无法通过 OPC UA 写入 {variables}, {self.uri}, 请把批次数量减少再次尝试")
            else:
                log.warning(
                    f"最终失败：无法通过 OPC UA 写入 {len(variables)} 个变量, {self.uri}, 请把批次数量减少再次尝试")
            return False

    async def _verify_write_result_with_retry(self, variables, max_verification_attempts=2):
        """
        验证写入结果，返回验证结果和失败的变量列表（使用容差比较）
        :param variables: 写入的变量列表
        :param max_verification_attempts: 最大验证尝试次数
        :return: (是否全部成功, 失败的变量列表)
        """
        node_ids = [v['node_id'] for v in variables]
        expected_values = [v['value'] for v in variables]
        failed_variables = []

        for attempt in range(max_verification_attempts):
            await asyncio.sleep(0.05 * (attempt + 1))  # 逐渐增加等待时间
            read_values = await self.read_multi_variables(node_ids)

            if read_values and len(read_values) == len(expected_values):
                # 检查每个变量的值（使用容差比较）
                current_failed = []
                for i, (var, expected, actual) in enumerate(zip(variables, expected_values, read_values)):
                    datatype = ua.VariantType(var['datatype'])

                    # 使用容差比较函数
                    if not are_values_equal(expected, actual, datatype,
                                            self.float_absolute_tolerance,
                                            self.float_relative_tolerance):
                        current_failed.append(var)
                        if attempt == max_verification_attempts - 1:  # 最后一次尝试才记录详细信息
                            # 记录详细的精度差异信息
                            if is_float_type(datatype):
                                diff = abs(expected - actual) if expected is not None and actual is not None else None
                                log.warning(f"浮点数变量验证失败: {var['node_id']}, "
                                            f"期望{expected}≠实际{actual}, 差值{diff}, "
                                            f"类型{ua_data_type_to_string(datatype)}")
                            else:
                                log.warning(f"变量验证失败: {var['node_id']}, 期望{expected}≠实际{actual}，请检查该变量是否支持写入")

                failed_variables = current_failed

                if not failed_variables:
                    log.info(f"写入验证成功: {len(variables)}个变量")
                    return True, []
                else:
                    if attempt < max_verification_attempts - 1:
                        log.warning(f"写入验证不匹配(尝试{attempt + 1}): {len(failed_variables)}个变量失败")
                    else:
                        log.error(f"写入验证最终失败: {len(failed_variables)}个变量失败")
            else:
                log.warning(
                    f"验证读取失败或数量不匹配: 期望{len(expected_values)}，实际{len(read_values) if read_values else 0}")
                # 如果读取失败，认为所有变量都需要重写
                failed_variables = variables.copy()

        return False, failed_variables

    async def read_multi_variables(self, node_ids, timeout=0.2, max_retries=None):
        """
        读取多个变量，支持重试机制
        :param node_ids: 要读取的节点ID列表
        :param timeout: 读取超时时间
        :param max_retries: 最大重试次数，为None时使用默认值
        """
        if max_retries is None:
            max_retries = self.read_retry_max

        result = []
        last_exception = None

        for attempt in range(max_retries + 1):  # +1 包含首次尝试
            try:
                nodes = []
                for n in node_ids:
                    nodes.append(ua.NodeId.from_string(n))

                # 动态调整超时时间
                adjusted_timeout = timeout + (len(node_ids) * 0.2)
                value = await asyncio.wait_for(
                    self.client.uaclient.read_attributes(nodes, ua.AttributeIds.Value),
                    adjusted_timeout
                )

                result = [v.Value.Value if v.Value is not None else None for v in value]

                self.last_linking_time = int(time.time() * 1000)
                if self.rw_failure_count > 2:
                    self.rw_failure_count -= 2
                else:
                    self.rw_failure_count = 0

                # 记录性能信息（慢操作警告）
                # if adjusted_timeout > 1.0:
                #     log.warning(f"慢读取操作: {len(node_ids)}个变量，超时{adjusted_timeout:.2f}s")

                return result

            except asyncio.TimeoutError:
                last_exception = f"读取超时，超时时间: {timeout}s"
                log.warning(f"第{attempt + 1}次读取超时: {node_ids}")
            except ua.UaError as e:
                last_exception = f"OPC UA错误: {e}"
                log.warning(f"第{attempt + 1}次读取UA错误: {e}")
            except ConnectionError as e:
                last_exception = f"连接错误: {e}"
                log.warning(f"第{attempt + 1}次读取连接错误: {e}")
            except Exception as e:
                last_exception = f"未知错误: {e}"
                log.warning(f"第{attempt + 1}次读取未知错误: {e}")

            # 如果不是最后一次尝试，则等待后重试
            if attempt < max_retries:
                retry_delay = 0.05 * (2 ** attempt)  # 指数退避
                await asyncio.sleep(retry_delay)
            else:
                self.rw_failure_count += 1
                log.error(f"读取失败，已重试{max_retries}次: {last_exception}")
                break

        return result  # 返回空列表或部分结果

    def _calculate_adaptive_batch_size(self, total_variables):
        """
        计算自适应批次大小
        """
        if total_variables <= self.min_batch_size:
            return total_variables  # 小批量直接全写
        elif total_variables <= 100:
            return self.min_batch_size
        else:
            # 大批次时适当减小批次大小
            return min(self.max_batch_size, max(self.min_batch_size, total_variables // 3))

    def _calculate_timeout(self, variable_count):
        """
        根据变量数量动态计算超时时间
        """
        base_timeout = self.base_timeout
        variable_factor = variable_count * 0.01  # 每个变量增加10ms
        calculated_timeout = base_timeout + variable_factor
        return min(calculated_timeout, self.max_timeout)

    async def check_write_result(self, nodes):
        """
        check wrote result (使用容差比较)
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

        # check write result (使用容差比较)
        all_success = True
        for i, (expected, actual, node) in enumerate(zip(value, datas, nodes)):
            datatype = ua.VariantType(node['datatype'])
            if not are_values_equal(expected, actual, datatype,
                                    self.float_absolute_tolerance,
                                    self.float_relative_tolerance):
                log.warning(f'变量验证失败: {node_id[i]}, 期望{expected}≠实际{actual}')
                all_success = False

        if all_success:
            log.info(f'success to check wrote {node_id} {value} to {self.uri}.')
            return True
        else:
            log.warning(f'Failure to write check wrote {node_id} to {self.uri}.')
            return False

    async def read_node_type(self, node: Node):
        """
            读节点类型
        :param node:
        :return:
        """
        type_result = None
        try:
            var_type = (await node.read_data_type_as_variant_type()).value
            var_type_str = ua_data_type_to_string(ua.VariantType(var_type))
            type_result = {'DataType': int(var_type), 'DataTypeString': var_type_str}
        except:
            print(f'无法确定节点变量类型:{node}')

        return type_result

    async def read_node_info(self, node: Node, path: str):
        """
            读取节点信息
        """
        # current node information
        value = 0
        decimal_point = 0
        node_name = (await node.read_browse_name()).Name
        node_name = str.replace(node_name, '[' or ']', '')
        if is_target_format(node_name):
            node_class = ua.NodeClass.Object
        else:
            node_class = ua.NodeClass.Variable
        # node_class = await node.read_node_class()
        if node_class in [ua.NodeClass.Object]:
            path = name_2path(path, node_name)
        info = path_2info(path)
        current_block_id = info["blockId"]
        current_index = info["index"]
        current_category = info["category"]
        current_code = info["name"]
        code_format = f"{current_block_id}_{current_index}_{current_category}_{current_code}"
        # print(path, info)
        # print(f"{path}:{info}")
        var_type = ua.VariantType.Null.value
        var_type_str = 'Null'
        var_type_size = 0
        opcua_sub = False
        read_enable = False
        read_period = 20
        if node_class != ua.NodeClass.Variable:
            array_dimensions = 0
            description = None
            reference = None
        else:
            # read data type
            try:
                var_type = (await node.read_data_type_as_variant_type()).value
                var_type_str = ua_data_type_to_string(ua.VariantType(var_type))
                var_type_size = ua_data_type_size(ua.VariantType(var_type))
            except ua.UaError:
                print(f'无法确定节点变量类型:{path}')
                # self.emit_msgs(f"无法确定节点变量类型:{path}")

            # read array dimensions
            try:
                array_dimensions = int((await node.read_array_dimensions())[0])
            except ua.UaError:
                array_dimensions = int(0)

            # read description
            try:
                description = (await node.read_description()).Text
                if description == 'nan':
                    description = None
            except ua.UaError:
                description = None

            # read reference
            try:
                reference = (await node.get_references())
            except ua.UaError:
                reference = None
            # read value
            if var_type_str == 'structure' or array_dimensions > 0:
                value = 0
            else:
                try:
                    value = (await node.read_value())
                    if value == '':
                        value = ' '
                except ua.UaError:
                    value = 0

            # checking
            if var_type_str == 'unknown':
                print(f'{node_name} is unknown type!')

            if var_type_str == "float" or var_type_str == "double":
                decimal_point = count_decimal_places(value)
                if decimal_point == 0:
                    decimal_point = 3
        return {
            'path': path,
            'name': node_name,
            'ArrayDimensions': array_dimensions,
            'DataType': int(var_type),
            'DataTypeString': var_type_str,
            'DecimalPoint': decimal_point,
            'NodeClass': node_class.value,
            'NodeID': node.nodeid.to_string(),
            'NodePath': path,
            "blockId": current_block_id,
            "category": current_category,
            "code": current_code,
            "index": current_index,
            "mqtt_publish": False,
            "opcua_subscribe": opcua_sub,
            "read_enable": read_enable,
            "read_period": read_period,
            "read_time": 0,
            "return_time": 0,
            "s7_bit": 0,
            "s7_db": 0,
            "s7_size": var_type_size,
            "s7_start": 0,
            "timed_clear": False,
            "timed_clear_time": 1000,
            'value': value,
        }

    # 新增配置方法
    def configure_write_settings(self, base_timeout=None, max_timeout=None,
                                 retry_max=None, verification_enabled=None,
                                 verification_retry_max=None, adaptive_batch=None,
                                 min_batch=None, max_batch=None,
                                 float_absolute_tolerance=None, float_relative_tolerance=None):
        """
        动态配置写入参数
        """
        if base_timeout is not None:
            self.base_timeout = base_timeout
        if max_timeout is not None:
            self.max_timeout = max_timeout
        if retry_max is not None:
            self.retry_write_max = retry_max
        if verification_enabled is not None:
            self.write_verification_enabled = verification_enabled
        if verification_retry_max is not None:
            self.verification_retry_max = verification_retry_max
        if adaptive_batch is not None:
            self.adaptive_batch_size = adaptive_batch
        if min_batch is not None:
            self.min_batch_size = min_batch
        if max_batch is not None:
            self.max_batch_size = max_batch
        if float_absolute_tolerance is not None:
            self.float_absolute_tolerance = float_absolute_tolerance
        if float_relative_tolerance is not None:
            self.float_relative_tolerance = float_relative_tolerance

    def configure_read_settings(self, retry_max=None):
        """
        动态配置读取参数
        """
        if retry_max is not None:
            self.read_retry_max = retry_max

    # 新增容差测试方法
    def test_tolerance_comparison(self, test_cases=None):
        """
        测试容差比较功能
        """
        if test_cases is None:
            test_cases = [
                # (期望值, 实际值, 数据类型, 应该相等)
                (47.6, 47.599998474121094, ua.VariantType.Float, True),
                (1.0, 1.0000001, ua.VariantType.Float, True),
                (0.0, 0.000000001, ua.VariantType.Float, True),
                (1000.0, 1000.0001, ua.VariantType.Float, True),
                (47.6, 47.5, ua.VariantType.Float, False),  # 这个应该失败
                (1, 1, ua.VariantType.Int32, True),  # 整数应该精确匹配
                ("test", "test", ua.VariantType.String, True),
            ]

        print("容差比较测试结果:")
        for i, (expected, actual, datatype, should_equal) in enumerate(test_cases):
            result = are_values_equal(expected, actual, datatype,
                                      self.float_absolute_tolerance,
                                      self.float_relative_tolerance)
            status = "✓" if result == should_equal else "✗"
            print(f"{status} 测试{i + 1}: 期望{expected} vs 实际{actual}, "
                  f"类型{ua_data_type_to_string(datatype)}, "
                  f"结果{result}(期望{should_equal})")