import asyncio
import copy
from collections import deque, defaultdict
import time

from asyncua import Client, Node, ua
from logger import log
from utils.global_var import GlobalVar
from utils.helpers import count_decimal_places, is_target_format


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


class op_linker(object):
    """
    opc ua linker, link opc ua server, browse nodes, subscription and write function
    """

    def __init__(self, config, op_browse):
        """
        opcua client init
        """
        self.uri = config['uri']
        self.main_node = config['main_node']
        self.timeout = config['timeout']
        self.watchdog_interval = config['watchdog_interval']

        self.op_browse = op_browse

        self.client = Client(self.uri, timeout=self.timeout, watchdog_intervall=self.watchdog_interval)

        self.subscription = None
        self.rw_failure_count = 0  # read write failure count
        self.last_linking_time = 0  # last reading variables or connecting time
        self.linking = False

        self.retry_write = 0  # 记录重写次数
        self.retry_write_max = 5  # 重写次数上限
        self.read_enable_paths = {}

        self.max_concurrent = 50
        self.batch_size = 20

        # 状态跟踪
        self.node_registry = {}  # {node_path: node_data}
        self.parent_child_map = defaultdict(list)  # {parent_path: [child_paths]}
        self.processing_queue = deque()
        self.semaphore = asyncio.Semaphore(self.max_concurrent)

        # 性能监控
        self.latency_history = []
        self.path_cache = {}

        self.existing_codes = None  # 会在browse_gather中初始化

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
            self.emit_msgs(f' {self.uri} 建立连接失败')
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
            self.emit_msgs(f' {self.uri} 已断开连接')
            log.info(f'Unlink to {self.uri}:{self.linking}')
            return True
        except:
            log.warning(f'Failure to unlink to {self.uri}.')
            self.emit_msgs(f' {self.uri} 断开连接失败')
            return False

    def is_contains(self, child_name, children_node_names):
        for child in children_node_names:
            if child["name"] == child_name:
                return True

        return False

    def emit_msgs(self, content):
        print(content)

    def emit_error(self, error_msg):
        print(f"变量遍历报错：{error_msg}")
        self.op_browse.is_all_success = False

    async def set_read_enable(self, obj_node, node_name, node):
        """
        遍历目标节点的所有子节点，并将 `read_enable` 属性置为 True
        """
        children = await node.get_children()
        for child in children:
            try:
                obj_node_name = (await obj_node.read_browse_name()).Name
                child_name = (await child.read_browse_name()).Name
                read_enable_path = f"/{node_name}/{child_name}"
                is_enable = True  # 默认全为True，表示在驱动主程序中，会把该节点下的所有变量都定时读出来
                self.read_enable_paths[read_enable_path] = is_enable
                # print(f"PLC:{base_node_name}：{read_enable_path}: {is_enable}")
            except Exception as e:
                print(f"Error processing node {child}: {e}")

    async def create_config(self, basic_config):
        try:
            main_node = self.client.get_node(basic_config['main_node'])
            if basic_config['main_node'] == "ns=3;s=DataBlocksGlobal":
                await self.config_browse_pra(main_node)  # 2_0_PRA节点单独处理
            else:
                await self.config_browse(main_node, 0)  # 处理其他节点
            return self.read_enable_paths
        except Exception as e:
            print(e)
            return None

    async def config_browse_pra(self, node: Node):
        """
            2_0_PRA节点单独处理
        """
        node_children = await node.get_children()
        for child in node_children:
            child_name = (await child.read_browse_name()).Name
            child_class = await child.read_node_class()
            if child_class in [ua.NodeClass.Object]:
                if is_target_format(child_name):
                    # 设置子节点的 read_enable 属性为 True
                    await self.set_read_enable(child, child_name, child)

    async def config_browse(self, node: Node, var_obj_read_flag: int):
        node_children = await node.get_children()
        for child in node_children:
            child_name = (await child.read_browse_name()).Name
            child_class = await child.read_node_class()
            if child_class in [ua.NodeClass.Object]:
                if is_target_format(child_name):
                    # 设置子节点的 read_enable 属性为 True
                    await self.set_read_enable(node, child_name, child)
                    await self.config_browse(child, 0)
                else:
                    if var_obj_read_flag == 0:  # 如果第一级格式不是'1_1_xxx'，则再往下遍历一级找到'1_1_xxx'
                        await self.config_browse(child, 1)
                    else:
                        continue  # invalid variable module
            elif child_class in [ua.NodeClass.Variable]:
                if is_target_format(child_name):  # valid variable module
                    # 设置子节点的 read_enable 属性为 True
                    await self.set_read_enable(node, child_name, child)

    async def browse(self, node: Node, path: str, var_obj_read_flag: int, read_enable_paths, existing_codes=None):
        """
        Build a nested node tree dict by recursion (filtered by OPC UA objects and variables).
        """
        # browse child node and append to children list
        if existing_codes is None:
            existing_codes = set()  # 初始化code集合
        if not GlobalVar.get_browse_var_state():
            return
        node_children = await node.get_children()
        children = []
        decimal_point = 0
        for child in node_children:
            child_name = (await child.read_browse_name()).Name
            child_class = await child.read_node_class()
            if child_class == ua.NodeClass.Object:
                if is_target_format(child_name):  # valid variable module
                    # 设置子节点的 read_enable 属性为 True
                    children.append(await self.browse(child, name_2path(path, child_name), 0, read_enable_paths, existing_codes))  #
                else:
                    if var_obj_read_flag == 0:  # 如果第一级格式不是'1_1_xxx'，则再往下遍历一级找到'1_1_xxx'
                        children.append(await self.browse(child, name_2path(path, child_name), 1, read_enable_paths, existing_codes))
                    else:
                        continue  # invalid variable module
            elif child_class == ua.NodeClass.Variable:
                children.append(await self.browse(child, name_2path(path, child_name), 0, read_enable_paths, existing_codes))

        # current node information
        none_tmp = 0
        value = 0
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
        # if not check_string_regex(current_code):
        #     self.emit_error(f"{info['blockId']}_{info['index']}_{info['category']}模组终止遍历，检测到异常的name，"
        #                     f"可能包含特殊字符: {current_code}, 路径为：{path}")
        #     raise ValueError(f"{info['blockId']}_{info['index']}_{info['category']}模组终止遍历，检测到异常的name，"
        #                      f"可能包含特殊字符: {current_code}, 路径为：{path}")
        if code_format in existing_codes:
            # current_code = current_code + "_Second"
            self.emit_msgs(f"{info['blockId']}_{info['index']}_{info['category']}模组终止遍历，"
                           f"检测到重复的name: {current_code}, 路径为：{path}")
            self.emit_error(f"{info['blockId']}_{info['index']}_{info['category']}模组终止遍历，"
                            f"检测到重复的name: {current_code}, 路径为：{path}")
            raise ValueError(f"{info['blockId']}_{info['index']}_{info['category']}模组终止遍历，"
                             f"检测到重复的name: {current_code}, 路径为：{path}")
        existing_codes.add(code_format)  # 添加当前code到集合
        # print(path, info)
        self.emit_msgs(f"{path}:{info}")
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

            # 找到第二个 / 的索引
            second_slash_index = path.find('/', path.find('/') + 1)
            # 截取第二个 / 之后的部分
            node_path = path[second_slash_index:]
            if node_path in read_enable_paths:  # 如果该path存在于read_enable_paths
                read_enable = read_enable_paths[node_path]

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
            'name': node_name,
            'NodePath': path,

            # attributes
            'NodeID': node.nodeid.to_string(),
            'NodeClass': node_class.value,
            # 'DisplayName': (await node.read_display_name()).Text,
            # 'Description': description,
            # 'WriteMask': int(0),
            # 'UserWriteMask': int(0),
            'value': value,
            'DecimalPoint': decimal_point,
            'DataType': int(var_type),
            'DataTypeString': var_type_str,
            'ArrayDimensions': array_dimensions,
            # 'StatusCode': 0,
            # 'SourceTimestamp': 0,
            # 'SourcePicoseconds': 0,
            # 'ServerTimestamp': 0,
            # 'ServerPicoseconds': 0,

            # references
            # 'Reference': reference,
            # relate with mqtt and opcua
            "blockId": current_block_id,
            "index": current_index,
            "category": current_category,
            "code": current_code,

            # subscription and publish option
            "opcua_subscribe": opcua_sub,  #
            "mqtt_publish": False,  #

            # period read option and information
            "read_enable": read_enable,
            "read_period": read_period,
            "read_time": 0,
            "return_time": 0,

            # s7 option
            "s7_db": 0,
            "s7_start": 0,
            "s7_bit": 0,
            "s7_size": var_type_size,

            # auto clear enable
            "timed_clear": False,
            "timed_clear_time": 1000,

            'children': children
        }

    async def browse_gather(self, root_node: Node, base_path: str, var_obj_flag: int, read_enable_paths: dict):
        """
        入口函数：启动优化后的浏览过程
        """
        self.existing_codes = set()  # 新增重复code检测集合
        # 初始化队列
        root_name = (await root_node.read_browse_name()).Name
        root_path = self._generate_path(base_path, root_name)
        self.processing_queue.append((root_node, root_path, None))

        # 批量处理循环
        while self.processing_queue:
            current_batch = self._get_next_batch()
            await self._process_batch(current_batch, var_obj_flag, read_enable_paths)

        # # 构建完整树形结构
        return self._build_tree_structure(root_path)

    async def _process_batch(self, batch, var_obj_flag, read_enable_paths):
        """ 处理单个节点批次 """
        tasks = []
        for node, path, parent in batch:
            task = self._process_node(node, path, parent, var_obj_flag, read_enable_paths)
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 处理结果
        for result in results:
            if isinstance(result, Exception):
                self._log_error(f"节点处理异常: {str(result)}")
                continue

            node_info, children = result
            if not node_info:
                continue

            # 注册节点信息
            self.node_registry[node_info['NodePath']] = node_info
            if node_info['NodePath'] in self.parent_child_map:
                self.parent_child_map[node_info['NodePath']].extend(children)
            else:
                self.parent_child_map[node_info['NodePath']] = children

            # 子节点入队
            for child_node, child_path in children:
                self.processing_queue.append((child_node, child_path, node_info['NodePath']))

    async def _process_node(self, node: Node, path: str, parent_path: str, var_obj_flag: int, read_enable_paths: dict):
        """ 单个节点的完整处理流程 """
        async with self.semaphore:
            try:
                # Step 1: 获取基础属性
                # browse_name, node_class = await asyncio.gather(
                #     node.read_browse_name(),
                #     node.read_node_class()
                # )
                # node_name = browse_name.Name.replace('[', '').replace(']', '')
                if not GlobalVar.get_browse_var_state():
                    return
                node_name = (await node.read_browse_name()).Name
                node_name = str.replace(node_name, '[' or ']', '')
                if is_target_format(node_name):
                    node_class = ua.NodeClass.Object
                else:
                    node_class = ua.NodeClass.Variable
                current_path = self._generate_path(parent_path, node_name) if parent_path else path
                current_block_id = 0
                current_index = 0
                current_category = ""
                current_code = ""
                if node_class in [ua.NodeClass.Object]:
                    info = path_2info(current_path)
                    current_block_id = info["blockId"]
                    current_index = info["index"]
                    current_category = info["category"]
                    current_code = info["name"]
                    code_format = f"{current_block_id}_{current_index}_{current_category}_{current_code}"
                    # if not check_string_regex(current_code):
                    #     self.emit_error(f"{info['blockId']}_{info['index']}_{info['category']}模组终止遍历，检测到异常的name，"
                    #                     f"可能包含特殊字符: {current_code}, 路径为：{path}")
                    #     raise ValueError(f"{info['blockId']}_{info['index']}_{info['category']}模组终止遍历，检测到异常的name，"
                    #                      f"可能包含特殊字符: {current_code}, 路径为：{path}")
                    if code_format in self.existing_codes:
                        # current_code = current_code + "_Second"
                        self.emit_msgs(f"{info['blockId']}_{info['index']}_{info['category']}模组终止遍历，"
                                       f"检测到重复的name: {current_code}, 路径为：{path}")
                        self.emit_error(f"{info['blockId']}_{info['index']}_{info['category']}模组终止遍历，"
                                        f"检测到重复的name: {current_code}, 路径为：{path}")
                        raise ValueError(f"{info['blockId']}_{info['index']}_{info['category']}模组终止遍历，"
                                         f"检测到重复的name: {current_code}, 路径为：{path}")
                    self.existing_codes.add(code_format)  # 添加当前code到集合
                    # print(path, info)
                    self.emit_msgs(f"{path}:{info}")

                # Step 2: 构建节点信息骨架
                node_info = {
                    'name': node_name,
                    'NodePath': current_path,
                    'children': [],
                    'NodeID': node.nodeid.to_string(),
                    'NodeClass': node_class.value,
                    # 'DisplayName': "",
                    # 'Description': None,
                    'value': None,
                    'DecimalPoint': 3,
                    'DataType': 0,
                    'DataTypeString': 0,
                    "blockId": current_block_id,
                    "index": current_index,
                    "category": current_category,
                    "code": current_code,
                    'ArrayDimensions': 0,
                    # subscription and publish option
                    "opcua_subscribe": False,  #
                    "mqtt_publish": False,  #

                    # period read option and information
                    "read_enable": False,
                    "read_period": 20,
                    "read_time": 0,
                    "return_time": 0,

                    # s7 option
                    "s7_db": 0,
                    "s7_start": 0,
                    "s7_bit": 0,
                    "s7_size": 0,

                    # auto clear enable
                    "timed_clear": False,
                    "timed_clear_time": 1000,
                }

                # Step 3: 变量节点特殊处理
                if node_class == ua.NodeClass.Variable:
                    await self._enrich_variable_node(node, current_path, node_info, read_enable_paths)

                # Step 4: 获取并过滤子节点
                children = await self._get_filtered_children(node, current_path, var_obj_flag)

                return node_info, children
            except Exception as e:
                return None, []

    async def _enrich_variable_node(self, node: Node, current_path: str, node_info: dict, read_enable_paths: dict):
        """ 增强变量节点信息 """
        try:
            if not GlobalVar.get_browse_var_state():
                return
            # 并发读取变量属性
            data_type_task = node.read_data_type_as_variant_type()
            array_dim_task = node.read_array_dimensions()
            desc_task = node.read_description()
            display_name_task = node.read_display_name()

            results = await asyncio.gather(
                data_type_task, array_dim_task, desc_task, display_name_task,
                return_exceptions=True
            )

            # 路径处理
            info = path_2info(current_path)
            self.emit_msgs(f"{current_path}:{info}")

            current_block_id = info["blockId"]
            current_index = info["index"]
            current_category = info["category"]
            current_code = info["name"]
            code_format = f"{current_block_id}_{current_index}_{current_category}_{current_code}"
            # if not check_string_regex(current_code):
            #     self.emit_error(f"{info['blockId']}_{info['index']}_{info['category']}模组终止遍历，检测到异常的name，"
            #                     f"可能包含特殊字符: {current_code}, 路径为：{path}")
            #     raise ValueError(f"{info['blockId']}_{info['index']}_{info['category']}模组终止遍历，检测到异常的name，"
            #                      f"可能包含特殊字符: {current_code}, 路径为：{path}")
            if code_format in self.existing_codes:
                # current_code = current_code + "_Second"
                self.emit_msgs(f"{info['blockId']}_{info['index']}_{info['category']}模组终止遍历，"
                               f"检测到重复的name: {current_code}, 路径为：{current_path}")
                self.emit_error(f"{info['blockId']}_{info['index']}_{info['category']}模组终止遍历，"
                                f"检测到重复的name: {current_code}, 路径为：{current_path}")
                raise ValueError(f"{info['blockId']}_{info['index']}_{info['category']}模组终止遍历，"
                                 f"检测到重复的name: {current_code}, 路径为：{current_path}")
            self.existing_codes.add(code_format)  # 添加当前code到集合

            # 处理变量属性
            var_type = results[0].value if not isinstance(results[0], Exception) else ua.VariantType.Null
            var_type_str = 'Null'
            var_type_size = 0
            value = 0
            decimal_point = 0
            try:
                var_type_str = ua_data_type_to_string(ua.VariantType(var_type))
                var_type_size = ua_data_type_size(ua.VariantType(var_type))
            except ua.UaError:
                print(f'无法确定节点变量类型 {current_path}')
                self.emit_msgs(f'无法确定节点变量类型 {current_path}')
            array_dim = results[1][0] if not isinstance(results[1], Exception) else 0
            description = results[2].Text if not isinstance(results[2], Exception) else None
            display_name = results[3].Text if not isinstance(results[3], Exception) else ""
            # 找到第二个 / 的索引
            second_slash_index = current_path.find('/', current_path.find('/') + 1)
            # 截取第二个 / 之后的部分
            node_path = current_path[second_slash_index:]
            read_enable = False
            if node_path in read_enable_paths:  # 如果该path存在于read_enable_paths
                read_enable = read_enable_paths[node_path]

            if var_type_str == 'structure' or array_dim > 0:
                value = 0
            else:
                try:
                    value = (await node.read_value())
                    if value == '':
                        value = ' '
                except ua.UaError:
                    value = 0

            if var_type_str == "float" or var_type_str == "double":
                decimal_point = count_decimal_places(value)
                if decimal_point == 0:
                    decimal_point = 3
            # 构建变量信息
            node_info.update({
                'NodeID': node.nodeid.to_string(),
                # 'DisplayName': display_name,
                # 'Description': description,
                # 'WriteMask': int(0),
                # 'UserWriteMask': int(0),
                'value': value,
                'DecimalPoint': decimal_point,
                'DataType': int(var_type),
                'DataTypeString': var_type_str,
                "blockId": current_block_id,
                "index": current_index,
                "category": current_category,
                "code": current_code,
                'ArrayDimensions': array_dim,
                # 'StatusCode': 0,
                # 'SourceTimestamp': 0,
                # 'SourcePicoseconds': 0,
                # 'ServerTimestamp': 0,
                # 'ServerPicoseconds': 0,

                # references
                # 'Reference': reference,
                # relate with mqtt and opcua


                # subscription and publish option
                "opcua_subscribe": False,  #
                "mqtt_publish": False,  #

                # period read option and information
                "read_enable": read_enable,
                "read_period": 20,
                "read_time": 0,
                "return_time": 0,

                # s7 option
                "s7_db": 0,
                "s7_start": 0,
                "s7_bit": 0,
                "s7_size": var_type_size,

                # auto clear enable
                "timed_clear": False,
                "timed_clear_time": 1000,
            })
        except Exception as e:
            self._log_error(f"变量节点处理失败: {node_info['NodePath']} - {str(e)}")

    async def _get_filtered_children(self, node: Node, current_path: str, var_obj_flag: int):
        """ 获取并过滤子节点 """
        try:
            children = await node.get_children()
            valid_children = []

            for child in children:
                child_name = (await child.read_browse_name()).Name
                child_class = await child.read_node_class()

                if child_class == ua.NodeClass.Object:
                    if not is_target_format(child_name) and var_obj_flag != 0:
                        continue
                valid_children.append((
                    child,
                    self._generate_path(current_path, child_name)
                ))

            return valid_children
        except Exception as e:
            self._log_error(f"获取子节点失败: {current_path} - {str(e)}")
            return []

    def _get_next_batch(self):
        """ 动态调整批次大小 """
        batch = []
        for _ in range(min(self.batch_size, len(self.processing_queue))):
            batch.append(self.processing_queue.popleft())
        return batch

    def _generate_path(self, parent_path: str, node_name: str) -> str:
        """ 生成标准化路径（带缓存优化） """
        if not parent_path:
            return f'/{node_name}'

        cache_key = (parent_path, node_name)
        if cache_key in self.path_cache:
            return self.path_cache[cache_key]

        new_path = f"{parent_path}/{node_name}"
        self.path_cache[cache_key] = new_path
        return new_path

    def _build_tree_structure(self, root_path: str):
        """ 构建完整的树形结构 """

        def recursive_builder(path):
            node = copy.deepcopy(self.node_registry.get(path))
            if not node:
                return None

            children_paths = [child[1] for child in self.parent_child_map.get(path, [])]
            node['children'] = [recursive_builder(p) for p in children_paths]
            node['children'] = [c for c in node['children'] if c is not None]
            return node

        return recursive_builder(root_path)

    def _log_error(self, message):
        """ 统一错误日志处理 """
        log.warning(message)

    async def browse_pra(self, node: Node, path: str, read_enable_paths, existing_codes=None):
        """
        2_0_pra 节点下数据单独处理
        """
        # browse child node and append to children list
        if existing_codes is None:
            existing_codes = set()  # 初始化code集合
        if not GlobalVar.get_browse_var_state():
            return
        node_children = await node.get_children()
        children = []
        decimal_point = 0
        for child in node_children:
            child_name = (await child.read_browse_name()).Name
            child_class = await child.read_node_class()
            if child_class == ua.NodeClass.Object:
                if is_target_format(child_name):  # valid variable module
                    # 设置子节点的 read_enable 属性为 True
                    children.append(await self.browse_pra(child, name_2path(path, child_name), read_enable_paths, existing_codes))  #
                else:
                    continue  # invalid variable module
            elif child_class == ua.NodeClass.Variable:
                children.append(await self.browse_pra(child, name_2path(path, child_name), read_enable_paths, existing_codes))

        # current node information
        none_tmp = 0
        value = 0
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
        # if not check_string_regex(current_code):
        #     self.emit_error(f"{info['blockId']}_{info['index']}_{info['category']}模组终止遍历，检测到异常的name，"
        #                     f"可能包含特殊字符: {current_code}, 路径为：{path}")
        #     raise ValueError(f"{info['blockId']}_{info['index']}_{info['category']}模组终止遍历，检测到异常的name，"
        #                      f"可能包含特殊字符: {current_code}, 路径为：{path}")
        if code_format in existing_codes:
            # current_code = current_code + "_Second"
            self.emit_msgs(f"{info['blockId']}_{info['index']}_{info['category']}模组终止遍历，"
                           f"检测到重复的name: {current_code}, 路径为：{path}")
            self.emit_error(f"{info['blockId']}_{info['index']}_{info['category']}模组终止遍历，"
                            f"检测到重复的name: {current_code}, 路径为：{path}")
            raise ValueError(f"{info['blockId']}_{info['index']}_{info['category']}模组终止遍历，"
                             f"检测到重复的name: {current_code}, 路径为：{path}")
        existing_codes.add(code_format)  # 添加当前code到集合
        # print(path, info)
        self.emit_msgs(f"{path}:{info}")
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
                # self.emit_msgs(f'无法确定节点变量类型:{path}')

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

            if path in read_enable_paths:  # 如果该path存在于read_enable_paths
                read_enable = read_enable_paths[path]

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
            'name': node_name,
            'NodePath': path,

            # attributes
            'NodeID': node.nodeid.to_string(),
            'NodeClass': node_class.value,
            # 'DisplayName': (await node.read_display_name()).Text,
            # 'Description': description,
            # 'WriteMask': int(0),
            # 'UserWriteMask': int(0),
            'value': value,
            'DecimalPoint': decimal_point,
            'DataType': int(var_type),
            'DataTypeString': var_type_str,
            'ArrayDimensions': array_dimensions,
            # 'StatusCode': 0,
            # 'SourceTimestamp': 0,
            # 'SourcePicoseconds': 0,
            # 'ServerTimestamp': 0,
            # 'ServerPicoseconds': 0,

            # references
            # 'Reference': reference,
            # relate with mqtt and opcua
            "blockId": current_block_id,
            "index": current_index,
            "category": current_category,
            "code": current_code,

            # subscription and publish option
            "opcua_subscribe": opcua_sub,  #
            "mqtt_publish": False,  #

            # period read option and information
            "read_enable": read_enable,
            "read_period": read_period,
            "read_time": 0,
            "return_time": 0,

            # s7 option
            "s7_db": 0,
            "s7_start": 0,
            "s7_bit": 0,
            "s7_size": var_type_size,

            # auto clear enable
            "timed_clear": False,
            "timed_clear_time": 1000,

            'children': children
        }
