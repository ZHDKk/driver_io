import asyncio
from datetime import datetime
import time
import pandas as pd
from asyncua.common.subscription import Subscription

from asyncua import Client

from logger import log
from parse import json_from_list, s7_datas_parse, datas_parse_o2m
from opcua_link import opcua_linker, SubHandler
from s7_link import s7_linker
from utils.helpers import code2format_str


async def async_cleanup(client, sub):
    if isinstance(sub, Subscription):
        # await sub.delete()
        print('exit subscription')
    if isinstance(client, Client):
        # await client.disconnect()
        print('exit opc client')


def cleanup(client, sub):
    asyncio.run(async_cleanup(client, sub))


class device(object):
    """
    opcua device
    """

    def __init__(self, config: dict, collection_handler):
        # device name and type
        self.name = config['name']
        self.link_type = config['link']
        self.M2O_All = False
        self.O2M_All = False

        # opcua linker
        self.linker = opcua_linker(config) if config['link'] == 'opcua' else s7_linker(config)

        # module description in opcua device
        self.module = []
        self.module_number = 0
        self.reconnect_timer = 0

        # data structure for opcua variable
        self.VarTree = None  # big tree for nodes
        self.VarList = []  # self define dictionary for nodes
        self.code_to_node = {}
        self.VarDf = pd.DataFrame()  # dataframe for nodes
        self.VarNumber = 0

        # subscription description
        self.VarSubscription = []  # subscription nodes list, point to tree or list
        self.Subscription_Collection = collection_handler  # callback data handler
        self.Subscription_Nodes_Number = 0

        # period read variable block
        self.ReadBlock = []
        self.TempReadBlock = []  # 创建一个临时读的block
        self.ReadBlock_Number = 0
        self.Read_Failure_Count = 0
        self.Read_Times = 0

        # request event
        self.RequestEvent = []
        self.RequestEvent_Number = 0

        # timed clear server
        self.TimedClear = []
        self.TimedClear_Number = 0

        # opcua device status information
        self.subscription_state = False
        self.loading = False
        self.connecting = False

        # 数据库相关
        # 生成数据批次
        self.db_module_name = ''
        self.data_batch = []

    def create_read_block(self):
        """
        create read block
        """
        self.ReadBlock = []  # clear read block

        # filter reading enable list
        tmp = list(filter(lambda x: x['read_enable'] is True, self.VarList))

        module_key = ['blockId', 'index', 'category']
        key = ['code', 'NodeID', 'read_period', 'read_time', 'return_time']
        s7 = ['s7_db', 's7_start', 's7_size']
        for n in tmp:  # extract key:value and create reading block
            # extract key in dict
            module = {k: v for k, v in n.items() if k in module_key}
            item = {k: v for k, v in n.items() if k in key}
            s7_item = {k: v for k, v in n.items() if k in s7}

            # add node location information to list
            index = self.VarList.index(n)
            # tree_node = find_path(self.VarTree, n['path'])
            # list_node = self.VarList[index]
            list_node = self.code_to_node.get(code2format_str(module['blockId'], module['index'],
                                                              module['category'], item['code']))
            item['module'] = module
            item['ListIndex'] = index
            # item['TreeNode'] = tree_node
            item['ListNode'] = list_node
            item['s7'] = s7_item

            # add to read block[]
            self.ReadBlock.append(item)
        self.ReadBlock_Number = len(self.ReadBlock)

    def create_timed_clear_block(self):
        """
        create timed clear block
        """
        self.TimedClear = []
        current_time = int(time.time() * 1000)

        # filter reading enable list
        tmp = list(filter(lambda x: x['timed_clear'] is True, self.VarList))

        # extract key:value and create reading list
        module_key = ['blockId', 'index', 'category']
        key = ['code', 'NodeID', 'timed_clear_time']
        s7 = ['s7_db', 's7_start', 's7_bit', 's7_size']
        for n in tmp:
            # extract key in dict
            module = {k: v for k, v in n.items() if k in module_key}
            item = {k: v for k, v in n.items() if k in key}
            s7_item = {k: v for k, v in n.items() if k in s7}

            # add node location information to list
            index = self.VarList.index(n)
            # tree_node = find_path(self.VarTree, n['path'])
            # list_node = self.VarList[index]
            list_node = self.code_to_node.get(code2format_str(module['blockId'], module['index'],
                                                              module['category'], item['code']))
            item['module'] = module
            item['ListIndex'] = index
            # item['TreeNode'] = tree_node
            item['ListNode'] = list_node
            item['s7'] = s7_item
            item['FalseTIme'] = current_time

            # add to read list
            self.TimedClear.append(item)
        self.TimedClear_Number = len(self.TimedClear)

    async def load_variable_list(self):
        """
        load variable list from config file. create tree, list data structure, read block and timed clear block
        """
        # create variable dataframe with config file
        try:
            self.VarDf = pd.read_csv(f'./config files/{self.name}.csv', encoding='utf-8')
        except:
            try:
                self.VarDf = pd.read_csv(f'./config files/{self.name}.csv', encoding='utf-8-sig')
            except:
                try:
                    self.VarDf = pd.read_csv(f'./config files/{self.name}.csv', encoding='gbk')
                except:
                    log.error(f'Failure to load {self.name}.csv file.')
                    return False
        # print(self.VarDf)

        # create tree and list data structure
        # dict_tmp = tree_to_dict(big_tree, all_attrs=True)
        # try:
        #     self.VarTree = dataframe_to_tree(self.VarDf)
        # except:
        #     log.error(f'Failure to convert {self.name}.csv file to tree.')
        #     return False

        try:
            self.VarList = self.VarDf.apply(lambda row: dict(row), axis=1).tolist()
            self.VarNumber = len(self.VarList)
            self.code_to_node = {f"{item['blockId']}_{item['index']}_{item['category']}_{item['code']}": item for item in self.VarList}
            # pprint.pprint(self.VarList)
        except:
            self.VarNumber = 0
            log.error(f'Failure to convert {self.name}.csv file to list.')
            return False
        # node_tree = tree_to_nested_dict(self.VarTree, all_attrs=True)
        # pprint.pprint(node_tree)

        # create module information
        module = list(filter(lambda x: x['NodeClass'] == 1 and (x['index'] + x['blockId']) > 0, self.VarList))
        for m in module:
            self.module.append({'blockId': m['blockId'], 'index': m['index'], 'category': m['category']})
        self.module_number = len(self.module)
        if self.module_number == 0:
            return False

        # create read block
        self.create_read_block()
        # pprint.pprint(self.ReadBlock)
        # print(f'Reading nodes of {self.name} is {self.ReadBlock_Number}.')

        # create timed clear block
        self.create_timed_clear_block()
        # pprint.pprint(self.TimedClear)
        # print(f'Timed clear nodes of {self.name} is {self.TimedClear_Number}.')

        self.loading = True
        return True

    async def connect(self):
        # connect to opcua device
        self.connecting = await self.linker.link()

    async def disconnect(self):
        # disconnect to opcua device
        await self.linker.unlink()
        self.connecting = False

    async def get_connecting_state(self):
        self.connecting = await self.linker.get_link_state()

    async def subscribe(self):
        """
        subscribe nodes
        """
        if not self.VarList or self.connecting is False or self.link_type == 's7':  # or self.Subscription_Nodes_Number > 0:
            return self.subscription_state

        list_tmp = list(filter(lambda x: x['opcua_subscribe'] is True, self.VarList))
        if not list_tmp:
            return self.subscription_state

        # create subscription callback handler
        handle = SubHandler(self.name, self.Subscription_Collection)
        self.linker.subscription = await self.linker.client.create_subscription(0, handle)

        # filter subscription variable list
        self.VarSubscription = []
        sub_nodes = []
        for n in list_tmp:
            index = self.VarList.index(n)
            # tree_node = find_path(self.VarTree, n['path'])
            # list_node = self.VarList[index]
            sub_nodes.append(n["NodeID"])
            self.VarSubscription.append({'ListIndex': index, 'ListNode': n})
            # self.VarSubscription.append({'ListIndex': index, 'TreeNode': tree_node, 'ListNode': list_node})

        # subscribe variable list
        self.Subscription_Nodes_Number = len(sub_nodes)
        # print("opcua subscription nodes:", self.subscription_count)
        # pprint.pprint(opcua_nodes)
        try:
            await self.linker.subscription_variables(sub_nodes)
            self.subscription_state = True
        except:
            self.subscription_state = False

        return self.subscription_state

    async def read_variable_block(self, mqtt_t, node_infos):
        """
        read variable value from opcua device
        node_infos:是否实时读变量，如果不为空则执行单次读  20250314
        """
        start_time = int(time.time() * 1000)
        # prepare reading nodes and parsing buffer for reading opcua
        nodes = []  # read nodes list [NodeID，...]
        O2M_list = []  # parse data list [{'module':{},'list':[{},{},...]},...]
        msg = []  # error message list
        if len(node_infos) == 0:  # 实时读变量
            try:
                for b in self.ReadBlock:  # scan read block list
                    nodes.append(b['NodeID'])
                    mt = {'module': b['module'], 'list': []}
                    if mt not in O2M_list:
                        O2M_list.append(mt)
                # pprint.pprint(nodes)
                # pprint.pprint(O2M_list)
            except Exception as e:
                log.warning(f'Failure to create reading nodes list: {e}.')
                return False
            # read value of nodes from opcua
            datas = await self.linker.read_multi_variables(nodes, timeout=1.5)
            read_time = int(time.time() * 1000)
            if not datas:
                log.warning(
                    f'Failure to read opcua {self.name},{self.linker.uri}, using time {read_time - start_time}ms.')
                return False
            self.Read_Times += 1
            # print(datas)
            # print(self.ReadBlock)
            # parse reading datas, and save single variable to list of corresponding module
            for index, _ in enumerate(self.ReadBlock):
                try:
                    m = list(filter(lambda x: x['module'] == self.ReadBlock[index]['module'], O2M_list))[0]
                    # datas_parse(self, self.ReadBlock[index]['TreeNode'], self.ReadBlock[index]['ListNode'],
                    #             datas[index],
                    #             False, None, self.O2M_All, m['list'], int(time.time() * 1000), msg)
                    datas_parse_o2m(self, self.ReadBlock[index]['ListNode'], datas[index], self.O2M_All, m['list'],
                                    int(time.time() * 1000), msg)

                    # print parse error message
                    # 2024/12/5 临时关闭打印
                    for s in msg:
                        # log.warning(s)
                        print(s)
                except Exception as e:
                    log.warning(f'{e}Failure to parse {nodes[index]}{datas[index]}.')
            parse_time = int(time.time() * 1000)

            # pack module data and publish to mqtt
            for md in O2M_list:
                if md['list']:
                    mqtt_frame = json_from_list(md)
                    if mqtt_t.connecting is True:
                        mqtt_t.publish(mqtt_t.pub_drv_data, mqtt_frame)

            end_time = int(time.time() * 1000)

            current_time = str(datetime.now().time())[:-7]  # collection time
            # print(current_time, f'O2M {self.name} Timing: module x{len(O2M_list)},reading {read_time - start_time},'
            #                     f'parsing {parse_time - read_time},publish {end_time - parse_time},'
            #                     f'Total is {end_time - start_time}ms')
        else:  # 单点读
            self.create_temp_read_block(node_infos)
            for node_info in node_infos:
                nodes.append(node_info['NodeID'])
                mt = {'module': node_info['module'], 'list': []}
                if mt not in O2M_list:
                    O2M_list.append(mt)
            datas = await self.linker.read_multi_variables(nodes, timeout=1.5)
            read_time = int(time.time() * 1000)
            if not datas:
                log.warning(
                    f'Failure to read opcua {self.name},{self.linker.uri}, using time {read_time - start_time}ms.')
                return False
            for index, _ in enumerate(self.TempReadBlock):
                try:
                    m = list(filter(lambda x: x['module'] == self.TempReadBlock[index]['module'], O2M_list))[0]
                    datas_parse_o2m(self, self.TempReadBlock[index]['ListNode'], datas[index], self.O2M_All, m['list'],
                                    int(time.time() * 1000), msg)
                    # datas_parse(self, self.TempReadBlock[index]['TreeNode'], self.TempReadBlock[index]['ListNode'],
                    #             datas[index],
                    #             False, None, self.O2M_All, m['list'], int(time.time() * 1000), msg)

                    # print parse error message
                    # 2024/12/5 临时关闭打印
                    for s in msg:
                        # log.warning(s)
                        print(s)
                except Exception as e:
                    log.warning(f'{e} Failure to parse {nodes[index]}{datas[index]}.')
                    return False
        return True

    def create_temp_read_block(self, node_infos):
        """
        20250314创建一个临时读的block
        """
        self.TempReadBlock.clear()
        module_key = ['blockId', 'index', 'category']
        key = ['code', 'NodeID', 'read_period', 'read_time', 'return_time']
        s7 = ['s7_db', 's7_start', 's7_size']
        for node_info in node_infos:
            module = node_info['module']
            tmp = list(filter(lambda x: x['blockId'] == module['blockId'] and x['index'] == module['index']
                                        and x['category'] == module['category'] and x['NodeID'] == node_info['NodeID'], self.VarList))
            for n in tmp:  # extract key:value and create reading block
                # extract key in dict
                module = {k: v for k, v in n.items() if k in module_key}
                item = {k: v for k, v in n.items() if k in key}
                s7_item = {k: v for k, v in n.items() if k in s7}

                # add node location information to list
                index = self.VarList.index(n)
                # tree_node = find_path(self.VarTree, n['path'])
                list_node = self.code_to_node.get(code2format_str(module['blockId'], module['index'],
                                                                  module['category'], item['code']))
                # list_node = self.VarList[index]
                item['module'] = module
                item['ListIndex'] = index
                # item['TreeNode'] = tree_node
                item['ListNode'] = list_node
                item['s7'] = s7_item

                # add to read block[]
                self.TempReadBlock.append(item)

    async def read_variable_block_vs7(self, mqtt_t):
        """
        read variable value from plc device via s7
        """
        start_time = int(time.time() * 1000)
        # prepare reading nodes and parsing buffer for reading opcua
        try:
            s7_nodes = []
            nodes = []  # read nodes list [NodeID，...]
            O2M_list = []  # parse data list [{'module':{},'list':[{},{},...]},...]
            msg = []  # error message list
            for b in self.ReadBlock:  # scan read block list
                s7_nodes.append(b['s7'])
                nodes.append(b['NodeID'])
                mt = {'module': b['module'], 'list': []}
                if mt not in O2M_list:
                    O2M_list.append(mt)
            # pprint.pprint(nodes)
            # pprint.pprint(O2M_list)
        except Exception as e:
            log.warning(f'Failure to create reading nodes list: {e}.')
            return False
        # print(s7_nodes)
        # read value of nodes from plc device via s7

        datas = await self.linker.read_multi_variables(s7_nodes, timeout=1.5)
        read_time = int(time.time() * 1000)
        if not datas:
            print(f'Failure to read s7 {self.name}, using time {read_time - start_time}ms.')
            log.warning(f'Failure to read s7 {self.name},{self.linker.uri}, using time {read_time - start_time}ms.')
            return False
        self.Read_Times += 1
        # pprint.pprint(datas)

        # parse reading datas, and save single variable to list of corresponding module
        for index, _ in enumerate(self.ReadBlock):
            try:
                m = list(filter(lambda x: x['module'] == self.ReadBlock[index]['module'], O2M_list))[0]
                s7_datas_parse(self, self.ReadBlock[index]['ListNode'], datas[index],
                               False, None, self.O2M_All, m['list'], int(time.time() * 1000), msg)
                # print parse error message
                for s in msg:
                    log.info(s)
            except:
                # print(str(datetime.now().time())[:-7], f'Failure to parse {nodes[index]}{datas[index]}.')
                log.warning(f'Failure to parse {nodes[index]}{datas[index]}.')
        parse_time = int(time.time() * 1000)

        # pack module data and publish to mqtt
        for md in O2M_list:
            if mqtt_t.connecting is True and md['list']:
                mqtt_frame = json_from_list(md)
                if mqtt_frame:
                    mqtt_t.publish(mqtt_t.pub_drv_data, mqtt_frame)
                # print(md)
        end_time = int(time.time() * 1000)

        current_time = str(datetime.now().time())[:-7]  # collection time
        # print(current_time, f'O2M {self.name} Timing: module x{len(O2M_list)},reading {read_time - start_time},'
        #                     f'parsing {parse_time - read_time},publish {end_time - parse_time},'
        #                     f'Total is {end_time - start_time}ms')
        # log.info('O2M %s Timing: module x%d,reading %d,parsing %d,publish %d,Total is %dms', self.name, len(O2M_list),
        #          read_time - start_time, parse_time - read_time, end_time - parse_time, end_time - start_time)
        return True

    async def device_manager(self, link):
        """
        opcua device manager
        """
        # reconnect or disconnect device
        if self.connecting is True:  # connecting, disconnect
            if not link or not self.loading:  # disconnect
                await self.disconnect()
                # print(f'Disconnect {dev.name}, {dev.connecting}.')
                log.warning(f'Disconnect {self.name}, {self.connecting}.')
        elif link and self.loading:  # disconnecting, reconnect
            self.Read_Times = 0
            if self.link_type == 'opcua':
                await self.linker.new_client()
                await self.connect()
                await self.subscribe()
            else:
                await self.connect()
            print(f'Connect {self.name}, {self.connecting}.')
            log.info(f'Connect {self.name}, {self.connecting}.')

    async def timed_clear_safety_variable(self):
        """
        # clear timed clear block
        """
        current_time = int(time.time() * 1000)
        M2O_list = []
        for b in self.TimedClear:
            if self.Read_Times < 3 or b['ListNode']["value"] is False:
                b['FalseTIme'] = current_time
            elif current_time - b['FalseTIme'] >= b['timed_clear_time']:
                if self.link_type == 'opcua':
                    M2O_list.append({'node_id': b['NodeID'], 'datatype': b['ListNode']['DataType'], 'value': False})
                    log.info(f'Timed Clear {b["NodeID"]}, {current_time - b["FalseTIme"]} > {b["timed_clear_time"]}')
                elif self.link_type == 's7':
                    M2O_list.append({'s7_db': b["s7"]["s7_db"], 's7_start': b["s7"]["s7_start"],
                                     's7_bit': b["s7"]["s7_bit"], 's7_size': b["s7"]["s7_size"], 'value': False})
                    log.info(f'Timed Clear {b["NodeID"]} via s7, '
                                f'{current_time - b["FalseTIme"]} > {b["timed_clear_time"]}')
                b['ListNode']["value"] = False
                b['FalseTIme'] = current_time

        if M2O_list:
            if self.link_type == 'opcua':
                await self.linker.write_multi_variables(M2O_list, timeout=0.2)
            elif self.link_type == 's7':
                await self.linker.write_multi_variables(M2O_list)

    async def close(self):
        # await self.linker.subscription.delete()
        await self.disconnect()
        self.VarSubscription = []
        log.info(f"opcua device {self.name} (uri: {self.uri},main_node: {self.main_node}) is removed")
        return True
