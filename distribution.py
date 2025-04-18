import asyncio
import json
import os
import subprocess
import sys
import time
from datetime import datetime
import pandas as pd
from mqtt_link import mqtt_linker
from device import device
from logger import log
from data_parse import nested_dict_2list, json_from_list, datas_parse_m2o, data_to_list, datas_parse_o2m
from recipe import request_recipe_handle_gather_link
from utils.helpers import code2format_str, save_config_file
from utils.time_util import get_current_time


def get_request_nodes(dev, node, request_update, request_update_id, request_update_result):
    """
    get request information with variable name in node tree
    :param node: node
    :return: request information dictionary
    """
    req_dict = {'request': dev.code_to_node.get(code2format_str(node['blockId'], node['index'], node['category'],
                                                                node['code']+"_"+request_update)),
                'id': dev.code_to_node.get(code2format_str(node['blockId'], node['index'], node['category'],
                                                           node['code']+"_"+request_update_id)),
                'result': dev.code_to_node.get(code2format_str(node['blockId'], node['index'], node['category'],
                                                               node['code']+"_"+request_update_result))}
    return req_dict


async def clear_request_result(dev, req):
    """
    clear request result to server
    :param dev: device object
    :param req: request information dictionary
    :return: result of writing request result to server
    """
    M2O_list = [{'node_id': req['result']["NodeID"], 'datatype': req['result']["DataType"], 'value': 0}]
    await dev.linker.write_multi_variables(M2O_list, 0.1)


class distribution_server(object):
    """
    data distribution server
    """

    def __init__(self):
        # scatter mode and manager
        self.M2O_All = False
        self.O2M_All = False

        # opcua device
        self.ua_device = []

        # mqtt interface
        self.mqtt = None

        # distribution config
        self.config = {}

        self.is_local = True  # 记录是否是本地模式
        # request recipe interface
        self.recipe_request_data = []
        self.recipe_single_module = []
        self.recipe_request_map = {}
        self.single_module_map = {}
        self.RESTART_FLAG = False  #  当前主程序是否重启Flag
        self.browse_proc = None  # 记录遍历变量进程

    async def __aenter__(self):

        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close_opcua_device()
        self.close_mqtt()

    def load_config_file(self):
        """
        load config file of distribution
        """
        # read config file
        try:
            with open(r'./config files/driver config.json', 'r', encoding='utf-8') as file:
                self.config = json.load(file)
                self.is_local = self.config["Control"]["isLocal"]
            # pprint.pprint(self.config)

            # save to drv_config.csv file
            list_data = []
            nested_dict_2list(self.config, list_data, 0)
            df = pd.DataFrame(list_data)
            df.to_csv(f'./config files/drv_config.csv', encoding='utf_8_sig', index=False)
            print('Driver Config file loaded - done.')
            log.info('Driver Config file loaded - done.')
        except FileNotFoundError:
            log.warning('Failure to find ./config files/driver config.json.')
            return
        except json.JSONDecodeError:
            log.warning('Failure to parse ./config files/driver config.json.')
            return
        except Exception as e:
            log.warning(f'Failure to read ./config files/driver config.json.{e}')
            return

    def load_request_file(self):
        """
        load request file of distribution
        """
        # read config file
        try:
            with open(r'./config files/recipe_config.json', 'r', encoding='utf-8') as file:
                recipe_config_data = json.load(file)
                recipe_monitor_info = recipe_config_data['recipe_monitor_info']
                self.recipe_request_data = recipe_monitor_info['recipe_request']
                for mc_module in self.recipe_request_data:
                    mc_mod_info = mc_module['module']
                    key = (mc_mod_info['blockId'], mc_mod_info['index'], mc_mod_info['category'])
                    self.recipe_request_map[key] = mc_module
                self.recipe_single_module = recipe_monitor_info['single_module']
                for module in self.recipe_single_module:
                    mod_info = module['module']
                    key = (mod_info['blockId'], mod_info['index'], mod_info['category'])
                    self.single_module_map[key] = module
            # pprint.pprint(self.recipe_config_data)
            print('Request Config file loaded - done.')
            log.info('Request Config file loaded - done.')
        except FileNotFoundError:
            log.warning('Failure to find ./config files/recipe_config.json.')
            return
        except json.JSONDecodeError:
            log.warning('Failure to parse ./config files/recipe_config.json.')
            return
        except Exception as e:
            log.warning('Failure to read ./config files/recipe_config.json.')
            return

    def find_dev_with_module(self, module):
        """
        find device(PLC) with module, module{0_1_MC}-->device{MC}
        """
        # find opcua device with module information
        for dev in self.ua_device:
            for m in dev.module:
                if m == module:  # match module blockID,index,category
                    return dev
        return None

    def json_data_parse(self, data, dev: device, module):
        """
        json datas parse
        :param data: json data
        :param dev: opcua device object
        :param module: module information (blockId, index, category)
        :return: result of parsing json data
        """
        current_time = str(datetime.now().time())[:-7]  # collection time
        result = {'Device': None, 'Module': None, 'Codes': 0, 'M2O_list': [], 'Nodes': 0, 'ErrMSG': []}

        # find opcua device with module (in data)
        module_tmp = {}
        try:
            # get module information from data frame
            module_tmp = {'blockId': data['blockId'], 'index': data['index'], 'category': data['category']}
            # print(module_tmp)
        except:
            # no module information in data frame, check input variable
            if dev is None or module is None:
                result['ErrMSG'].append('Failure to get device and module information.')
                return result

        if module_tmp:  # data frame include module information
            module = module_tmp
            dev = self.find_dev_with_module(module)
            if dev is None:  # don't find device with module information
                result['ErrMSG'].append(f'Failure to matched device with module {module}.')
                return result

        result['Device'] = dev
        result['Module'] = module
        # print(dev.name, module)

        # code information in data frame
        code_value = {}
        code_count = len(data['list'])
        result['Codes'] = code_count

        # parse code in data frame
        for n in data['list']:
            # find node and list node by code value
            try:
                # list_node = list(filter(lambda x: x['blockId'] == module['blockId'] and x['index'] == module['index']
                #                                   and x['category'] == module['category'] and x['code'] == n['code'],
                #                         dev.VarList))[0]
                # node = find_path(dev.VarTree, list_node['path'])
                list_node = dev.code_to_node.get(code2format_str(module['blockId'], module['index'], module['category'],
                                                                 n['code']))
            except:
                result['ErrMSG'].append(f'Failure to find code in the variable list, {n}.')
                return result

            # get variable value, recursive parse structure data
            try:
                value = n['value']
            except:
                result['ErrMSG'].append(f'Failure to get value from the data frame, {n}.')
                return result

            # datas parse
            # datas_parse(dev, node, list_node, value, self.M2O_All, result['M2O_list'], False, None,
            #             str(datetime.now().time())[:-7], result['ErrMSG'])
            datas_parse_m2o(dev, list_node, value, self.M2O_All, result['M2O_list'],
                            str(datetime.now().time())[:-7], result['ErrMSG'])
            code_value[n['code']] = n['value']

        result['Nodes'] = len(result['M2O_list'])
        print(f'{current_time} Collection Json Frame:{code_count} codes in {module}. Code:Value:{code_value}')
        log.info(f'Collection Json Frame:{code_count} codes in {module}. Code:Value:{code_value}')
        # pprint.pprint(result)
        return result

    async def mqtt_cmd_read(self, data, topic, dev, module, single=True, is_from_plc=False):
        """
        mqtt read command handle for mqtt subscription command
        :param data: data
        :param topic: mqtt topic
        :param dev: opcua device object
        :param module: module information (blockId, index, category)
        :param single: single read or struct read
        :return: None
        """
        result = {'module': module, 'list': []}
        try:
            code_list = data['list']
        except:
            log.warning(f'Failure to get code list from {data} of mqtt frame.')
            self.mqtt.publish(topic + '/reply', json.dumps({'success': False,
                                                            'message': f'Failure to get code list from {data}.'}))
            return
        nodes = []
        read_vars_info = []
        for n in code_list:
            try:
                # list_node = list(filter(lambda x: x['blockId'] == module['blockId'] and x['index'] == module['index']
                #                                   and x['category'] == module['category'] and x['code'] == n['code'],
                #                         dev.VarList))[0]
                list_node = dev.code_to_node.get(code2format_str(module['blockId'], module['index'], module['category'],
                                                                 n['code']))
                # node = find_path(dev.VarTree, list_node['path'])
                nodes.append(list_node)
                read_vars_info.append({'module': module, 'NodeID': list_node["NodeID"]})
            except Exception as e:
                log.warning(f'Failure to find {n} in the list, when mqtt read.')
                self.mqtt.publish(topic + '/reply', json.dumps({'success': False,
                                                                'message': f'Failure to find {n} in the list.'}))
                return
        if is_from_plc and len(read_vars_info) > 0:
            try:
                await dev.read_variable_block(self.mqtt, read_vars_info)
            except Exception as e:
                print(f"从PLC读数据错误{e}")

        # get datas
        for node in nodes:
            try:
                if single is True:  # single read
                    # tree_to_list(node, result['list'], int(time.time() * 1000))
                    data_to_list(node, result['list'], int(time.time() * 1000), dev)
                else:  # struct read
                    result['list'].append({"code": node["code"], "value": node["value"], "dataType": node["DataTypeString"],
                                           "arrLen": node["ArrayDimensions"], "time": int(time.time() * 1000)})
            except:
                log.warning(f'Failure to find {n} in the list, when mqtt read.')
                self.mqtt.publish(topic + '/reply', json.dumps({'success': False,
                                                                'message': f'Failure to find {n} in the list.'}))
                return

        # mqtt publish
        # if self.mqtt.connecting is True and result['list']:
        result.update({'success': True, 'message': 'OK'})
        mqtt_frame = json_from_list(result)
        self.mqtt.publish(topic + '/reply', mqtt_frame, 2)
        log.info(f'MQTT Read {len(code_list)} nodes {code_list}, return {len(result["list"])} variables.')

    async def mqtt_cmd_write(self, frame_id, data, topic):
        """
        write command handle for mqtt subscription command
        :param data: data
        :param topic: mqtt topic
        :param dev: opcua device object
        :param module: module information (blockId, index, category)
        :return: None
        """
        start_time = int(time.time() * 1000)
        success = False
        message = ''

        # parse json datas
        result = self.json_data_parse(data, None, None)
        # pprint.pprint(result)
        parse_time = int(time.time() * 1000)

        # error handle or write opcua handle
        if result['ErrMSG']:  # error message
            for m in result['ErrMSG']:
                # print(str(datetime.now().time())[:-7], m)
                log.warning(m)
                message = message + m + ';'
        elif result['M2O_list']:
            # pprint.pprint(result['M2O_list'])
            if result['Device'].connecting is False:
                message = f'Failure to write opcua {result["Device"].name}, not linked.'
                log.warning(message)
            else:
                if result['Device'].link_type == 'opcua':  # write via opcua
                    success = await result['Device'].linker.write_multi_variables(result['M2O_list'], 0.5)
                    if success is False:  # write opcua failure, read and check
                        success = await result['Device'].linker.check_write_result(result['M2O_list'])
                elif result['Device'].link_type == 's7':  # write via s7
                    success = await result['Device'].linker.write_multi_variables(result['M2O_list'], 0.5)
                else:
                    message = f'Invalid link type: {result["Device"].link_type}.'
                write_time = int(time.time() * 1000)

                if success is True:
                    message = 'OK'
                    device_name = result['Device'].name
                    print(str(datetime.now().time())[:-7],
                          f'M2O {device_name} Timing: parsing {parse_time - start_time},'
                          f'writing {write_time - parse_time},'
                          f'Total is {write_time - start_time}, {message}')
                    log.info(f'M2O {device_name} Timing: parsing {parse_time - start_time},'
                             f'writing {write_time - parse_time}, Total is {write_time - start_time}, {message}')
                elif message == '':
                    message = f'Failure to write {result["Device"].name} via {result["Device"].link_type}.'

        self.mqtt.publish(topic + '/reply', json.dumps({'success': success, 'id': frame_id, 'message': message}))

    async def mqtt_cmd_parse(self, frame_id, data, topic):
        """
        mqtt subscription command handle
        :param data: data
        :param topic: mqtt topic
        :return: None
        """
        # find opcua device and module information
        try:
            module = {'blockId': data['blockId'], 'index': data['index'], 'category': data['category']}
            dev = self.find_dev_with_module(module)
            if dev is None:  # don't find device with module information
                log.warning(f'Failure to match {module} to device.')
                self.mqtt.publish(topic + '/reply', json.dumps({'success': False,
                                                                'message': f'Failure to match {module} to device.'}))
                return
        except:
            log.warning(f'Failure to get device and module information from {data} of mqtt frame.')
            self.mqtt.publish(topic + '/reply', json.dumps({'success': False,
                                                            'message': f'Failure to get device and module from {data}.'}))
            return

        try:
            cmd = data['cmd']
        except:
            cmd = 'write'

        match cmd:
            case 'read':
                log.info(f'接收到Mqtt read指令:{data}')
                print(f'{get_current_time()}:接收到Mqtt read指令:{data}')
                await self.mqtt_cmd_read(data, topic, dev, module)
            case 'read_struct':
                log.info(f'接收到Mqtt read_struct指令:{data}')
                print(f'{get_current_time()}:接收到Mqtt read_struct指令:{data}')
                await self.mqtt_cmd_read(data, topic, dev, module, single=False)
            case 'read_plc':  # 单次从plc读数据，不需要实时刷
                print(f'{get_current_time()}:接收到Mqtt read_plc指令:{data}')
                log.info(f'接收到Mqtt read_plc指令:{data}')
                await self.mqtt_cmd_read(data, topic, dev, module, is_from_plc=True)
            case 'read_plc_struct':
                print(f'{get_current_time()}:接收到Mqtt read_plc_struct指令:{data}')
                log.info(f'接收到Mqtt read_plc_struct指令:{data}')
                await self.mqtt_cmd_read(data, topic, dev, module, single=False, is_from_plc=True)
            case 'write':
                print(f'{get_current_time()}:接收到Mqtt write指令:{data}')
                log.info(f'接收到Mqtt write指令:{data}')
                await self.mqtt_cmd_write(frame_id, data, topic)
            case 'write_recipe':
                print(f'{get_current_time()}:接收到Mqtt write_recipe指令:{data}')
                log.info(f'接收到Mqtt write_recipe指令:{data}')
                try:
                    key = (module["blockId"], module["index"], module["category"])
                    if mc_match := self.recipe_request_map.get(key):  # 如果是MC则直接写配方
                        await self.mqtt_cmd_write(frame_id, data, topic)  # 开始写配方
                    elif match := self.single_module_map.get(key):
                        writable_path = match['recipe_writable_path']
                        recipe_valid_code = match['recipe_valid_code']
                        recipe_valid_info = dev.code_to_node.get(code2format_str(module['blockId'], module['index'],
                                                                                 module['category'], recipe_valid_code))
                        if not dev.code_to_node.get(code2format_str(module['blockId'], module['index'],
                                                                    module['category'], writable_path))["value"]:  # 检查当前模组是否支持下载配方
                            msg = f'{module["blockId"]}-{module["index"]}-{module["category"]}模组当前不支持下载配方'
                            print(f'{get_current_time()}: {msg}')
                            log.info(msg)
                            self.mqtt.publish(topic + '/reply', json.dumps({'success': False,
                                                                            'message': msg}))
                            return
                        else:
                            if await dev.linker.write_multi_variables([{'node_id': recipe_valid_info["NodeID"],
                                                                        'datatype': recipe_valid_info["DataType"],
                                                                        'value': True}],
                                                                      1.5):  # 先把模组的Recipe_Valid’为True
                                await self.mqtt_cmd_write(frame_id, data, topic)  # 开始写配方
                                await dev.linker.write_multi_variables([{'node_id': recipe_valid_info["NodeID"],
                                                                         'datatype': recipe_valid_info["DataType"],
                                                                         'value': False}],
                                                                       1.5)  # 再把模组的Recipe_Valid’为False
                except Exception as e:
                    log.warning(f'向模组{module}手动写配方异常：{e}')
                    self.mqtt.publish(topic + '/reply', json.dumps({'success': False,
                                                                    'message': f'向模组{module}写配方异常：{e}'}))

    def mqtt_msg_parse(self, data, topic):
        """
        mqtt subscription message handle
        :param data: data
        :param topic: mqtt topic
        :return: None
        """
        # find opcua device and module information
        try:
            module = {'blockId': data['blockId'], 'index': data['index'], 'category': data['category']}
            dev = self.find_dev_with_module(module)
            if dev is None:  # don't find device with module information
                log.warning(f'Failure to match {module} to device.')
                self.mqtt.publish(topic + '/reply', json.dumps({'success': False,
                                                                'message': f'Failure to match {module} to device.'}))
                return
        except:
            log.warning(f'Failure to get device and module information from {data} of mqtt frame.')
            self.mqtt.publish(topic + '/reply', json.dumps({'success': False,
                                                            'message': f'Failure to get device and module from {data}.'}))
            return

        log.info(f'MQTT Collection Message:{data["cmd"]} {dev.name}{module},{data["list"]}')

        # match data['cmd']:
        #     case 'read':
        #         self.mqtt_read(dev, module, data['list'], topic)

    async def mqtt_general_command(self, data, topic):
        try:
            current_driver = {"blockId": self.config["Basic"]["blockId"], "index": self.config["Basic"]["index"],
                              "category": self.config["Basic"]["category"]}
            if data.get("commandType") == "DEV_RECONNECT":  # 设备重连指令
                command_content = data.get("commandContent")
                dev_name = command_content.get("devName")
                module = {"blockId": data.get("blockId", ""), "index": data.get("index", ""),
                          "category": data.get("category", "")}
                if module == current_driver:
                    state = await self.dev_reconnect(dev_name)
                    if state:
                        self.mqtt.publish(topic + '/reply',
                                          json.dumps({'success': True, 'message': f'{current_driver["blockId"]}_{current_driver["index"]}_{current_driver["category"]}: {dev_name}模组重连成功'}))
                    else:
                        self.mqtt.publish(topic + '/reply',
                                          json.dumps({'success': False, 'message': f'{current_driver["blockId"]}_{current_driver["index"]}_{current_driver["category"]}: {dev_name}模组重连失败，请重试'}))
                elif not data.get("blockId", "") and not data.get("index", "") and not data.get("category", ""):
                    state = await self.dev_reconnect(dev_name)
                    if state:
                        self.mqtt.publish(topic + '/reply',
                                          json.dumps({'success': True,
                                                      'message': f'{dev_name}模组重连成功'}))
                    else:
                        self.mqtt.publish(topic + '/reply',
                                          json.dumps({'success': False,
                                                      'message': f'{dev_name}模组重连失败，请重试'}))
            elif data.get("commandType") == "DEV_DISCONNECT":  # 设备断连指令
                command_content = data.get("commandContent")
                dev_name = command_content.get("devName")
                module = {"blockId": data.get("blockId", ""), "index": data.get("index", ""),
                          "category": data.get("category", "")}
                if module == current_driver:
                    state = await self.disconnect_dev(dev_name)
                    if state:
                        self.mqtt.publish(topic + '/reply',
                                          json.dumps({'success': True, 'message': f'{dev_name}模组断开连接成功'}))
                    else:
                        self.mqtt.publish(topic + '/reply',
                                          json.dumps({'success': False, 'message': f'{dev_name}模组断开连接失败，请重试'}))
                # else:
                #     self.mqtt.publish(topic + '/reply',
                #                       json.dumps({'success': False,
                #                                   'message': f'未匹配到模组：{module["blockId"]}_{module["index"]}_{module["category"]}'}))
            elif data.get("commandType") == "DEV_CONNECT":  # 设备连接指令
                command_content = data.get("commandContent")
                dev_name = command_content.get("devName")
                module = {"blockId": data.get("blockId", ""), "index": data.get("index", ""),
                          "category": data.get("category", "")}
                if module == current_driver:
                    state = await self.connect_dev(dev_name)
                    if state:
                        self.mqtt.publish(topic + '/reply',
                                          json.dumps({'success': True, 'message': f'{dev_name}模组连接成功'}))
                    else:
                        self.mqtt.publish(topic + '/reply',
                                          json.dumps({'success': False, 'message': f'{dev_name}模组连接失败，请重试'}))
                # else:
                #     self.mqtt.publish(topic + '/reply',
                #                       json.dumps({'success': False,
                #                                   'message': f'未匹配到模组：{module["blockId"]}_{module["index"]}_{module["category"]}'}))
            elif data.get("commandType") == "MODIFY_CONFIG":  # 修改配置内容
                command_content = data.get("commandContent")
                module = {"blockId": data.get("blockId", ""), "index": data.get("index", ""),
                          "category": data.get("category", "")}
                if module == current_driver:
                    state = save_config_file(f'./config files/driver config.json', command_content)
                    if state:
                        self.mqtt.publish(topic + '/reply',
                                          json.dumps({'success': True, 'message': '配置内容修改成功'}))
                    else:
                        self.mqtt.publish(topic + '/reply',
                                          json.dumps({'success': False, 'message': '配置内容修改失败，请重试'}))
                # else:
                #     self.mqtt.publish(topic + '/reply',
                #                       json.dumps({'success': False,
                #                                   'message': f'未匹配到模组：{module["blockId"]}_{module["index"]}_{module["category"]}'}))
            elif data.get("commandType") == "RESTART_PROCESS":  # 重启当前整个程序
                module = {"blockId": data.get("blockId", ""), "index": data.get("index", ""),
                          "category": data.get("category", "")}
                if module == current_driver:
                    self.RESTART_FLAG = True
                    self.mqtt.publish(topic + '/reply',
                                      json.dumps({'success': True, 'message': f'{current_driver["blockId"]}_{current_driver["index"]}_{current_driver["category"]} 重启中...'}))
                    await asyncio.sleep(2)
                    self.before_restarting()  # 把所有link置为False再发一次
                    self.restart_io_process()
                # else:
                #     self.mqtt.publish(topic + '/reply',
                #                       json.dumps({'success': False,
                #                                   'message': f'未匹配到模组：{module["blockId"]}_{module["index"]}_{module["category"]}'}))
            elif data.get("commandType") == "START_BROWSE_PROCESS":  # 启动遍历变量进程
                module = {"blockId": data.get("blockId", ""), "index": data.get("index", ""),
                          "category": data.get("category", "")}
                if module == current_driver:
                    self.browse_proc = self.start_browse_process()
                    self.mqtt.publish(topic + '/reply',
                                      json.dumps({'success': True, 'message': f'{current_driver["blockId"]}_{current_driver["index"]}_{current_driver["category"]} 遍历变量程序启动中...'}))
                # else:
                #     self.mqtt.publish(topic + '/reply',
                #                       json.dumps({'success': False,
                #                                   'message': f'未匹配到模组：{module["blockId"]}_{module["index"]}_{module["category"]}'}))
            elif data.get("commandType") == "STOP_BROWSE_PROCESS":  # 停止遍历变量进程
                module = {"blockId": data.get("blockId", ""), "index": data.get("index", ""),
                          "category": data.get("category", "")}
                if module == current_driver:
                    if self.stop_process(self.browse_proc, timeout=5):
                        self.browse_proc = None
                        self.mqtt.publish(topic + '/reply',
                                          json.dumps({'success': True, 'message': f'{current_driver["blockId"]}_{current_driver["index"]}_{current_driver["category"]} 遍历变量程序关闭'}))
                    else:
                        self.mqtt.publish(topic + '/reply',
                                          json.dumps({'success': False,
                                                      'message': f'{current_driver["blockId"]}_{current_driver["index"]}_{current_driver["category"]} 遍历变量程序未开启'}))
                # else:
                #     self.mqtt.publish(topic + '/reply',
                #                       json.dumps({'success': False,
                #                                   'message': f'未匹配到模组：{module["blockId"]}_{module["index"]}_{module["category"]}'}))
        except Exception as e:
            log.warning(f"mqtt 一般指令处理:{e}")

    def restart_io_process(self):
        """
            重启当前进程
        :param dis:
        :return:
        """
        try:
            # 重启程序
            print(f"{get_current_time()} 程序即将重启...")
            log.info("程序即将重启...")
            python = sys.executable
            os.execv(python, [python] + sys.argv)
        except Exception as e:
            print(f"程序重启失败:{e}")

    def start_browse_process(self):
        try:
            other_script_path = "./vars_browse_remote/browse_main.py"

            if not os.path.isfile(other_script_path):
                raise FileNotFoundError(f"无法找到browse_main.py，路径：{other_script_path}")
            # 使用subprocess.Popen启动脚本，注意这里不阻塞父进程
            process = subprocess.Popen(["python", other_script_path])
            return process
        except Exception as e:
            print("启动子进程失败:", e)
            return None

    def stop_process(self, process, timeout=10):
        """
        停止通过start_other_script启动的子进程
        :param process: subprocess.Popen 返回的进程对象
        :param timeout: 等待进程退出的超时秒数
        """
        # 检查子进程是否还在运行
        if self.browse_proc:
            if process.poll() is None:
                try:
                    process.terminate()  # 发送 SIGTERM 信号
                    process.wait(timeout=timeout)  # 等待进程优雅退出
                    return True
                except subprocess.TimeoutExpired:
                    print("子进程未能在超时时间内退出，使用 kill() 强制结束。")
                    process.kill()  # 强制杀死
                    process.wait()
                    return True
            else:
                print("子进程已经退出。")
                return False
        else:
            return False

    async def mqtt_parse(self, topic: str, data):
        """
        mqtt subscription incoming data handle
        :param topic: mqtt topic
        :param data: data
        :return: None
        """
        # print(str(datetime.now().time())[:-5], 'mqtt collection threaded:', threading.current_thread())
        # print(f"mqtt collection:{data}")

        # check json format
        try:
            frame = json.loads(data)
            frame_id = frame['id']
            # frame_ack = frame['ask']
        except:
            log.warning(f'Failure to match the format. mqtt datas: {data}')
            self.mqtt.publish(topic + '/reply',
                              json.dumps({'success': False, 'message': 'Failure to match the format.'}))
            return

        # check topic and frame type
        try:
            if topic[:len(self.mqtt.sub_gui_cmd) - 1] == self.mqtt.sub_gui_cmd[:len(self.mqtt.sub_gui_cmd) - 1]:
                await self.mqtt_cmd_parse(frame_id, frame['data'], topic)
            elif topic[:len(self.mqtt.sub_gui_msg) - 1] == self.mqtt.sub_gui_msg[:len(self.mqtt.sub_gui_msg) - 1]:
                self.mqtt_msg_parse(frame['msg'], topic)
            elif topic[:len(self.mqtt.sub_server_cmd) - 1] == self.mqtt.sub_server_cmd[
                                                              :len(self.mqtt.sub_server_cmd) - 1]:
                await self.mqtt_cmd_parse(frame_id, frame['data'], topic)
            elif topic[:len(self.mqtt.sub_general_cmd) - 1] == self.mqtt.sub_general_cmd[
                                                               :len(self.mqtt.sub_general_cmd) - 1]:
                # TODO: 去做控制单设备重连等操作
                print(f"接收到general_command指令：{topic}:{frame['data']}")
                log.info(f"接收到general_command指令：{topic}:{frame['data']}")
                await self.mqtt_general_command(frame['data'], topic)

        except:
            log.warning(f'Failure to match the format. mqtt datas: {data}')
            self.mqtt.publish(topic + '/reply',
                              json.dumps({'success': False, 'message': 'Failure to match the format.'}))

    def collection_from_opcua_subscription(self, opcua_name, node_id, value):
        """
        opcua subscription datas collection handle, single variable
        :param opcua_name: opcua device name
        :param node_id: opcua node id
        :param value: opcua node value
        :return: None
        """
        # print(str(datetime.now().time())[:-5], 'opcua collection threaded:', threading.current_thread())
        current_time = str(datetime.now().time())[:-7]  # collection time
        print(current_time, f'OPCUA Sub Collection:{opcua_name}, {node_id}, {value}')
        log.info(f'OPCUA Sub Collection:{opcua_name}, {node_id}, {value}')

        if type(value) is list:
            print(current_time, f'Failure to receive value list {type(value)}.')
            log.warning(f'Failure to receive value list {type(value)}.')
            return

        # find what opcua and search node id in subscription list of device
        try:
            dev = list(filter(lambda x: x.name == opcua_name, self.ua_device))[0]
        except:
            print(current_time, f'Failure to match {opcua_name} in device list.')
            log.warning(f'Failure to match {opcua_name} in device list.')
            return
        # print(dev.opcua.client)
        try:
            sub = list(filter(lambda x: x['ListNode']['NodeID'] == node_id, dev.VarSubscription))[0]
        except:
            print(current_time, f'Failure to match {node_id} in subscription list.')
            log.warning(f'Failure to match {node_id} in subscription list.')
            return
        # print(sub)

        # parse datas
        O2M_list = []
        msg = []
        node = sub['ListNode']
        O2M_list.append({'module': {'blockId': node["blockId"], 'index': node["index"], 'category': node["category"]},
                         'list': []})

        try:
            # datas_parse(dev, sub['TreeNode'], sub['ListNode'], value,
            #             False, None, self.O2M_All, O2M_list[0]['list'], int(time.time() * 1000), msg)
            asyncio.create_task(datas_parse_o2m(dev, sub['ListNode'], value,self.O2M_All, O2M_list[0]['list'], int(time.time() * 1000), msg))
            # print parse error message
            for s in msg:
                print(s)
        except:
            print(str(datetime.now().time())[:-7], f'Failure to parse opcua subscription {node_id}{value}.')
            log.warning(f'Failure to parse opcua subscription {node_id}{value}.')

        # pack module data and publish to mqtt
        for md in O2M_list:
            if md['list']:
                mqtt_frame = json_from_list(md)
                if mqtt_frame:
                    self.mqtt.publish(self.mqtt.pub_drv_data, mqtt_frame)

    async def request_task(self):
        """
        request recipe task
        """
        # define modules (in request config file) request recipe
        for rr in self.recipe_request_data:
            module = rr['module']
            write_recipe_id = rr['write_recipe_id']
            dev = self.find_dev_with_module(module)
            # print(module, dev.name, dev.connecting)
            if dev is not None:
                if dev.connecting is True:
                    try:
                        node = dev.code_to_node.get(code2format_str(module['blockId'], module['index'],
                                                                    module['category'], rr['request_node_path']))
                        req = get_request_nodes(dev, node, rr['recipe_request_update'],
                                                rr['recipe_request_id'], rr['recipe_request_result'])
                        if req['request']["value"] is True and req['result']["value"] == 0:  #
                            # trigger to request recipe
                            # await request_recipe_handle(self, self.config['Server']['Basic']['recipe_req_url'], req, dev,
                            #                                  module, write_recipe_id)
                            # await request_recipe_handle_gather(self, self.config['Server']['Basic']['recipe_req_url'],
                            #                                         req, dev, module, write_recipe_id)  # 并发下发Recipe-单模组
                            # await request_recipe_handle_gather_plc(self, self.config['Server']['Basic']['recipe_req_url'],
                            #                                             req, dev, module, write_recipe_id)  # 并发下发Recipe - 单plc
                            await request_recipe_handle_gather_link(self, self.config['Server']['Basic']['recipe_req_url'],
                                                                        req, dev, module, write_recipe_id, self.ua_device)  # 并发下发Recipe - 单link
                        elif req['request']["value"] is False and (req['result']["value"] != 0):
                            await clear_request_result(dev, req)
                    except Exception as e:
                        log.warning(f'Failure to request {module} recipe.{e}')

    async def timed_clear_task(self):
        """
        timed to clear safety control variable
        """
        # start opcua device task
        tasks = []
        for dev in self.ua_device:  # scan device
            if dev.TimedClear and dev.loading is True and dev.connecting is True:  # and dev.TimedClear:
                tasks.append(asyncio.create_task(dev.timed_clear_safety_variable()))

        # wait all task finish
        if tasks:
            await asyncio.gather(*tasks)

    async def opcua_device_read_task(self):
        """
        read device task
        """
        # start opcua device task
        tasks = []
        dev_sync = []
        for dev in self.ua_device:  # scan device
            try:
                read_cfg = self.config['Opcua'][dev.name]['Control']['Read']
                await dev.get_connecting_state()
                if dev.loading is True and dev.connecting is True and dev.ReadBlock and read_cfg is True:
                    if dev.link_type == 'opcua':
                        tasks.append(asyncio.create_task(dev.read_variable_block(self.mqtt, [])))
                    elif dev.link_type == 's7':
                        if dev.linker.sync is True:  # synchronous read
                            dev_sync.append(dev)
                        else:  # asynchronous read
                            tasks.append(asyncio.create_task(dev.read_variable_block_vs7(self.mqtt)))
            except Exception as e:
                log.warning(f'{e}Failure to read {dev.name} variable, check configuration.')

        # synchronous read s7 device
        for dev in dev_sync:
            try:
                await dev.read_variable_block_vs7(self.mqtt)
            except:
                log.warning(f'Failure to read {dev.name} via s7.')

        # wait all task finish
        if tasks:
            await asyncio.gather(*tasks)

    async def mqtt_handler(self):
        """
        MQTT 数据处理器
        """
        try:
            item = await self.mqtt.mq.get()
            try:
                await self.mqtt_parse(item['topic'], item['data'])
            except Exception as e:
                log.warning(f"处理MQTT消息时发生错误：{e}", exc_info=True)
            finally:
                self.mqtt.mq.task_done()
        except asyncio.CancelledError:
            log.info("MQTT处理任务被取消")
            raise
        except Exception as e:
            log.warning(f"MQTT处理过程中发生意外错误：{e}", exc_info=True)

    def before_restarting(self):
        for dev in self.ua_device:  # scan device
            dev_cfg = self.config['Opcua'][dev.name]
            dev_cfg['Status']['Linking'] = False
        mframe = json_from_list({'module': {"blockId": self.config["Basic"]["blockId"], "index": self.config["Basic"]["index"], "category": self.config["Basic"]["category"]}, 'list': self.config})
        if mframe:
            self.mqtt.publish(self.mqtt.pub_drv_data_struct, mframe)

    async def opcua_device_manage_task(self):
        """
        device manage task
        """
        # start opcua device task
        if not self.RESTART_FLAG:
            tasks = []
            for dev in self.ua_device:  # scan device
                dev_cfg = self.config['Opcua'][dev.name]
                if dev.loading is True:
                    tasks.append(asyncio.create_task(dev.device_manager(dev_cfg['Control']['Link'])))
                # loading status to config
                dev_cfg['Status']['Load'] = dev.loading
                dev_cfg['Status']['Linking'] = dev.connecting
                dev_cfg['Status']['Module_Number'] = dev.module_number
                dev_cfg['Status']['Variable_Number'] = dev.VarNumber
                dev_cfg['Status']['Read_Block_Number'] = dev.ReadBlock_Number
                dev_cfg['Parameter']['modules'] = dev.module

            # publish driver status (include opcua device) to mqtt
            # list_data = []
            # nested_dict_2list(self.config, list_data, int(time.time() * 1000))
            # pprint.pprint(list_data)
            # mframe = json_from_list({'module': {"blockId": self.config["Basic"]["blockId"], "index": self.config["Basic"]["index"], "category": self.config["Basic"]["category"]}, 'list': list_data})
            mframe = json_from_list({'module': {"blockId": self.config["Basic"]["blockId"], "index": self.config["Basic"]["index"], "category": self.config["Basic"]["category"]}, 'list': self.config})
            if mframe:
                # self.mqtt.publish(self.mqtt.pub_drv_data, mframe)
                self.mqtt.publish(self.mqtt.pub_drv_data_struct, mframe)

            # wait all task finish
            if tasks:
                await asyncio.gather(*tasks)

    # 定时检查模组的连接状态，并发布
    async def modules_connection_state_task(self):
        try:
            devs_connection_state = []
            if self.ua_device:
                for dev in self.ua_device:
                    devs_connection_state.append({"moduleName": dev.name, "connectionState": dev.connecting})
                self.mqtt.publish(self.mqtt.pub_modules_status,
                                  json.dumps({"data": {"commandType": "moduleConnectionState",
                                                       "commandContent": {"list": devs_connection_state},
                                                       "blockId": self.config["Basic"]["blockId"], "index": self.config["Basic"]["index"], "category": self.config["Basic"]["category"]}}))
        except Exception as e:
            log.warning(f"定时检查模组的连接状态:{e}")

    # async def initialize_opcua_device(self):
    #     """
    #     # create opcua devices with config file (*.csv), initialization device structure and subscription variable
    #     """
    #
    #     # create opcua device with device list in config file
    #     for k, dev_cfg in self.config['Opcua'].items():
    #         # create new opcua device
    #         # print(k, dev_cfg['Basic'])
    #         dev = device(dev_cfg['Basic'], self.collection_from_opcua_subscription)
    #         print(f"dev:{dev.linker}")
    #         dev.O2M_All = self.O2M_All
    #         dev.M2O_All = self.M2O_All
    #
    #         # print(dev.name, dev.opcua.uri, dev.opcua.main_node)
    #         print(f'Add opcua {dev.name}:{dev.link_type} [{dev.linker.uri},{dev.linker.main_node})] to system.')
    #         log.info(f'Add opcua {dev.name}{dev.link_type} [{dev.linker.uri},{dev.linker.main_node})] to system.')
    #         if dev_cfg['Control']['Load'] is True:
    #             await dev.load_variable_list()
    #             print(f'Load {dev.name} variable list, {dev.loading}.')
    #             print(f'--Module is {dev.module_number}, Variable is {dev.VarNumber},'
    #                   f'Reading Block is {dev.ReadBlock_Number}.')
    #             log.info(f'Load {dev.name} variable list, {dev.loading}.')
    #             log.info(f'--Module is {dev.module_number}, Variable is {dev.VarNumber},'
    #                      f'Reading Block is {dev.ReadBlock_Number}.')
    #         else:
    #             print(f'Configuration setting is no load.')
    #             log.info(f'Configuration setting is no load.')
    #         dev_cfg['Control']['Load'] = False
    #
    #         # connect to opcua device
    #         if dev_cfg['Control']['Link'] is True:
    #             if dev.loading is True:
    #                 await dev.connect()
    #                 await dev.subscribe()
    #                 print(f'Connect {dev.name}, {dev.connecting}.')
    #                 print(f'--Subscription {dev.subscription_state},'
    #                       f'Subscription variable is {dev.Subscription_Nodes_Number}.')
    #                 log.info(f'Connect {dev.name}, {dev.connecting}.')
    #                 log.info(f'--Subscription {dev.subscription_state},'
    #                          f'Subscription variable is {dev.Subscription_Nodes_Number}.')
    #             # else:
    #             #     print(f'None variable list.')
    #             #     log.warning(f'None variable list.')
    #         else:
    #             print(f'Configuration setting is no connect.')
    #             log.warning(f'Configuration setting is no connect.')
    #         self.ua_device.append(dev)
    #
    #         # subscribe opcua nodes
    #
    #         # print_tree(dev.VarTree, all_attrs=True)
    #         # pprint.pprint(dev.VarList)
    #         # print(dev.VarSubscription)
    #         # print_tree(dev.VarTree)

    async def initialize_opcua_device(self):
        """
        Create OPC UA devices with config file (*.csv), initialize device structure, and subscribe variables.
        """

        async def setup_device(k, dev_cfg):
            """Setup a single OPC UA device."""
            # Create a new OPC UA device
            dev = device(dev_cfg['Basic'], self.collection_from_opcua_subscription)
            dev.O2M_All = self.O2M_All
            dev.M2O_All = self.M2O_All

            print(f'Add opcua {dev.name}:{dev.link_type} [{dev.linker.uri},{dev.linker.main_node})] to system.')
            log.info(f'Add opcua {dev.name}{dev.link_type} [{dev.linker.uri},{dev.linker.main_node})] to system.')

            # Load variable list if configured
            if dev_cfg['Control']['Load'] is True:
                await dev.load_variable_list()
                print(f'Load {dev.name} variable list, {dev.loading}.')
                print(f'--Module is {dev.module_number}, Variable is {dev.VarNumber},'
                      f'Reading Block is {dev.ReadBlock_Number}.')
                log.info(f'Load {dev.name} variable list, {dev.loading}.')
                log.info(f'--Module is {dev.module_number}, Variable is {dev.VarNumber},'
                         f'Reading Block is {dev.ReadBlock_Number}.')
            else:
                print(f'Configuration setting is no load.')
                log.info(f'Configuration setting is no load.')
            # dev_cfg['Control']['Load'] = False

            # Connect to OPC UA device if configured
            if dev_cfg['Control']['Link'] is True:
                if dev.loading is True:
                    await dev.connect()
                    await dev.subscribe()
                    print(f'Connect {dev.name}, {dev.connecting}.')
                    print(f'--Subscription {dev.subscription_state},'
                          f'Subscription variable is {dev.Subscription_Nodes_Number}.')
                    log.info(f'Connect {dev.name}, {dev.connecting}.')
                    log.info(f'--Subscription {dev.subscription_state},'
                             f'Subscription variable is {dev.Subscription_Nodes_Number}.')
            else:
                print(f'Configuration setting is no connect.')
                log.warning(f'Configuration setting is no connect.')

            # Append the device to the device list
            self.ua_device.append(dev)

        # 并行执行每个设备的初始化
        tasks = [
            setup_device(k, dev_cfg)
            for k, dev_cfg in self.config['Opcua'].items()
        ]
        try:
            # Run all tasks concurrently
            await asyncio.gather(*tasks)
        except Exception as e:
            print(e)
        # 串行执行每个设备的初始化
        # for k, dev_cfg in self.config['Opcua'].items():
        #     try:
        #         await setup_device(k, dev_cfg)  # 使用 await 逐个执行
        #     except Exception as e:
        #         print(e)
        #         log.error(f"Error initializing device {k}: {str(e)}")

    async def close_opcua_device(self):
        """
        close opcua device
        """
        for dev in self.ua_device:
            if dev.connecting is True:
                await dev.disconnect()
                print(f'device {dev.name}, linking:{dev.connecting}')

    def initialize_mqtt(self):
        """
        initialize mqtt client
        """
        # create new mqtt linker
        self.mqtt = mqtt_linker(self.config['Mqtt']['Basic'], self.config['Mqtt']['Parameter'])

        # connect to mqtt broker
        self.mqtt.connect()
        print(f'MQTT id:{self.mqtt.id}, url:{self.mqtt.url}:{self.mqtt.port}.')
        log.info(f'MQTT id:{self.mqtt.id}, url:{self.mqtt.url}:{self.mqtt.port}.')
        # subscribe topic
        # self.mqtt.subscription()
        print(f'MQTT subscription is {self.mqtt.subscription_state}')
        log.info(f'MQTT subscription is {self.mqtt.subscription_state}')

        self.mqtt.client.loop_start()

    def close_mqtt(self):
        """
        close mqtt client
        """
        try:
            self.mqtt.disconnect()
            print(f'close mqtt, linking:{self.mqtt.connecting}')
        except Exception as e:
            print(f'close mqtt error:{e}')

    # 单设备断开连接
    async def disconnect_dev(self, dev_name):
        """
        单设备断开连接
        """
        disconnect_state = None
        for dev in self.ua_device:
            if dev_name == dev.name and dev.connecting:
                await dev.disconnect()
                message = f'INFO:{dev.name} connection status: {dev.connecting}.'
                log.info(message)
                disconnect_state = True
        return disconnect_state

    async def connect_dev(self, dev_name):
        """
        单设备连接
        """
        connect_state = None
        for dev in self.ua_device:
            if dev_name == dev.name and dev.connecting is False:
                await dev.load_variable_list()
                await dev.connect()
                # await dev.subscribe()
                message = f'INFO:{dev.name} connection status: {dev.connecting}.'
                log.info(message)
                connect_state = True
        return connect_state

    async def dev_reconnect(self, dev_name):
        """
        单设备重连
        """
        reconnect_state = None
        for dev in self.ua_device:
            if dev_name == dev.name and dev.connecting:
                await dev.disconnect()
                message = f'INFO:{dev.name}:{dev.linker.uri} Disconnected. Try to reconnect.....'
                print(message)
                log.info(message)
                await dev.load_variable_list()
                await asyncio.sleep(8)  # 休眠8s 检测设备是否自动重连成功
                reconnect_state = dev.connecting
                if not dev.connecting:
                    await dev.connect()
                    message = f'INFO:{dev.name}:{dev.linker.uri} reconnection state: {dev.connecting}.'
                    print(message)
                    log.info(message)
                    reconnect_state = dev.connecting
                await dev.subscribe()
        return reconnect_state

    async def initialize(self):
        """
        initialize distribution system
        """
        # relate opcua subscription to mqtt publish, directly
        self.O2M_All = True
        # relate mqtt subscription to write to opcua
        self.M2O_All = True

        # load driver config
        self.load_config_file()
        # initialize opcua device
        await self.initialize_opcua_device()
        # initialize mqtt
        self.initialize_mqtt()
        # load request config
        self.load_request_file()  # after initialize_opcua_device
