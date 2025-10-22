import json
import time

import asyncio
from datetime import datetime

from bigtree import find_child_by_name, find_path

from api.api_manager import request_get
from logger import log
from utils.helpers import code2format_str
from utils.time_util import get_current_time, get_milliseconds


async def return_request_state(dev, req, state):
    """
    return request state to server
    :param dev: device object
    :param req: request information dictionary
    :param state: request state
    :return: result of writing request state to server
    """
    M2O_list = [{'node_id': req['result']["NodeID"], 'datatype': req['result']["DataType"], 'value': state}]
    await dev.linker.write_multi_variables(M2O_list, 0.1)


def find_dev_with_module(module, ua_device):
    """
        find device(PLC) with module, module{0_1_MC}-->device{MC}
    """
    # find opcua device with module information
    for dev in ua_device:
        for m in dev.module:
            if m == module:  # match module blockID,index,category
                return dev
    return None


# recipe 改为并发向模组下载数据
async def request_recipe_handle_gather(dis, url, req, dev, module, write_recipe_id):
    """
    request recipe handle
    :param url: server url
    :param req: request nodes
    :param dev: opcua device object
    :param module: module information (blockId, index, category)
    :return: None
    """
    start_total_time = time.time()  # 记录开始时间
    recipe_id = req['id']["value"]
    # request datas from server
    print(f'{get_current_time()}: Request recipeId {recipe_id} from {url}')
    log.info(f'Request recipeId {recipe_id} from {url}')
    # datas = server_datas_testing  # testing
    await return_request_state(dev, req, 1)
    params = {'recipeId': recipe_id}
    datas = request_get(url, "", params)
    print(f'{get_current_time()}: 配方请求结果：{datas}')
    log.info(f'配方请求结果：{datas}')

    # data parse and write recipe to opcua
    if datas is None:  # no response or response none
        log.warning(f'Failure to request from server, No response.')
        await return_request_state(dev, req, 1001)
    elif datas['code'] == 10000:
        log.warning(f'Failure to request from server, 1000: software has error occur.')
        await return_request_state(dev, req, 1002)
    elif datas['code'] == 20001:
        log.warning(f'Failure to request from server, 20001: recipe is not exist.')
        await return_request_state(dev, req, 1003)
    elif datas['code'] == 20002:
        log.warning(f'Failure to request from server, 20002: recipe class is invalid.')
        await return_request_state(dev, req, 1004)
    elif datas['code'] == 200:
        await return_request_state(dev, req, 2)
        print(f'{get_current_time()}: 配方开始向PLC下载...')
        log.info(f' 配方开始向PLC下载...')
        all_success = True
        # 使用asyncio.gather并发执行每个mr
        tasks = []
        for mr in datas['data']:
            tasks.append(process_recipe_data(dis, mr, dev, module, all_success))

        results = await asyncio.gather(*tasks)
        all_success = all(results)

        # check all recipe write success or not
        if all_success:
            log.info(f'Success to write {module} recipe.')
            print(f'{get_current_time()}: 所有配方下载成功，开始下发recipe_id给MC')
            log.info(f'所有配方下载成功，开始下发recipe_id给MC')
            # recipe_id 再下发一次给MC
            await dev.linker.write_multi_variables([{'datatype': 6,
                                                     'node_id': write_recipe_id,
                                                     'value': recipe_id}], 1.5)
            await return_request_state(dev, req, 3)
            end_total_time = time.time()  # 记录结束时间
            total_time = end_total_time - start_total_time  # 计算总耗时
            print(f'{get_current_time()}: 下载所有配方所用的总时间: {total_time:.2f} seconds')
            log.info(f'下载所有配方所用的总时间: {total_time:.2f} seconds')
        else:
            log.warning(f'Failure to write {module} recipe.')
            await return_request_state(dev, req, 1009)


async def process_recipe_data(dis, mr, dev, module, all_success):
    """
    Helper function to process each recipe data
    """
    start_time = int(time.time() * 1000)
    success = False
    if mr['category'] == 'MC':  # 针对MC做特殊处理
        mr["list"][0]["value"]["Basic"]["Id"] = 0

    # parse json datas
    result = await dis.json_data_parse(mr, dev, module)
    parse_time = int(time.time() * 1000)

    if result['ErrMSG']:  # error ,parse fail
        all_success = False
        for msg in result['ErrMSG']:
            print(str(datetime.now().time())[:-7], msg)
            log.warning(msg)
    elif result['M2O_list']:  # write recipe to opcua
        if result['Device'].connecting is False:
            log.warning(f'Failure to write opcua {result["Device"].name}, not linked.')
        else:
            success = await result['Device'].linker.write_multi_variables(result['M2O_list'], 1.5)

        if success:
            write_time = int(time.time() * 1000)
            device_name = result['Device'].name
            log.info(f'Download recipe to {device_name} Timing: '
                     f'parsing {parse_time - start_time},'
                     f'writing {write_time - parse_time},'
                     f'Total is {write_time - start_time}')
        else:
            all_success = False
            log.warning(f'Failure to write recipe to {result["Device"].name}.')

    return all_success


def process_recipes(dis, datas):
    # 分类映射字典（category -> uri）
    category_uri_map = {}
    for item in dis.recipe_single_module:
        category = item["module"]["category"]
        uri = item["uri"]
        if category in category_uri_map and category_uri_map[category] != uri:
            raise ValueError(f"Conflict URI for category {category}")
        category_uri_map[category] = uri
    for item in dis.recipe_request_data:
        category = item["module"]["category"]
        uri = item["uri"]
        if category in category_uri_map and category_uri_map[category] != uri:
            raise ValueError(f"Conflict URI for category {category}")
        category_uri_map[category] = uri

    grouped_data = {}
    for mr in datas['data']:
        if mr['category'] == 'MC':
            mr["list"][0]["value"]["Basic"]["Id"] = 0

        uri = category_uri_map.get(mr['category'])
        if not uri:
            continue
        ip_parts = uri.split('.')[-2:]  # 取最后两个数字段
        group_key = f"plc_{'_'.join(ip_parts)}_recipe_datas"
        if group_key not in grouped_data:
            grouped_data[group_key] = []
        grouped_data[group_key].append(mr)
    return grouped_data


# recipe 改为向单个plc并发下载数据
async def request_recipe_handle_gather_plc(dis, url, req, dev, module, write_recipe_id):
    """
    request recipe handle
    :param url: server url
    :param req: request nodes
    :param dev: opcua device object
    :param module: module information (blockId, index, category)
    :return: None
    """
    start_total_time = time.time()  # 记录开始时间
    recipe_id = req['id']["value"]
    # request datas from server
    print(f'{get_current_time()}: Request recipeId {recipe_id} from {url}')
    log.info(f'Request recipeId {recipe_id} from {url}')
    # datas = server_datas_testing  # testing
    await return_request_state(dev, req, 1)
    params = {'recipeId': recipe_id}
    # params = {'recipeId': 47}
    datas = request_get(url, "", params)
    # datas = request_get('http://192.168.55.17:13871/api/upper/recipe/info/drive/format?recipeId=47', "", params)
    print(f'{get_current_time()}: 配方请求结果：{datas}')
    log.info(f'配方请求结果：{datas}')
    # data parse and write recipe to opcua
    # print(f'Server response data: {datas}.')
    if datas is None:  # no response or response none
        log.warning(f'Failure to request from server, No response.')
        await return_request_state(dev, req, 1001)
    elif datas['code'] == 10000:
        log.warning(f'Failure to request from server, 1000: software has error occur.')
        await return_request_state(dev, req, 1002)
    elif datas['code'] == 20001:
        log.warning(f'Failure to request from server, 20001: recipe is not exist.')
        await return_request_state(dev, req, 1003)
    elif datas['code'] == 20002:
        log.warning(f'Failure to request from server, 20002: recipe class is invalid.')
        await return_request_state(dev, req, 1004)
    elif datas['code'] == 200:
        await return_request_state(dev, req, 2)
        print(f'{get_current_time()}: 配方开始向PLC下载...')
        log.info(f' 配方开始向PLC下载...')
        # 先把数据根据uri动态组织到对应的数组里面去
        grouped_data = process_recipes(dis, datas)

        tasks = []
        for recipe_data in grouped_data:
            tasks.append(write_recipe_handle(dis, grouped_data.get(recipe_data), dev, module, req))
        results = await asyncio.gather(*tasks)
        all_success = all(results)

        # check all recipe write success or not
        if all_success:
            log.info(f'Success to write {module} recipe.')
            print(f'{get_current_time()}: 所有配方下载成功，开始下发recipe_id给MC')
            log.info(f'所有配方下载成功，开始下发recipe_id给MC')
            # recipe_id 再下发一次给MC
            await dev.linker.write_multi_variables([{'datatype': 6,
                                                     'node_id': write_recipe_id,
                                                     'value': recipe_id}], 1.5)
            await return_request_state(dev, req, 3)
            end_total_time = time.time()  # 记录结束时间
            total_time = end_total_time - start_total_time  # 计算总耗时
            print(f'{get_current_time()}: 下载所有配方所用的总时间: {total_time:.2f} seconds')
            log.info(f'下载所有配方所用的总时间: {total_time:.2f} seconds')
        else:
            log.warning(f'自动下载配方失败-1009，请查看上面写入失败原因')
            await return_request_state(dev, req, 1009)


async def write_recipe_handle(dis, arr, dev, module, req):
    # parse recipe data and write recipe to opcua device
    all_success = True
    write_time = 0
    for mr in arr:
        print(f'{get_current_time()}:开始向{mr["blockId"]}-{mr["index"]}-{mr["category"]}模组下载配方')
        log.info(f'开始向{mr["blockId"]}-{mr["index"]}-{mr["category"]}模组下载配方')
        start_time = int(time.time() * 1000)
        key = (mr["blockId"], mr["index"], mr["category"])
        if mc_match := dis.recipe_request_map.get(key):  # 针对MC做特殊处理
            mr["list"][0]["value"]["Basic"]["Id"] = 0
        # if mr['category'] == self.recipe_request_data[0]['module']['category']:  # 针对MC做特殊处理
        #     mr["list"][0]["value"]["Basic"]["Id"] = 0
        # parse json datas
        result = await dis.json_data_parse(mr, dev, module)
        parse_time = int(time.time() * 1000)
        if result['ErrMSG']:  # error ,parse fail
            all_success = False
            for msg in result['ErrMSG']:
                # print(str(datetime.now().time())[:-7], msg)
                log.warning(f'单模组配方写入失败:{msg}')
        # write recipe to opcua
        elif result['M2O_list']:
            success = False
            if result['Device'].connecting is False:
                log.warning(f'Failure to write opcua {result["Device"].name}, not linked.')
            else:
                if mc_match := dis.recipe_request_map.get(key):  # 如果是MC则直接写配方
                    success = await result['Device'].linker.write_multi_variables(result['M2O_list'],
                                                                                  1.5)  # 开始写配方
                    write_time = int(time.time() * 1000)

                try:
                    recipe_valid_info = dev.code_to_node.get(
                        code2format_str(module['blockId'], module['index'],
                                        module['category'], "Others_Recipe_valid"))
                    if not recipe_valid_info:
                        recipe_valid_info = dev.code_to_node.get(
                            code2format_str(module['blockId'], module['index'],
                                            module['category'], "Other_Reicpe_Valid"))
                except:
                    recipe_valid_info = dev.code_to_node.get(
                        code2format_str(module['blockId'], module['index'],
                                        module['category'], "Other_Reicpe_Valid"))

                try:
                    writable_path_info = dev.code_to_node.get(
                        code2format_str(module['blockId'], module['index'],
                                        module['category'], "Others_Recipe_Writable"))
                    if not writable_path_info:
                        writable_path_info = dev.code_to_node.get(
                            code2format_str(module['blockId'], module['index'],
                                            module['category'], "Other_Reicpe_Writable"))
                except:
                    writable_path_info = dev.code_to_node.get(
                        code2format_str(module['blockId'], module['index'],
                                        module['category'], "Other_Reicpe_Writable"))

                if not writable_path_info["value"]:  # 检查当前模组是否支持下载配方
                    msg = f'{mr["blockId"]}-{mr["index"]}-{mr["category"]}模组当前不支持下载配方，终止操作'
                    print(f'{get_current_time()}: {msg}')
                    log.info(msg)
                    await return_request_state(dev, req, 1009)
                    return
                else:
                    if await result['Device'].linker.write_multi_variables(
                            [{'node_id': recipe_valid_info["NodeID"],
                              'datatype': recipe_valid_info["DataType"],
                              'value': True}], 1.5):  # 先把模组的Recipe_Valid’为True
                        success = await result['Device'].linker.write_multi_variables(result['M2O_list'],
                                                                                      1.5)  # 开始写配方
                        write_time = int(time.time() * 1000)
                        await result['Device'].linker.write_multi_variables(
                            [{'node_id': recipe_valid_info["NodeID"],
                              'datatype': recipe_valid_info["DataType"],
                              'value': False}], 1.5)  # 再把模组的Recipe_Valid’为False
                        log.warning(f'向{mr["blockId"]}-{mr["index"]}-{mr["category"]}模组下载配方失败')

            if success is True:
                print(f'{get_current_time()}：向{mr["blockId"]}-{mr["index"]}-{mr["category"]}模组下载配方完成，耗时: '
                      f'parsing {parse_time - start_time}ms,'
                      f'writing {write_time - parse_time}ms,'
                      f'Total is {write_time - start_time}ms')
                log.info(f'向{mr["blockId"]}-{mr["index"]}-{mr["category"]}模组下载配方完成，耗时: '
                         f'parsing {parse_time - start_time}ms,'
                         f'writing {write_time - parse_time}ms,'
                         f'Total is {write_time - start_time}ms')
            else:
                all_success = False
    return all_success


async def request_recipe_handle(dis, url, req, dev, module, write_recipe_id):
    """
    request recipe handle
    :param url: server url
    :param req: request nodes
    :param dev: opcua device object
    :param module: module information (blockId, index, category)
    :return: None
    """
    start_total_time = time.time()  # 记录开始时间
    recipe_id = req['id']["value"]
    # request datas from server
    print(f'{get_current_time()}: Request recipeId {recipe_id} from {url}')
    log.info(f'Request recipeId {recipe_id} from {url}')
    # datas = server_datas_testing  # testing
    await return_request_state(dev, req, 1)
    params = {'recipeId': recipe_id}
    # params = {'recipeId': 47}
    datas = request_get(url, "", params)
    # datas = request_get('http://192.168.55.17:13871/api/upper/recipe/info/drive/format?recipeId=47', "", params)
    print(f'{get_current_time()}: 配方请求结果：{datas}')
    log.info(f'配方请求结果：{datas}')
    # data parse and write recipe to opcua
    # print(f'Server response data: {datas}.')
    if datas is None:  # no response or response none
        log.warning(f'Failure to request from server, No response.')
        await return_request_state(dev, req, 1001)
    elif datas['code'] == 10000:
        log.warning(f'Failure to request from server, 1000: software has error occur.')
        await return_request_state(dev, req, 1002)
    elif datas['code'] == 20001:
        log.warning(f'Failure to request from server, 20001: recipe is not exist.')
        await return_request_state(dev, req, 1003)
    elif datas['code'] == 20002:
        log.warning(f'Failure to request from server, 20002: recipe class is invalid.')
        await return_request_state(dev, req, 1004)
    elif datas['code'] == 200:
        await return_request_state(dev, req, 2)
        print(f'{get_current_time()}: 配方开始向PLC下载...')
        log.info(f' 配方开始向PLC下载...')
        all_success = await write_recipe_handle(dis, datas['data'], dev, module, req)
        # check all recipe write success or not
        if all_success is True:
            log.info(f'Success to write {module} recipe.')
            print(f'{get_current_time()}: 所有配方下载成功，开始下发recipe_id给MC')
            log.info(f'所有配方下载成功，开始下发recipe_id给MC')
            # recipe_id 再下发一次给MC
            await dev.linker.write_multi_variables([{'datatype': 6,
                                                     'node_id': write_recipe_id,
                                                     'value': recipe_id}], 1.5)
            await return_request_state(dev, req, 3)
            end_total_time = time.time()  # 记录结束时间
            total_time = end_total_time - start_total_time  # 计算总耗时
            print(f'{get_current_time()}: 下载所有配方所用的总时间: {total_time:.2f} seconds')
            log.info(f'下载所有配方所用的总时间: {total_time:.2f} seconds')
        else:
            log.warning(f'自动下载配方失败-1009，请查看上面写入失败原因')
            await return_request_state(dev, req, 1009)


async def request_recipe_handle_gather_link(dis, url, req, dev, module, write_recipe_id, ua_device, flow_index, valid_keys, writable_keys, mqtt):
    """
    request recipe handle
    :param url: server url
    :param req: request nodes
    :param dev: opcua device object
    :param module: module information (blockId, index, category)
    :return: None
    """
    start_total_time = time.time()  # 记录开始时间
    recipe_id = req['id']["value"]
    # request datas from server
    print(f'{get_current_time()}: Request recipeId {recipe_id} from {url}')
    log.info(f'Request recipeId {recipe_id} from {url}')
    # datas = server_datas_testing  # testing
    await return_request_state(dev, req, 1)
    if flow_index is None:
        params = {'recipeId': recipe_id}
    else:
        params = {'recipeId': recipe_id, 'flowIndex': flow_index}
    # params = {'recipeId': 47}
    datas = request_get(url, "", params)
    # datas = request_get('http://192.168.55.71:13871/api/upper/recipe/info/drive/format?recipeId=47&flowIndex=', "", params)
    print(f'{get_current_time()}: 配方请求结果：{datas}')
    log.info(f'配方请求结果：{datas}')
    # data parse and write recipe to opcua
    # print(f'Server response data: {datas}.')
    if datas is None:  # no response or response none
        log.warning(f'Failure to request from server, No response.')
        await return_request_state(dev, req, 1001)
    elif datas['code'] == 10000:
        log.warning(f'Failure to request from server, 1000: software has error occur.')
        await return_request_state(dev, req, 1002)
    elif datas['code'] == 20001:
        log.warning(f'Failure to request from server, 20001: recipe is not exist.')
        await return_request_state(dev, req, 1003)
    elif datas['code'] == 20002:
        log.warning(f'Failure to request from server, 20002: recipe class is invalid.')
        await return_request_state(dev, req, 1004)
    elif datas['code'] == 20003:  # 增加Recipe check 逻辑
        log.warning(f'Failure to request from server, 20003: {datas["message"]}')
        mqtt.publish(mqtt.pub_drv_broadcast, json.dumps({
            "timestamp":get_milliseconds(),
            "type":"RecipeCheckError",
            "data":datas["checkResult"]
        }), 2)
        await return_request_state(dev, req, 1009)
    elif datas['code'] == 200:
        await return_request_state(dev, req, 2)
        print(f'{get_current_time()}: 配方开始向PLC下载...')
        log.info(f' 配方开始向PLC下载...')
        # parse all datas
        results = []
        all_success = True
        for mr in datas['data']:
            key = (mr["blockId"], mr["index"], mr["category"])
            if mc_match := dis.recipe_request_map.get(key):  # 针对MC做特殊处理
                mr["list"][0]["value"]["Basic"]["Id"] = 0
            # parse json datas
            result = await dis.json_data_parse(mr, dev, module)
            # print(f"合并前：Name:{result['Device'].name} Nodes: {result['Nodes']}  list len:{len(result['M2O_list'])}")
            results.append(result)

        # check writable
        rv_m2o_list = []
        for re_check in results:
            re_module = re_check['Module']
            if re_module:
                current_dev = find_dev_with_module(re_module, ua_device)
                key = (re_module["blockId"], re_module["index"], re_module["category"])
                if not dis.recipe_request_map.get(key):
                    # try:
                    #     recipe_valid_info = current_dev.code_to_node.get(
                    #         code2format_str(re_module['blockId'], re_module['index'],
                    #                         re_module['category'], "Others_Recipe_valid"))
                    #     if not recipe_valid_info:
                    #         recipe_valid_info = current_dev.code_to_node.get(
                    #             code2format_str(re_module['blockId'], re_module['index'],
                    #                             re_module['category'], "Other_Reicpe_Valid"))
                    # except:
                    #     recipe_valid_info = current_dev.code_to_node.get(
                    #         code2format_str(re_module['blockId'], re_module['index'],
                    #                         re_module['category'], "Other_Reicpe_Valid"))
                    #
                    # try:
                    #     writable_path_info = current_dev.code_to_node.get(
                    #         code2format_str(re_module['blockId'], re_module['index'],
                    #                         re_module['category'], "Others_Recipe_Writable"))
                    #     if not writable_path_info:
                    #         writable_path_info = current_dev.code_to_node.get(
                    #             code2format_str(re_module['blockId'], re_module['index'],
                    #                             re_module['category'], "Other_Reicpe_Writable"))
                    # except:
                    #     writable_path_info = current_dev.code_to_node.get(
                    #         code2format_str(re_module['blockId'], re_module['index'],
                    #                         re_module['category'], "Other_Reicpe_Writable"))
                    recipe_valid_info = None
                    for key in valid_keys:
                        try:
                            recipe_valid_info = current_dev.code_to_node.get(
                                code2format_str(re_module['blockId'], re_module['index'],
                                                re_module['category'], key))
                            if recipe_valid_info:
                                break
                        except Exception:
                            continue

                    writable_path_info = None
                    for key in writable_keys:
                        try:
                            writable_path_info = current_dev.code_to_node.get(
                                code2format_str(re_module['blockId'], re_module['index'],
                                                re_module['category'], key))
                            if writable_path_info:
                                break
                        except Exception:
                            continue
                    if recipe_valid_info is None or writable_path_info is None:
                        log.warning(f'终止操作，请核对{re_module["blockId"]}-{re_module["index"]}-{re_module["category"]}模组Recipe Valid或Writable是否存在或异常')
                        await return_request_state(dev, req, 1005)
                        return
                    if not writable_path_info["value"]:  # 检查当前模组是否支持下载配方
                        msg = f'{re_module["blockId"]}-{re_module["index"]}-{re_module["category"]}模组当前不支持下载配方，终止操作'
                        log.warning(msg)
                        all_success = False
                        await return_request_state(dev, req, 1005)
                        return
                    else:
                        if await re_check['Device'].linker.write_multi_variables(
                                [{'node_id': recipe_valid_info["NodeID"],
                                  'datatype': recipe_valid_info["DataType"],
                                  'value': True}], 1.5):  # 先把模组的Recipe_Valid’为True
                            # rv_m2o_list.append(recipe_valid_info)  # 如果写入成功则把当前模组recipe_valid放入list中进行临时保存
                            rv_m2o_list.append({current_dev: recipe_valid_info})  # 如果写入成功则把当前模组recipe_valid放入list中进行临时保存
                        else:
                            # 检测如果一个模组的recipe_valid写入失败，则直接终止所有操作
                            msg = (
                                f'{re_module["blockId"]}-{re_module["index"]}-{re_module["category"]}模组recipe_valid置为'
                                f'True写入失败，终止操作')
                            log.warning(msg)
                            all_success = False
                            await return_request_state(dev, req, 1005)
                            return
            else:
                msg = f"{re_check['ErrMSG']}, 终止操作"
                log.warning(msg)
                all_success = False
                await return_request_state(dev, req, 1005)
                return

        # Merge  with the same device
        for result in results:
            # find same device in results
            same_dev_results = list(filter(lambda x: x['Device'] == result['Device'], results))
            for r in same_dev_results:
                if r != result:
                    result['M2O_list'].extend(r['M2O_list'])
                    result['ErrMSG'].extend(r['ErrMSG'])
                    result['Nodes'] += r['Nodes']
                    # delete same device in results
                    results.remove(r)
            # print(f"合并后：Name:{result['Device'].name} Nodes: {result['Nodes']}  list len:{len(result['M2O_list'])}")
            if len(result['M2O_list']) == 0:
                results.remove(result)
        # write recipe data to all device
        tasks = []
        for re in results:
            print(
                f'{get_current_time()}:开始向{re["Device"].name}下载配方')
            log.info(
                f'开始向{re["Device"].name}下载配方')
            if re['Device'].connecting:
                tasks.append(re['Device'].linker.write_multi_variables(re['M2O_list'], 8))

        recipe_write_states = await asyncio.gather(*tasks)
        all_success = all(recipe_write_states)
        # check all recipe write success or not
        if all_success:
            # 开始给所有模组的Recipe_Valid 写False 操作
            if rv_m2o_list:
                if not await write_all_rv_false(rv_m2o_list, dev, req):
                    return
            print(f'{get_current_time()}: 所有配方下载成功，开始下发recipe_id给MC')
            log.info(f'所有配方下载成功，开始下发recipe_id给MC')
            # recipe_id 再下发一次给MC
            await dev.linker.write_multi_variables([{'datatype': 6,
                                                     'node_id': write_recipe_id,
                                                     'value': recipe_id}], 1.5)
            # await return_request_state(dev, req, 3)
            # 2025/6/23修改: 如果是单flow result的值写1次，多flow result的值重复写5次
            if flow_index is None:
                await return_request_state(dev, req, 3)
            else:
                for _ in range(5):
                    await return_request_state(dev, req, 3)
                # await asyncio.sleep(0.1)
            end_total_time = time.time()  # 记录结束时间
            total_time = end_total_time - start_total_time  # 计算总耗时
            print(f'{get_current_time()}: 下载所有配方所用的总时间: {total_time:.2f} seconds')
            log.info(f'下载所有配方所用的总时间: {total_time:.2f} seconds')
        else:
            log.warning(f'自动下载配方失败-1009，请查看上面写入失败原因')
            await return_request_state(dev, req, 1009)
    else:
        mqtt.publish(mqtt.pub_drv_broadcast, json.dumps({
            "timestamp": get_milliseconds(),
            "type": "RecipeDownloadError",
            "data": datas["message"]
        }), 2)
        await return_request_state(dev, req, datas['code'])


async def write_all_rv_false(rv_m2o_list, dev, req):
    for rv_m2o in rv_m2o_list:
        for current_dev, value in rv_m2o.items():
            if not await current_dev.linker.write_multi_variables(
                    [{'node_id': value["NodeID"],
                      'datatype': value["DataType"],
                      'value': False}], 1.5):  # 先把模组的Recipe_Valid’为False
                # 检测如果一个模组的recipe_valid写入失败，则直接终止所有操作
                msg = (
                    f'{current_dev} recipe_valid置为False写入失败，终止操作')
                print(f'{get_current_time()}: {msg}')
                log.warning(msg)
                await return_request_state(dev, req, 1005)
                return False  # 如果其中有一个模组的Recipe_Valid 写False失败就终止所有操作
    return True
