import asyncio
import json
import sys

from logger import log
from mqtt_client import MQTTClient
from op_browse import opcuaBrowse
from utils.global_var import GlobalVar
from utils.helpers import load_config_file, json_from_list, save_config_file
from utils.time_util import get_current_time


async def process_cmd_msg_coroutine(opcua_browse_file_path, config_file_path, config_json_data, mqtt_client, opcua_browse):
    """
    异步处理消息队列中的消息。
    """
    ios_config_data = load_config_file(
        f"{config_json_data['base_file_path']}/{config_json_data['Drv']['config_file_name']}")
    basic_data = ios_config_data.get("Basic")
    ios_block_id = basic_data.get("blockId")
    ios_block_index = basic_data.get("index")
    ios_category = basic_data.get("category")
    obbc_module = {"blockId": ios_block_id, "index": ios_block_index,
                      "category": "OBBCData"}
    obc_module = {"blockId": ios_block_id, "index": ios_block_index,
                      "category": "OBCData"}
    while True:
        try:
            msg = await mqtt_client.message_queue.get()
            try:
                msg_decode = json.loads(msg.payload.decode())
                data = msg_decode.get("data", "")
                if len(data) == 0:return
                topic = msg.topic
                if data.get("commandType") == "MODIFY_OPCUA_BROWSE_CONFIG":  # 修改配置内容
                    command_content = data.get("commandContent")
                    module = {"blockId": data.get("blockId", ""), "index": data.get("index", ""),
                              "category": data.get("category", "")}
                    if module == obbc_module:
                        state = save_config_file(opcua_browse_file_path, command_content)
                        if state:
                            mqtt_client.publish(topic + '/reply',
                                                json.dumps({'success': True, 'message': 'OBBCData配置内容修改成功'}))  # 修改basic_config.json
                        else:
                            mqtt_client.publish(topic + '/reply',
                                                json.dumps({'success': False, 'message': 'OBBCData配置内容修改失败，请重试'}))
                    elif module == obc_module:
                        state = save_config_file(config_file_path, command_content)
                        if state:
                            mqtt_client.publish(topic + '/reply',
                                                json.dumps({'success': True, 'message': 'OBCData配置内容修改成功'}))  # 修改config.json
                        else:
                            mqtt_client.publish(topic + '/reply',
                                                json.dumps({'success': False, 'message': 'OBCData配置内容修改失败，请重试'}))
                    # else:
                    #     mqtt_client.publish(topic + '/reply',
                    #                       json.dumps({'success': False,
                    #                                   'message': f'未匹配到模组：{module["blockId"]}_{module["index"]}_{module["category"]}'}))
                elif data.get("commandType") == "START_OPCUA_BROWSE":
                    module = {"blockId": data.get("blockId", ""), "index": data.get("index", ""),
                              "category": data.get("category", "")}
                    if module == {"blockId": ios_block_id, "index": ios_block_index, "category": ios_category}:
                        GlobalVar.set_browse_var_state(True)
                        if await opcua_browse.start():
                            mqtt_client.publish(topic + '/reply',
                                                json.dumps({'success': True, 'message': f'{module["blockId"]}_{module["index"]}_{module["category"]}遍历变量成功，请重新启动主程序'}))
                        else:
                            mqtt_client.publish(topic + '/reply',
                                                json.dumps({'success': False,
                                                            'message': f'{module["blockId"]}_{module["index"]}_{module["category"]}遍历变量失败，请重新尝试'}))
                    # else:
                    #     mqtt_client.publish(topic + '/reply',
                    #                       json.dumps({'success': False,
                    #                                   'message': f'未匹配到模组：{module["blockId"]}_{module["index"]}_{module["category"]}'}))
            except json.JSONDecodeError as e:
                log.error(f"JSON解析错误：{e}")
            except Exception as e:
                log.error(f"处理消息时发生错误：{e}", exc_info=True)
            finally:
                mqtt_client.message_queue.task_done()
        except asyncio.CancelledError:
            log.info("MQTT处理任务被取消")
            # 可以在这里添加更多的清理逻辑
            raise
        except Exception as e:
            log.error(f"MQTT处理过程中发生意外错误：{e}", exc_info=True)
        if mqtt_client.message_queue.empty():
            await asyncio.sleep(0.02)

async def opcua_manager_coroutine(config_json_data, opcua_browse_json_data, mqtt_client):
    ios_config_data = load_config_file(f"{config_json_data['base_file_path']}/{config_json_data['Drv']['config_file_name']}")
    basic_data = ios_config_data.get("Basic")
    ios_block_id = basic_data.get("blockId")
    ios_block_index = basic_data.get("index")
    while True:
        obbc_frame = json_from_list({'module': {"blockId": ios_block_id,
                                                        "index": ios_block_index,
                                                        "category": "OBBCData"},  #  OpcuaBrowseBasicConfigData
                                             'list': opcua_browse_json_data})
        if obbc_frame:
            mqtt_client.publish(mqtt_client.pub_drv_data_struct_bro, obbc_frame)

        obc_frame = json_from_list({'module': {"blockId": ios_block_id,
                                                        "index": ios_block_index,
                                                        "category": "OBCData"},  # OpcuaBrowseConfigData
                                             'list': config_json_data})
        if obc_frame:
            mqtt_client.publish(mqtt_client.pub_drv_data_struct_bro, obc_frame)
        await asyncio.sleep(2)
        

async def main():
    opcua_browse_file_path = '../vars_browse_remote/config_files/basic_config.json'
    config_file_path = '../vars_browse_remote/config_files/config.json'
    mqtt_client = None
    opcua_browse = None
    opcua_browse_json_data = {}
    config_json_data = {}
    # 加载配置
    try:
        opcua_browse_json_data = load_config_file(opcua_browse_file_path)
        config_json_data = load_config_file(config_file_path)
    except Exception as e:
        log.warning(f"加载配置文件失败: {str(e)}")
        sys.exit(1)

    if not opcua_browse_json_data or not config_json_data:
        opcua_browse_file_path = './vars_browse_remote/config_files/basic_config.json'
        config_file_path = './vars_browse_remote/config_files/config.json'
        opcua_browse_json_data = load_config_file(opcua_browse_file_path)
        config_json_data = load_config_file(config_file_path)

    try:
        mqtt_client = MQTTClient(config_json_data)
        mqtt_client.connect()
        await asyncio.sleep(2)
    except Exception as e:
        log.warning("MQTT 错误", f"MQTT 初始化失败：{e}")
    try:
        opcua_browse = opcuaBrowse(opcua_browse_file_path, config_json_data, mqtt_client)
        # asyncio.create_task(opcua_browse.start())  # 开始执行遍历
    except Exception as e:
        log.warning( f"opcuaBrowse 初始化失败：{e}")
    asyncio.create_task(opcua_manager_coroutine(config_json_data, opcua_browse_json_data, mqtt_client))
    asyncio.create_task(process_cmd_msg_coroutine(opcua_browse_file_path, config_file_path, config_json_data,
                                                  mqtt_client, opcua_browse))  # 处理cmd指令
    print(f"{get_current_time()}: 遍历变量程序已启动")
    log.info(f"{get_current_time()}: 遍历变量程序已启动")
    await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("程序被用户中断，正在清理资源并退出...")
        log.info("程序被用户中断，正在清理资源并退出...")
        sys.exit(0)