import asyncio
import json
from datetime import datetime
import pandas as pd
from bigtree import nested_dict_to_tree, tree_to_dataframe

from utils.global_var import GlobalVar
from utils.helpers import upsert_config_file, load_config_file, name_2path
from op_link import op_linker


class opcuaBrowse:

    def __init__(self, opcua_browse_file_path, config_json_data, mqtt_client):
        self.opcua_browse_file_path = opcua_browse_file_path
        self.base_file_path = config_json_data.get("base_file_path")
        self.mqtt_client = mqtt_client
        self.concurrent_recursion = config_json_data.get("concurrent_recursion", False)  # 是否是并发递归,默认是顺序递归
        self.browse_num = 0  # 记录当前遍历模组总数量
        self.is_all_success = False

    def emit_msgs(self, content):
        print(content)

    def emit_finished(self):
        self.is_all_success = True

    def emit_error(self, error_msg):
        print(f"变量遍历报错：{error_msg}")
        self.is_all_success = False

    async def update_dataframe_from_csv(self, df: pd.DataFrame, name: str):
        """
        连接 OPC UA 服务器，根据 CSV 配置文件初始化数据结构并订阅变量
        """
        # 读取旧配置文件
        try:
            df_old = pd.read_csv(f'{self.base_file_path}/{name}.csv', encoding='utf-8')
        except:
            try:
                df_old = pd.read_csv(f'{self.base_file_path}/{name}.csv', encoding='utf-8-sig')
            except:
                try:
                    df_old = pd.read_csv(f'{self.base_file_path}/{name}.csv', encoding='gbk')
                except:
                    self.emit_msgs(f'无旧{name}.csv文件')
                    return False

        # 输出变更信息
        self.emit_msgs(f'变量列表变化: 旧 {len(df_old)} -> 新 {len(df)}')
        self.emit_msgs(f'新增变量数量: {len(df) - len(df_old)}')

        # 设置新 DataFrame 索引以便快速匹配
        df.set_index('path', inplace=True)

        # 处理 read_enable 配置
        if 'read_enable' in df_old.columns:
            read_enable_mask = df_old['read_enable']
            if read_enable_mask.any():
                df_read_enable = df_old[read_enable_mask].set_index('path')
                common_read = df.index.intersection(df_read_enable.index)

                # 批量更新 read 配置
                df.update(df_read_enable[['read_enable', 'read_period']])
                self.emit_msgs(f'read_enable配置更新: 旧 {len(df_read_enable)} 条，已更新 {len(common_read)} 条')
            else:
                self.emit_msgs('未检测到read_enable的配置项')

        # 处理 timed_clear 配置
        if 'timed_clear' in df_old.columns:
            timed_clear_mask = df_old['timed_clear']
            if timed_clear_mask.any():
                df_timed_clear = df_old[timed_clear_mask].set_index('path')
                common_clear = df.index.intersection(df_timed_clear.index)

                # 批量更新 clear 配置
                df.update(df_timed_clear[['timed_clear', 'timed_clear_time']])
                self.emit_msgs(f'timed_clear配置更新: 旧 {len(df_timed_clear)} 条，已更新 {len(common_clear)} 条')
            else:
                self.emit_msgs('未检测到timed_clear的配置项')

        # 重置索引返回原始格式
        df.reset_index(inplace=True)
        return True

    def update_basic_config(self, read_enable_paths, all_config_datas, table_name, opcua_browse_file_path):
        # 查找 table_name 为 指定table_name比如CH_COT_DEV
        for server in all_config_datas['opcua_servers']:
            if server['table_name'] == table_name:
                # 创建一个新的字典来存储更新后的 read_enable_paths
                updated_read_enable_paths = server['read_enable_paths'].copy()

                # 遍历 data，并根据条件更新 updated_read_enable_paths
                for key, value in read_enable_paths.items():
                    if key in updated_read_enable_paths:
                        # 如果 key 已经存在，则保持其原值（不更新为 data 中的值）
                        updated_read_enable_paths[key] = updated_read_enable_paths[key]
                    else:
                        # 如果 key 不存在，则新增该 key 和对应的 value
                        updated_read_enable_paths[key] = value

                # 将更新后的字典赋值
                server['read_enable_paths'] = updated_read_enable_paths
                break  # 找到后退出循环

        # 将更新后的配置写回 JSON 文件
        upsert_config_file(opcua_browse_file_path, all_config_datas)
        updated_config_datas = load_config_file(opcua_browse_file_path)  # 再把配置文件读出来
        opcua_server = None
        for s in updated_config_datas['opcua_servers']:
            if s['table_name'] == table_name:
                opcua_server = s
        return opcua_server

    async def create_opcua_nodes_file(self, config: dict, all_config_datas: dict):
        """
        # browse nodes of opcua server and generate nodes file ("name".csv)
        """
        name = config['table_name']
        uri = config['uri']
        main_node = config['main_node']

        # link to opcua server
        opcua = op_linker(config, self)
        self.emit_msgs(f" {name}: {opcua.uri}开始建立连接...")
        if await opcua.link() is True:
            self.emit_msgs(f' {name}: {uri} 连接成功!')
        else:
            self.emit_msgs(f'  {name}: {uri} 连接失败!')
            return False

        # browse nodes of opcua server and create node tree
        try:

            # node_tree = await self.opcua.browse_all(self.opcua.client.get_node(main_node), '', config)
            read_enable_paths = await opcua.create_config(config)
            updated_config_data = self.update_basic_config(read_enable_paths, all_config_datas, name,
                                                           self.opcua_browse_file_path)
            # print(updated_config_data)
            if main_node == "ns=3;s=DataBlocksGlobal":  # 2_0_PRA单独处理
                node_tree = await opcua.browse_pra(opcua.client.get_node(main_node), '',
                                                   updated_config_data.get('read_enable_paths'))
            else:
                node = opcua.client.get_node(main_node)
                node_name = (await node.read_browse_name()).Name
                node_path = name_2path('', node_name)
                node_tree = await opcua.browse(node, node_path, 0,
                                               updated_config_data.get('read_enable_paths'))
            if not GlobalVar.get_browse_var_state():
                self.emit_msgs(f"停止遍历!!")
                await opcua.unlink()
                return
            self.emit_msgs(f' {name} : {main_node} 遍历成功!')
            await opcua.unlink()
        except:
            self.emit_msgs(f' {name} : {main_node} 遍历失败，请重试!')
            await opcua.unlink()
            return False
        # pprint.pprint(node_tree)

        # create dataframe and save to csv file
        big_tree = nested_dict_to_tree(node_tree)
        # print_tree(big_tree, all_attrs=True)  # 打印出树结构数据
        df = tree_to_dataframe(big_tree, all_attrs=True)

        # fill config information with old csv
        await self.update_dataframe_from_csv(df, name)

        # save to csv file
        try:
            df.to_csv(f'{self.base_file_path}/{name}.csv', encoding='utf_8', index=False)
            self.emit_msgs(f' {self.base_file_path}/{name}.csv 保存成功!')
            if self.browse_num != 0:
                self.browse_num = self.browse_num - 1
                if self.browse_num == 0:
                    # 如果操作成功，发射结束信号
                    self.emit_finished()
        except Exception as e:
            now = datetime.now()
            current_time = str(now.year) + str(now.month) + str(now.day) + str(now.hour) + str(now.minute) + str(
                now.second)
            csv_name = name + '_' + current_time

            try:
                df.to_csv(f'{self.base_file_path}/{csv_name}.csv', encoding='utf_8', index=False)
                self.emit_msgs(f'{self.base_file_path}/{csv_name}.csv 保存成功!')
                if self.browse_num != 0:
                    self.browse_num = self.browse_num - 1
                    if self.browse_num == 0:
                        # 如果操作成功，发射结束信号
                        self.emit_finished()
            except Exception as e:
                self.emit_msgs(f'{csv_name}.csv 保存失败!')
                if self.browse_num != 0:
                    self.browse_num = self.browse_num - 1
                    if self.browse_num == 0:
                        # 如果操作成功，发射失败信号
                        self.emit_error(f'{csv_name}.csv 保存失败: {e}')

    async def create_opcua_nodes_file_gather(self, config: dict, all_config_datas: dict):
        """
        # browse nodes of opcua server and generate nodes file ("name".csv)
        """
        name = config['table_name']
        uri = config['uri']
        main_node = config['main_node']

        # link to opcua server
        opcua = op_linker(config, self)
        self.emit_msgs(f" {name}: {opcua.uri}开始建立连接...")
        if await opcua.link() is True:
            self.emit_msgs(f' {name}: {uri} 连接成功!')
        else:
            self.emit_msgs(f'  {name}: {uri} 连接失败!')
            return False

        # browse nodes of opcua server and create node tree
        try:

            # node_tree = await self.opcua.browse_all(self.opcua.client.get_node(main_node), '', config)
            read_enable_paths = await opcua.create_config(config)
            updated_config_data = self.update_basic_config(read_enable_paths, all_config_datas, name,
                                                           self.opcua_browse_file_path)
            # print(updated_config_data)
            if main_node == "ns=3;s=DataBlocksGlobal":  # 2_0_PRA单独处理
                node_tree = await opcua.browse_pra(opcua.client.get_node(main_node), '',
                                                   updated_config_data.get('read_enable_paths'))
                big_tree = nested_dict_to_tree(node_tree)
                # print_tree(big_tree, all_attrs=True)  # 打印出树结构数据
                df = tree_to_dataframe(big_tree, all_attrs=True)
            else:
                node_tree = await opcua.browse_gather(opcua.client.get_node(main_node), '', 0,
                                                      updated_config_data.get('read_enable_paths'))
                big_tree = nested_dict_to_tree(node_tree)
                # print_tree(big_tree, all_attrs=True)  # 打印出树结构数据
                df = tree_to_dataframe(big_tree, all_attrs=True)
            if not GlobalVar.get_browse_var_state():
                self.emit_msgs(f"停止遍历!!")
                await opcua.unlink()
                return
            self.emit_msgs(f' {name} : {main_node} 遍历成功!')
            await opcua.unlink()
        except Exception as e:
            self.emit_msgs(f' {name} : {main_node} 遍历失败:{e}，请重试!')
            await opcua.unlink()
            return False
        # fill config information with old csv
        await self.update_dataframe_from_csv(df, name)

        # save to csv file
        try:
            df.to_csv(f'{self.base_file_path}/{name}.csv', encoding='utf_8', index=False)
            self.emit_msgs(f' {self.base_file_path}/{name}.csv 保存成功!')
            if self.browse_num != 0:
                self.browse_num = self.browse_num - 1
                if self.browse_num == 0:
                    # 如果操作成功，发射结束信号
                    self.emit_finished()
        except Exception as e:
            now = datetime.now()
            current_time = str(now.year) + str(now.month) + str(now.day) + str(now.hour) + str(now.minute) + str(
                now.second)
            csv_name = name + '_' + current_time

            try:
                df.to_csv(f'{self.base_file_path}/{csv_name}.csv', encoding='utf_8', index=False)
                self.emit_msgs(f'{self.base_file_path}/{csv_name}.csv 保存成功!')
                if self.browse_num != 0:
                    self.browse_num = self.browse_num - 1
                    if self.browse_num == 0:
                        # 如果操作成功，发射结束信号
                        self.emit_finished()
            except Exception as e:
                self.emit_msgs(f'{csv_name}.csv 保存失败!')
                if self.browse_num != 0:
                    self.browse_num = self.browse_num - 1
                    if self.browse_num == 0:
                        # 如果操作成功，发射结束信号
                        self.emit_error(f'{csv_name}.csv 保存失败: {e}')

    async def create_opcua_nodes_file_with_config(self):
        """
        # browse nodes of opcua server and generate config file (*.csv)
        """
        # read opcua config file
        try:
            config_file = open(self.opcua_browse_file_path, 'r', encoding='utf-8')
        except FileNotFoundError as e:
            self.emit_msgs('没有opcua相关配置文件!')
            return False
        try:
            config_data = json.load(config_file)
            ua_list = config_data['opcua_servers']
            # print(ua_list, type(ua_list))

            # list opcua device
            tasks = []
            if self.browse_num != 0:
                self.browse_num = 0
            for opcua_server in ua_list:
                # opcua_server = n['opcua_server']
                is_read = opcua_server['is_read']
                if is_read:
                    self.browse_num += 1
                    if self.concurrent_recursion:  # 如果是True，则并发递归，否则顺序递归
                        tasks.append(self.create_opcua_nodes_file_gather(opcua_server, config_data))
                    else:
                        tasks.append(self.create_opcua_nodes_file(opcua_server, config_data))

            # Run all tasks concurrently
            await asyncio.gather(*tasks)
        except Exception as e:
            print(e)
        return True

    async def start(self):
        """
        browse variable list from opcua device
        """
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)

        if await self.create_opcua_nodes_file_with_config() is True:
            print('Browse complete!')
            if self.is_all_success:
                return True
            else:
                return False
        else:
            print('Browse false!')
            return False
