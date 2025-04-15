import asyncio
import paho.mqtt.client as mqtt

from utils.time_util import get_current_time
from vars_browse_remote.logg import log


class MQTTClient:
    def __init__(self, config: dict):
        self.broker_address = config['Mqtt']['Basic']['url']
        self.port = config['Mqtt']['Basic']['port']
        self.keep_alive = config['Mqtt']['Basic']['keepalive']
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect  # 添加断开连接回调
        self.is_connected = False
        self.reconnect_delay = 5  # 每次重连间隔（秒）
        self.message_queue = asyncio.Queue()  # 异步队列，用于存放消息
        self.loop = asyncio.get_event_loop()  # 获取事件循环
        self.sub_general_cmd_bro = config['Mqtt']['Parameter']['sub_general_cmd']
        self.pub_drv_data_struct_bro = config['Mqtt']['Parameter']['pub_drv_data_struct']
        # 直接启动mqtt连接监控
        self.loop.create_task(self.start_connection_monitor(2))

    async def start_connection_monitor(self, interval=5):
        """
        定时检测 MQTT 连接状态，如果断开则尝试重连。

        :param interval: 检测间隔时间（秒）
        """
        while True:
            await asyncio.sleep(interval)
            if not self.is_connected:
                log.warning("MQTT 客户端未连接，尝试重新连接...")
                await self.reconnect()

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            log.info(f"成功连接到 MQTT 服务器: {self.broker_address}:{self.port}")
            self.is_connected = True
            self.subscribe(self.sub_general_cmd_bro)
        else:
            log.error(f"连接到 MQTT 服务器失败，错误代码: {rc}")

    def on_disconnect(self, client, userdata, rc):
        self.is_connected = False

    async def reconnect(self):
        """
        尝试重新连接到 MQTT 服务器（无限次尝试）。
        """
        if self.is_connected:
            return

        while not self.is_connected:
            try:
                log.info("尝试重新连接 MQTT 服务器...")
                self.client.connect(self.broker_address, self.port, self.keep_alive)
                self.client.loop_start()
                await asyncio.sleep(self.reconnect_delay)
                if self.is_connected:
                    print(f"{get_current_time()} 成功重新连接到 MQTT 服务器")
                    log.info("成功重新连接到 MQTT 服务器")
                    return
            except Exception as e:
                log.error(f"重连尝试失败：{e}")

    def on_message(self, client, userdata, msg):
        if msg.topic[:len(self.sub_general_cmd_bro) - 1] == self.sub_general_cmd_bro[
                                                         :len(self.sub_general_cmd_bro) - 1]:  # general_cmd指令处理
            asyncio.run_coroutine_threadsafe(
                self.handle_cmd_msg(msg),
                self.loop
            )

    async def handle_cmd_msg(self, msg):
        """
            message_queue 生产者
        """
        if self.message_queue.full():
            try:
                self.message_queue.get_nowait()  # 丢弃旧消息
            except asyncio.QueueEmpty:
                pass
        await self.message_queue.put(msg)

    def connect(self):
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self.client.connect(self.broker_address, self.port, self.keep_alive)
            self.client.loop_start()
            print("Mqtt 连接成功")
            # # 直接启动mqtt连接监控
            # loop.run_until_complete(self.start_connection_monitor(1))
        except Exception as e:
            log.error(f"Error connecting to MQTT broker: {e}")
            self.is_connected = False

    def subscribe(self, topic):
        """
        订阅
        """
        if self.is_connected:
            try:
                self.client.subscribe(topic)
                log.info(f"Subscribed to topic: {topic}")
            except Exception as e:
                log.error(f"Failed to subscribe to topic {topic}: {e}")
        else:
            log.warning("Cannot subscribe. Client is not connected.")

    def unsubscribe(self, topic):
        """
        取消订阅
        """
        if self.is_connected:
            try:
                self.client.unsubscribe(topic)
                log.info(f"Unsubscribed from topic: {topic}")
            except Exception as e:
                log.error(f"Failed to unsubscribe from topic {topic}: {e}")
        else:
            log.warning("Cannot unsubscribe. Client is not connected.")

    def publish(self, topic, message):
        if self.is_connected:
            try:
                self.client.publish(topic, message)
                log.info(f"Published message '{message}' to topic '{topic}'")
            except Exception as e:
                log.error(f"Failed to publish message to topic {topic}: {e}")
        else:
            log.warning("Cannot publish. Client is not connected.")

    def disconnect(self):
        if self.is_connected:
            self.client.loop_stop()
            self.client.disconnect()
            self.is_connected = False
            log.info("Disconnected from MQTT broker")
        log.info("Processing task stopped")

