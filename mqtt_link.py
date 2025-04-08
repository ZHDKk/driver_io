import queue
import random
from datetime import datetime
from paho.mqtt import client as mqtt_client
from logger import log
from utils.time_util import get_current_time


class mqtt_linker(object):
    """
    mqtt linker, link to mqtt server(broker) and subscription or publish variables
    """

    def __init__(self, config: dict, topics: dict):
        # mqtt base information and client linker
        self.name = config['name']
        self.url = config['url']
        self.port = config['port']
        self.keepalive = int(config['keepalive'])

        self.id = f'python-mqtt-sub-{random.randint(0, 1000)}'  # 客户端id不能重复
        self.client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2, self.id)
        self.collection_handler = self.collection  # subscription return data handle

        # mqtt topic
        self.sub_gui_msg = topics['sub_gui_msg']
        self.sub_gui_cmd = topics['sub_gui_cmd']
        self.sub_server_cmd = topics['sub_server_cmd']
        self.sub_general_cmd = topics['sub_general_cmd']  # 通常驱动内部使用该topic,用于单设备重连等操作

        self.pub_drv_data = topics['pub_drv_data']
        self.pub_modules_status = topics['pub_modules_status']  # 发布模组的当前连接状态
        self.pub_drv_msg = topics['pub_drv_msg']

        # message queue for mqtt
        self.mq = queue.Queue()  # message queue for mqtt

        # mqtt state
        self.subscription_state = False
        self.connecting = False

    def collection(self, topic: str, data):
        """
        # mqtt subscription collection handle
        """
        self.mq.put({'topic': topic, 'data': data})
        # print(f'mqtt queue:{self.mq}')

    def connect(self):
        """connect mqtt server"""

        def on_connect(client, userdata, flags, rc, extra=None):
            if rc == 0:
                self.subscription()
                self.connecting = True
                print(f"Connected to MQTT, connecting is {self.connecting}.")
                log.info(f"Connected to MQTT, connecting is {self.connecting}.")
            else:
                log.error(f"Connection failed with code {rc}")

        def on_disconnect(client, userdata, flags, rc, properties):
            self.connecting = False
            if rc != 0:
                log.warning(f"Unexpected disconnection (rc={rc})")

        try:
            self.client.on_connect = on_connect
            self.client.on_disconnect = on_disconnect
            self.client.connect(self.url, self.port, self.keepalive)
            self.client.loop_start()  # 启动后台循环线程
        except Exception as e:
            log.error(f"Connection error: {e}")
            self.connecting = False

    def disconnect(self):
        try:
            self.client.loop_stop()  # 停止循环线程
            self.client.disconnect()
            self.connecting = False
            log.info("MQTT client disconnected and loop stopped.")
        except Exception as e:
            print(f"MQTT client disconnected and loop stopped error:{e}")

    def subscribe(self, topic):
        """subscription topic"""

        def on_message(client, userdata, msg):
            self.collection_handler(msg.topic, msg.payload.decode())
            # print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")

        # subscription topic
        self.client.subscribe(topic)
        self.client.on_message = on_message
        print(f'MQTT subscribe new topic:{topic}')
        log.info(f'MQTT subscribe new topic:{topic}')

    # subscribe topic
    def subscription(self):
        """subscription topic"""
        try:
            self.subscribe(self.sub_gui_msg)
            self.subscribe(self.sub_gui_cmd)
            self.subscribe(self.sub_server_cmd)
            self.subscribe(self.sub_general_cmd)
            self.subscription_state = True
        except:
            self.subscription_state = False

    def publish(self, topic, msg, qos = 0):
        """publish topic"""
        try:
            if topic != self.pub_drv_data and topic != self.pub_modules_status:
                re = self.client.publish(topic, msg, qos)
                if re.rc == 0:
                    print(f"{get_current_time()} {topic}:{msg} 通过Mqtt发布:成功")
                    log.info(f"{topic}:{msg} 通过Mqtt发布:成功")
                else:
                    log.warning(f"{topic}:{msg} 通过Mqtt发布:失败")
            else:
                re_msg = self.client.publish(topic, msg)
        except Exception as e:
            log.warning(f"Failure to send message {msg} to topic {topic}, connecting is {self.connecting}，{e}.")