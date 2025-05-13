import asyncio
import struct
import time

import snap7
from logger import log
from snap7.client import Client as Snap7Client


class s7_linker(object):
    """
    snap7 linker, link opc ua server, browse nodes, subscription and write function
    """

    def __init__(self, config):
        """
        init s7 linker
        """
        # self.uri = re.search(r'//([^:]+):', config['uri'])
        self.uri = config['uri'][10:-5]
        self.main_node = config['main_node']
        self.timeout = config['timeout']
        self.watchdog_interval = config['watchdog_interval']

        self.client = snap7.client.Client()  # create snap7 client for read operation
        self.client_w = snap7.client.Client()  # create snap7 client for write operation

        self.sync = True  # synchronous or asynchronous mode
        self.rw_lock = asyncio.Lock()  # read write lock for asynchronous mode

        self.rw_failure_count = 0  # read write failure count
        self.last_linking_time = 0  # last reading variables or connecting time
        self.linking = False

    async def async_read_db(self, client: Snap7Client, db_number, start, size):
        async with self.rw_lock:
            return await asyncio.get_running_loop().run_in_executor(None, client.read_area,
                                                                    snap7.types.Areas.DB, db_number, start, size)

    async def async_write_db(self, client: Snap7Client, db_number, start, data):
        async with self.rw_lock:
            return await asyncio.get_running_loop().run_in_executor(None, client.write_area,
                                                                    snap7.types.Areas.DB, db_number, start, data)

    async def new_client(self):
        pass
        # self.client = snap7.client.Client()

    async def link(self):
        """
        link to opcua server
        """
        try:
            self.client.connect(self.uri, 0, 1)
            self.client_w.connect(self.uri, 0, 1)
            self.linking = True
            self.rw_failure_count = 0
            self.last_linking_time = int(time.time() * 1000)
            print(f'link to {self.uri}:{self.linking}')
            log.info(f'link to {self.uri}:{self.linking}')
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
            self.client.disconnect()
            # self.client.destroy()
            self.client_w.disconnect()
            # self.client_w.destroy()
            log.info(f'Unlink to {self.uri}, link state is {self.linking}.')
            return True
        except:
            log.warning(f'Failure to unlink to {self.uri}.')
            return False

    async def get_link_state(self):
        """
        get link state
        """
        # linking = self.linking
        # print(f'link state {linking}')
        # self.linking = self.client.get_connected()
        # if self.linking is False and linking is True:
        #     await self.unlink()
        #     print(f'Unlink to {self.uri}:{self.linking}')
        #     log.warning(f'Unlink to {self.uri}:{self.linking}')
        if self.linking is True and self.rw_failure_count > 3:
            await self.unlink()
            self.linking = False
            self.rw_failure_count = 0
            log.warning(f'unlink s7 {self.uri}, link state is {self.linking}')

        return self.linking

    async def write_multi_variables(self, nodes, timeout=0.5):
        """
        Write values to plc via snap7
        :param nodes: write nodes
        :param timeout: timeout for write operation
        :return: True if success, False if failure
        """

        try:
            for node in nodes:
                db = node['s7_db']
                byte_index = node['s7_start']
                bit_index = node['s7_bit']
                value = node['value']
                size = node['s7_size']
                dataTypeString = node['datatype']
                # print(f'write {value} to {db} {byte_index} {bit_index} / {size} ')

                if type(value) == bool:
                    if self.sync is True:
                        tmp_w = self.client_w.read_area(snap7.types.Areas.DB, db, byte_index, size)
                    else:
                        tmp_w = await asyncio.wait_for(self.async_read_db(self.client_w, db, byte_index, size), timeout=timeout)
                    # print(f'before write bool {tmp_w}')
                    snap7.util.set_bool(tmp_w, 0, bit_index, value)
                elif type(value) == str:
                    tmp_w = bytearray(len(value) + 2)
                    snap7.util.set_string(tmp_w, 0, value, len(value))
                elif dataTypeString == 'float':
                    tmp_w = struct.pack('>f', node['value'])
                else:
                    tmp_w = node['value'].to_bytes(size, byteorder='big')
                # print(f'write {tmp_w}')

                # write data to plc via snap7
                if self.sync is True:
                    self.client_w.write_area(snap7.types.Areas.DB, db, byte_index, tmp_w)
                else:
                    await asyncio.wait_for(self.async_write_db(self.client_w, db, byte_index, tmp_w), timeout=timeout)

            self.last_linking_time = int(time.time() * 1000)
            log.info(f'Success to Write {nodes} variables via snap7.')
            return True
        except:
            if self.rw_lock.locked():
                self.rw_lock.release()
            log.warning(f'Failure to write {nodes} variables via snap7.')
            return False

    async def read_multi_variables(self, nodes, timeout=0.2):
        """
        Read values from plc via snap7
        :param nodes: read nodes
        :param timeout: timeout for read operation
        :return: values if success, empty list if failure
        """

        result = []
        try:
            for node in nodes:
                db = node['s7_db']
                byte_index = node['s7_start']
                size = node['s7_size']
                # read data from plc via snap7
                if self.sync is True:
                    value = self.client.read_area(snap7.types.Areas.DB, db, byte_index, size)
                else:
                    value = await asyncio.wait_for(self.async_read_db(self.client, db, byte_index, size), timeout)
                # print('read s7 value:', value)
                result.append(value)  # append value to result list

            self.last_linking_time = int(time.time() * 1000)
            if self.rw_failure_count > 2:
                self.rw_failure_count -= 2
            else:
                self.rw_failure_count = 0

            return result
        except:
            self.rw_failure_count += 1
            result = []  # clear result list if failure
            if self.rw_lock.locked():
                self.rw_lock.release()
            log.warning(f'Failure to read snap7: {nodes}, timeout is {timeout}.')
            return result

    async def subscribe(self):
        return False
