import argparse
import socket
from datetime import datetime, timedelta
import _pickle as pickle
import glob
import os
import sys
import uuid


class TaskQueueServer:
    def __init__(self, file_name='logs'):
        self.file_name = file_name
        self.queue_list = []

    def add(self, query_dict):
        element = self._check(query_dict['queue'])
        if not element:
            with open(self.file_name, 'wb') as f:
                self.queue_list.append(query_dict)
                pickle.dump(self.queue_list, f)
                self.queue_list = []
            return query_dict['id'].encode('ascii')
        else:
            return element['id'].encode('ascii')

    def _check(self, query):
        if glob.glob(self.file_name):
            if os.path.getsize(self.file_name) > 0:
                f = open(self.file_name, 'rb')
                self.queue_list = pickle.load(f)
                for elem in self.queue_list:
                    if query == elem['queue']:
                        f.close()
                        return elem
                f.close()
        else:
            open('log', 'wb+').close()
        return False

    def in_queue(self, queue, id):
        element = self._check(queue)
        if not isinstance(element, bool) and element['id'] == id:
            return True
        else:
            return False

    def cure(self, queue):
        queue_from_file = self._check(queue)
        if queue_from_file:  
            with open(self.file_name, 'wb') as f:
                queue_from_file['start_date'] = datetime.now()
                pickle.dump(self.queue_list, f)
            self.timeout(self.queue_list)
            return queue_from_file
        return False

    def ack(self, queue, id_elem):
        element = self._check(queue)
        find_queue = False
        if not isinstance(element, bool):
            if element['id'] == id_elem and element['start_date'] is not None:
                if datetime.now() - element['start_date'] <= timedelta(minutes=5):
                    self.queue_list.remove(element)
                    find_queue = True
            self.timeout(self.queue_list)
            if self.queue_list:
                with open(self.file_name, 'wb') as f:
                    pickle.dump(self.queue_list, f)
            else:
                open(self.file_name, 'wb').close()
        return find_queue

    def timeout(self, queue_list):
        for element in self.queue_list:
            if element['start_date'] is not None and (datetime.now() - element['start_date']) > timedelta(
                    minutes=5):
                element['start_date'] = None        

    @classmethod
    def run(cls):
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        connection.bind(('0.0.0.0', 5555))
        connection.listen(1)
        file = TaskQueueServer('logs')
        while True:
            conn, addr = connection.accept()
            while True:
                try:
                    data = conn.recv(1000000)
                    data_list = data.decode('utf8').split()
                    if data_list[0] == 'ADD':
                        query = {'queue': data_list[1],
                                 'length': data_list[2],
                                 'data': data_list[3],
                                 'start_date': None,
                                 'id': str(uuid.uuid4())}

                        conn.send(file.add(query))
                    if data_list[0] == 'IN':
                        if file.in_queue(data_list[1], data_list[2]):
                            conn.send(b'YES')
                        else:
                            conn.send(b'NO')
                    if data_list[0] == 'GET':
                        result_queue = file.cure(data_list[1])
                        if result_queue:
                            string_for_user = ' '.join(
                                [result_queue['id'], result_queue['length'], result_queue['data']]).encode(
                                'ascii')
                            conn.send(string_for_user)
                        else:
                            conn.send(b'NONE')
                    if data_list[0] == 'ACK':
                        if file.ack(data_list[1], data_list[2]):
                            conn.send(b'YES')
                        else:
                            conn.send(b'NO')
                    if (data_list[0] != 'ACK' and data_list[0] != 'ADD'
                        and data_list[0] != 'GET' and data_list[0] != 'GET'):
                        conn.send(b'ERROR')
                    conn.close()
                    break
                except (IndexError, KeyboardInterrupt):
                    conn.close()
                    sys.exit(0)


if __name__ == '__main__':
    server = TaskQueueServer()
    server.run()
