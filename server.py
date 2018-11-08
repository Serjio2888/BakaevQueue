import argparse
import socket
from datetime import datetime, timedelta
import _pickle as pickle
import glob
import os
import sys
import uuid
import argparse


class Running:
    def __init__(self, ip, port, path, timeout):
        self.ip = ip
        self.port = port
        self.path = path
        self.timeout = timeout
        
    def run(self):
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        connection.bind((self.ip, self.port ))
        connection.listen(1)
        file = TaskQueueServer('logs')
        while True:
            conn, addr = connection.accept()
            while True:
                try:
                    data = conn.recv(1000000)
                    data_list = data.decode('utf8').split() 
                    if data_list[0] == 'SAVE':
                        self.check_save(file, conn)     
                    if data_list[0] == 'ADD':
                        self.check_add(conn, file, data_list)
                    if data_list[0] == 'IN':
                        self.check_in(data_list, file, conn)
                    if data_list[0] == 'GET':
                        self.check_get(conn, file, data_list)
                    if data_list[0] == 'ACK':
                        self.check_ack(conn, file, data_list)
                    if (data_list[0] != 'ACK' and data_list[0] != 'ADD'
                        and data_list[0] != 'GET' and data_list[0] != 'SAVE'):
                        conn.send(b'ERROR')
                    conn.close()
                    break
                except (IndexError, KeyboardInterrupt):
                    conn.close()
                    sys.exit(0)
                    
    def check_save(self,file,  conn):
        file.save()
        return conn.send(b'OK')
        
    def check_in(self, data_list, file, conn):
        if file.in_queue(data_list[1], data_list[2]):
            return conn.send(b'YES')
        else:
            return conn.send(b'NO')

    def check_get(self, conn, file, data_list):
        result_queue = file.cure(data_list[1])
        if result_queue:
            string_for_user = ' '.join(
                [result_queue['id'], result_queue['length'], result_queue['data']]).encode(
                'ascii')
            return conn.send(string_for_user)
        else:
            return conn.send(b'NONE')

    def check_add(self, conn, file, data_list):
        query = {'queue': data_list[1],
                'length': data_list[2],
                'data': data_list[3],
                'start_date': None,
                'id': str(uuid.uuid4())}
        conn.send(file.add(query))
    
    def check_ack(self, conn, file, data_list):
        if file.ack(data_list[1], data_list[2]):
            conn.send(b'YES')
        else:
            conn.send(b'NO')

            
class TaskQueueServer(Running):
    def __init__(self, file_name='logs'):
        self.file_name = file_name
        self.queue_list = []


    def save(self):
        with open(self.file_name, 'wb') as f:
            pickle.dump(self.queue_list, f)#      
        
    def add(self, query_dict):
        element = self._check(query_dict['queue'])
        if not element:
            with open(self.file_name, 'wb') as f:
                self.queue_list.append(query_dict)
                pickle.dump(self.queue_list, f)#
                self.queue_list = []
            return query_dict['id'].encode('ascii')
        else:
            return element['id'].encode('ascii')

    def _check(self, query):
        if glob.glob(self.file_name):
            if os.path.getsize(self.file_name) > 0:
                f = open(self.file_name, 'rb')
                self.queue_list = pickle.load(f)#
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
                pickle.dump(self.queue_list, f)#
            return queue_from_file
        return False

    def ack(self, queue, id_elem):
        element = self._check(queue)
        find_queue = False
        if not isinstance(element, bool):
            if element['id'] == id_elem and element['start_date'] is not None:
                if datetime.now() - element['start_date'] <= timedelta(seconds=server.timeout):
                    self.queue_list.remove(element)
                    find_queue = True
            if self.queue_list:
                with open(self.file_name, 'wb') as f:
                    pickle.dump(self.queue_list, f)#
            else:
                open(self.file_name, 'wb').close()
        return find_queue
                
                
def parse_args():
    parser = argparse.ArgumentParser(description='This is a simple task queue server with custom protocol')
    parser.add_argument(
        '-p',
        action="store",
        dest="port",
        type=int,
        default=5555,
        help='Server port')
    parser.add_argument(
        '-i',
        action="store",
        dest="ip",
        type=str,
        default='0.0.0.0',
        help='Server ip adress')
    parser.add_argument(
        '-c',
        action="store",
        dest="path",
        type=str,
        default='./',
        help='Server checkpoints dir')
    parser.add_argument(
        '-t',
        action="store",
        dest="timeout",
        type=int,
        default=300,
        help='Task maximum GET timeout in seconds')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    server = Running(**args.__dict__)
    server.run()
