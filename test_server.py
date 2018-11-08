from unittest import TestCase

import time
import socket

import subprocess

from server import TaskQueueServer
import os
import _pickle as pickle


class ServerBaseTest(TestCase):
    def setUp(self):
        self.server = subprocess.Popen(['python', 'server.py'])
        # даем серверу время на запуск
        time.sleep(0.5)

    def tearDown(self):
        self.server.terminate()
        self.server.wait()

    def send(self, command):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(('127.0.0.1', 5555))
        s.send(command)
        data = s.recv(1000000)
        s.close()
        return data

    def test_base_scenario(self):
        task_id = self.send(b'ADD 1 5 12345')
        
        self.assertEqual(b'YES', self.send(b'IN 1 ' + task_id))

        self.assertEqual(b'OK', self.send(b'SAVE'))

        self.assertEqual(task_id + b' 5 12345', self.send(b'GET 1'))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + task_id))
        self.assertEqual(b'YES', self.send(b'ACK 1 ' + task_id))
        self.assertEqual(b'NO', self.send(b'ACK 1 ' + task_id))
        self.assertEqual(b'NO', self.send(b'IN 1 ' + task_id))
        self.assertEqual(b'OK', self.send(b'SAVE'))

    def test_two_tasks(self):
        first_task_id = self.send(b'ADD 1 5 12345')
        second_task_id = self.send(b'ADD 1 5 12345')
        self.assertEqual(b'YES', self.send(b'IN 1 ' + first_task_id))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + second_task_id))

        self.assertEqual(first_task_id + b' 5 12345', self.send(b'GET 1'))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + first_task_id))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + second_task_id))
        self.assertEqual(second_task_id + b' 5 12345', self.send(b'GET 1'))

        self.assertEqual(b'YES', self.send(b'ACK 1 ' + second_task_id))
        self.assertEqual(b'NO', self.send(b'ACK 1 ' + second_task_id))

    def test_three_tasks(self):
        first_task_id = self.send(b'ADD 12 6 123456')
        second_task_id = self.send(b'ADD 23 7 1234567')
        third_task_id = self.send(b'ADD 90 2 12')
        
        self.assertEqual(b'YES', self.send(b'IN 12 ' + first_task_id))
        self.assertEqual(b'YES', self.send(b'IN 23 ' + second_task_id))
        self.assertEqual(b'YES', self.send(b'IN 90 ' + third_task_id))
        
        self.assertEqual(first_task_id + b' 6 123456', self.send(b'GET 12'))
        self.assertEqual(third_task_id + b' 2 12', self.send(b'GET 90'))
        
        self.assertEqual(b'YES', self.send(b'IN 12 ' + first_task_id))
        self.assertEqual(b'YES', self.send(b'IN 90 ' + third_task_id))
        self.assertEqual(b'YES', self.send(b'IN 23 ' + second_task_id))
        self.assertEqual(b'YES', self.send(b'ACK 90 ' + third_task_id))
        self.assertEqual(b'NO', self.send(b'ACK 23 ' + second_task_id))
        self.assertEqual(b'YES', self.send(b'ACK 12 ' + first_task_id))
        self.assertEqual(b'NO', self.send(b'IN 90 ' + third_task_id))
        self.assertEqual(b'YES', self.send(b'IN 23 ' + second_task_id))
        
        self.assertEqual(second_task_id + b' 7 1234567', self.send(b'GET 23'))
        self.assertEqual(b'YES', self.send(b'ACK 23 ' + second_task_id))
        self.assertEqual(b'NO', self.send(b'IN 23 ' + second_task_id))

    def test_wrong_command(self):
        self.assertEqual(b'ERROR', self.send(b'ADDD 1 5 12345'))


if __name__ == '__main__':
    unittest.main()
