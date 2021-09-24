# -*- encoding: utf-8 -*-
'''
@File    :   testcase.py
@Time    :   2021/09/22 15:48:29
@Author  :   hongchunhua
@Contact :   需要依赖python3、kazoo组件：pip3 install kazoo
@License :   (C)Copyright 2020-2025
'''

import unittest
from zks_daemon import * 
'''
    测试环境搭建：
        详细见README

'''
class TestZksDaemon(unittest.TestCase):
    MY_ID = 1
    MY_HOST = '172.22.2.2:9639'

    def setUp(self):
        print('setUp...')

    def tearDown(self):
        print('tearDown...')

    def test_init(self):
        with self.assertRaises(TypeError):
            zks = ZksDaemon()
        with self.assertRaises(TypeError):
            zks = ZksDaemon(myid=self.MY_ID)
        with self.assertRaises(TypeError):
            zks = ZksDaemon(myhost=self.MY_HOST)
    
if __name__ == "__main__":
    unittest.main()
            
