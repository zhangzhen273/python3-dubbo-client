# coding=utf-8
"""
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

"""
import time

from dubbo_client import ZookeeperRegistry, DubboClient, DubboClientError, ApplicationConfig

__author__ = 'caozupeng'

if __name__ == '__main__':

    service_interface = 'com.clife.robot.service.interfaces.ForBigDataFacadeService'
    config = ApplicationConfig('clife-business-robot')
    registry = ZookeeperRegistry('200.200.200.55:2181', config, version='2.5.3', group='/clife-v4')
    user_provider = DubboClient(service_interface, registry, version='2.5.3', group='/clife-v4')
    for i in range(1000):
        try:
            print(user_provider.getDevicesInfoByBoxMac('10D07A764E1F'))
        except DubboClientError:
            print('error')
        except ConnectionError:
            print('error')
        time.sleep(5)

