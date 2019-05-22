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
from jsonrpc import requests
from jsonrpc import json

from dubbo_client.config import ApplicationConfig
from dubbo_client.registry import Registry
from dubbo_client.registry import ZookeeperRegistry


class DubboClient(object):
    interface = ''
    group = ''
    version = ''

    class _Method(object):

        def __init__(self, client_instance, method):
            self.client_instance = client_instance
            self.method = method

        def __call__(self, *args, **kwargs):
            return self.client_instance.call(self.method, *args, **kwargs)

    def __init__(self, interface, registry, **kwargs):
        assert isinstance(registry, Registry)
        self.interface = interface
        self.registry = registry
        self.group = kwargs.get('group', '')
        self.version = kwargs.get('version', '')
        self.registry.subscribe(interface, group=self.group, version=self.version)
        self.registry.register(interface, group=self.group, version=self.version)

    def call(self, method, *args, **kwargs):
         provider = self.registry.get_random_provider(self.interface, version=self.version, group=self.group)
         data = {
             "method": method,
             "params": args,
             "jsonrpc": "2.0",
         }
         headers = {'content-type': 'application/json'}
         response = requests.post(
             url="http://{0}{1}".format(provider.location),
             data=json.dumps(data),
             headers=headers
         ).json()
         print(response)
         return response



    def __call__(self, method, *args, **kwargs):
        """
        Redirects the direct call to *self.call*
        """
        return self.call(method, *args, **kwargs)

    def __getattr__(self, method):
        """
        Allows the usage of attributes as *method* names.
        """
        return self._Method(client_instance=self, method=method)


if __name__ == '__main__':
    service_interface = 'com.clife.bigdate.service.CityDubboService'
    config = ApplicationConfig('clife-bigdata-business-scene')
    registry = ZookeeperRegistry('200.200.200.55:2181', config, version='2.5.3', group='/clife-v4')
    user_provider = DubboClient(service_interface, registry, version='2.5.3', group='/clife-v4')
    user_provider.findCityByName('A003')
