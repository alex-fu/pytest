# coding=utf8

import requests

r = requests.get('http://www.baidu.com')
print(r.status_code)
print(r.headers['content-type'])
print(r.headers)
print(r.encoding)
print(r.text[:100])
# print(r.json())
