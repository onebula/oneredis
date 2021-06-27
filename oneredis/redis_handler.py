#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 24 00:25:23 2021

@author: onebula
"""

import time
from itertools import chain
from json import JSONDecodeError
from json import dumps as js_dumps
from json import loads as js_loads

from orjson import loads as orjs_loads
from ciso8601 import parse_datetime
import redis  # pip install redis hiredis
from redis.exceptions import ResponseError


def json_dumps(data):
    # 去掉 json.dumps 后的大量空格，缩短字符串
    data_str = js_dumps(data, separators=(',', ':'))
    return data_str


def json_loads(data_str):
    # 使用orjson加速解析json
    try:
        data = orjs_loads(data_str)
    except JSONDecodeError:
        data = js_loads(data_str)
    return data


class RedisKV(object):
    def __init__(self, name=None, ip=None, port=None, pwd=None, ex=3600,
                 n=None,
                ):
        self.name = name
        if name is not None:
            ip, port = self.address(self.name, ip, port)
        self.connect(ip, port, pwd)
        self.pwd = pwd
        self.ex = ex
        self.n = n
        
    def connect(self, ip, port, pwd):
        pool = redis.ConnectionPool(host=ip, port=port, password=pwd, decode_responses=True)
        self.redis = redis.Redis(connection_pool=pool)
        self.pipe = self.redis.pipeline(transaction=False)
        self.ip, self.port = ip, port
    
    def address(self, name, ip=None, port=None):
        return None, None
    
    def retry(func):
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as err:  # pylint: disable=broad-except
                print(err)
                ip, port = self.address(self.name)
                if ip is not None and port is not None:
                    self.connect(ip, port, self.pwd)
                return func(self, *args, **kwargs)
        return wrapper
    
    @retry
    def setone(self, k, v, ex=None):
        # 赋值
        if ex is None:
            ex = self.ex
        self.redis.set(k, v, ex=ex)
    
    @retry
    def mset(self, kvs, ex=None, n=None):
        # 批量赋值，使用redis的pipeline一次提交多个命令，避免多次请求造成的网络等待
        if not kvs:
            return
        if ex is None:
            ex = self.ex
        if n is None:
            n = self.n
        for idx, k in enumerate(kvs):
            vs = kvs[k]
            if not vs:
                continue
            self.pipe.set(k, vs, ex=ex)
            if n is not None and idx % n == 0:
                self.pipe.execute()
        self.pipe.execute()
        
    @retry
    def getone(self, k):
        # 取值
        v = self.redis.get(k)
        return v
    
    def safe_mget(self, ks):
        # 采用二分递归自适应批量取值，避免单次数据量过大而失败
        if not ks:
            return []
        try:
            vs = self.redis.mget(ks)
        except ResponseError:
            half = int(len(ks) / 2)
            vs = self.safe_mget(ks[0:half]) + self.safe_mget(ks[half:])
        return vs
    
    @retry
    def mget(self, ks, n=None):
        # 批量取值，设置单次最大取值数量n，避免请求失败
        # mget不能使用pipeline(transaction = False)，非事务时的pipeline不具有原子性，多次取值结果返回的先后顺序不固定，导致错配
        # 但是 transaction = True时，即使单条get的pipeline在execute之后会出错，暂时不确定原因，因此弃用pipeline，改用多次mget
        if not ks:
            return []
        if n is None:
            n = self.n
        if n is None:
            vs = self.safe_mget(ks)
        else:
            vs = []
            for i in range(0, len(ks), n):
                vs += self.safe_mget(ks[i:i+n])
        return vs
        
    
class RedisList(object):
    def __init__(self, name=None, ip=None, port=None, pwd=None, ex=3600,
                 n=None,
                ):
        self.name = name
        if name is not None:
            ip, port = self.address(self.name, ip, port)
        self.connect(ip, port, pwd)
        self.pwd = pwd
        self.ex = ex
        self.n = n
    
    def connect(self, ip, port, pwd):
        pool = redis.ConnectionPool(host=ip, port=port, password=pwd, decode_responses=True)
        self.redis = redis.Redis(connection_pool=pool)
        self.pipe = self.redis.pipeline(transaction=False)
        self.ip, self.port = ip, port
    
    def address(self, name, ip=None, port=None):
        return None, None
    
    def retry(func):
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as err:  # pylint: disable=broad-except
                print(err)
                ip, port = self.address(self.name)
                if ip is not None and port is not None:
                    self.connect(ip, port, self.pwd)
                return func(self, *args, **kwargs)
        return wrapper
    
    @retry
    def push(self, list_name, items, ex=None):
        # 选取list的左侧为出口，右侧为入口，这样迭代list时从出口开始迭代
        if not isinstance(items, list):
            items = [items]
        if not items:
            return
        if ex is None:
            ex = self.ex
        self.pipe.rpush(list_name, *items)
        if ex is not None:
            self.pipe.expire(list_name, ex)
        self.pipe.execute()
    
    @retry
    def init_list(self, list_name, items=None, ex=None):
        # 若list_name已经存在，则删除该键
        if self.redis.exists(list_name):
            self.redis.delete(list_name)
        if items is not None:
            self.push(list_name, items, ex)
            
    @retry
    def get_len(self, list_name):
        size = self.redis.llen(list_name)
        return size
    
    def format_start_end(self, list_name, start, end):
        if start >= 0 and end < 0:
            size = self.get_len(list_name)
            end = size + end
        return start, end
    
    def safe_lrange(self, list_name, start, end):
        # 采用二分递归自适应批量取值，避免单次数据量过大而失败
        try:
            vs = self.redis.lrange(list_name, start, end)
        except ResponseError:
            start, end = self.format_start_end(list_name, start, end)
            half = int((start + end) / 2)
            vs = self.safe_lrange(list_name, start, half) + self.safe_lrange(list_name, half, end)
        return vs
    
    @retry
    def slicing(self, list_name, start=0, end=-1, n=None):
        if n is None:
            n = self.n
        if n is None:
            queue = self.safe_lrange(list_name, start, end)
        else:
            queue = []
            start, end = self.format_start_end(list_name, start, end)
            for left in range(start, end + 1, n):
                right = min(end, left + n - 1)
                queue += self.safe_lrange(list_name, left, right)
        return queue
    
    @retry
    def get(self, list_name, n=None):
        res = self.slicing(list_name, 0, -1, n)
        return res
    
    @retry
    def get_last(self, list_name):
        return self.redis.lindex(list_name, -1)
    
    @retry
    def pop(self, list_name):
        item = self.redis.lpop(list_name)
        return item
    

class RedisSet(object):
    def __init__(self, name=None, ip=None, port=None, pwd=None, ex=3600,
                 n=None,
                ):
        self.name = name
        if name is not None:
            ip, port = self.address(self.name, ip, port)
        self.connect(ip, port, pwd)
        self.pwd = pwd
        self.ex = ex
        self.n = n
    
    def connect(self, ip, port, pwd):
        pool = redis.ConnectionPool(host=ip, port=port, password=pwd, decode_responses=True)
        self.redis = redis.Redis(connection_pool=pool)
        self.pipe = self.redis.pipeline(transaction=False)
        self.ip, self.port = ip, port
    
    def address(self, name, ip=None, port=None):
        return None, None
    
    def retry(func):
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as err:  # pylint: disable=broad-except
                print(err)
                ip, port = self.address(self.name)
                if ip is not None and port is not None:
                    self.connect(ip, port, self.pwd)
                return func(self, *args, **kwargs)
        return wrapper
    
    @retry
    def addone(self, set_name, item, ex=None):
        self.pipe.sadd(set_name, item)
        if ex is None:
            ex = self.ex
        if ex is not None:
            self.pipe.expire(set_name, ex)
        self.pipe.execute()
    
    def safe_sadd(self, set_name, items):
        # 采用二分递归自适应批量赋值，避免单次数据量过大而失败
        try:
            self.redis.sadd(set_name, *items)
        except ResponseError:
            half = int(len(items) / 2)
            left_items = items[0:half]
            right_items = items[half:]
            self.safe_sadd(set_name, left_items)
            self.safe_sadd(set_name, right_items)
    
    @retry
    def add(self, set_name, items, ex=None, n=None):
        if not isinstance(items, (set, list)):
            items = [items]
        if not items:
            return
        items = list(items)
        if ex is None:
            ex = self.ex
        if n is None:
            n = self.n
        if n is None:
            self.safe_sadd(set_name, items)
        else:
            for idx in range(0, len(items), n):
                self.safe_sadd(set_name, items[idx:idx + n])
        if ex is not None:
            self.pipe.expire(set_name, ex)
        self.pipe.execute()
        
    @retry
    def update(self, kvs, ex=None, n=None):
        # kvs: {k: [v1, v2, ...], ...}
        if not kvs:
            return
        if ex is None:
            ex = self.ex
        if n is None:
            n = self.n
        for idx, k in enumerate(kvs):
            vs = kvs[k]
            if not vs:
                continue
            self.pipe.sadd(k, *vs)
            if ex is not None:
                self.pipe.expire(k, ex)
            if n is not None and idx % n == 0:
                self.pipe.execute()
        self.pipe.execute()
        
    @retry
    def get(self, set_name):
        items = self.redis.smembers(set_name)
        return items
    
    @retry
    def get_union(self, set_names):
        for set_name in set_names:
            self.pipe.smembers(set_name)
        sets = self.pipe.execute()
        union_sets = set().union(*sets)
        return union_sets
    
    @retry
    def get_size(self, set_name):
        size = self.redis.scard(set_name)
        return size
    
    @retry
    def isin(self, set_name, item):
        flag = self.redis.sismember(set_name, item)
        return flag
    
    @retry
    def empty(self, set_name):
        self.redis.delete(set_name)
        

class RedisZSet(object):
    def __init__(self, name=None, ip=None, port=None, pwd=None, ex=3600,
                 limit_size=0, n=None,
                ):
        self.name = name
        if name is not None:
            ip, port = self.address(self.name, ip, port)
        self.connect(ip, port, pwd)
        self.pwd = pwd
        self.ex = ex
        self.limit_size = limit_size  # limit_size > 0时取 top k，limit_size < 0 时取 bottom k，limit_size = 0 时不限制
        self.n = n
    
    def connect(self, ip, port, pwd):
        pool = redis.ConnectionPool(host=ip, port=port, password=pwd, decode_responses=True)
        self.redis = redis.Redis(connection_pool=pool)
        self.pipe = self.redis.pipeline(transaction=False)
        self.ip, self.port = ip, port
    
    def address(self, name, ip=None, port=None):
        return None, None
    
    def retry(func):
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as err:  # pylint: disable=broad-except
                print(err)
                ip, port = self.address(self.name)
                if ip is not None and port is not None:
                    self.connect(ip, port, self.pwd)
                return func(self, *args, **kwargs)
        return wrapper
    
    @retry
    def addone(self, zset_name, k, v, limit_size=0, ex=None):
        if limit_size == 0:
            limit_size = self.limit_size
        if ex is None:
            ex = self.ex
        self.pipe.zadd(zset_name, {k: v})
        if limit_size > 0:
            self.pipe.zremrangebyrank(zset_name, 0, -limit_size - 1)
        if limit_size < 0:
            self.pipe.zremrangebyrank(zset_name, -limit_size, -1)
        if ex is not None:
            self.pipe.expire(zset_name, ex)
        self.pipe.execute()
    
    def safe_zadd(self, zset_name, kv):
        # 采用二分递归自适应批量赋值，避免单次数据量过大而失败
        try:
            self.redis.zadd(zset_name, kv)
        except ResponseError:
            items = list(kv.items())
            half = int(len(items) / 2)
            self.safe_zadd(zset_name, dict(items[0:half]))
            self.safe_zadd(zset_name, dict(items[half:]))
    
    @retry
    def add(self, zset_name, kv, limit_size=0, ex=None, n=None):
        # kv: {k1: v1, k2: v2, ...}
        if not kv:
            return
        if limit_size == 0:
            limit_size = self.limit_size
        if ex is None:
            ex = self.ex
        if n is None:
            n = self.n
        if n is None:
            self.safe_zadd(zset_name, kv)
        else:
            items = kv.items()
            for idx in range(0, len(kv), n):
                self.safe_zadd(zset_name, dict(items[idx:idx + n]))
        if limit_size > 0:
            self.pipe.zremrangebyrank(zset_name, 0, -limit_size - 1)
        if limit_size < 0:
            self.pipe.zremrangebyrank(zset_name, -limit_size, -1)
        if ex is not None:
            self.pipe.expire(zset_name, ex)
        self.pipe.execute()
        
    @retry
    def update(self, kkv, limit_size=0, ex=None, n=None):
        # kkv: {k1: {kk1: v1, kk2: v2, ...}, ...}
        if not kkv:
            return
        if limit_size == 0:
            limit_size = self.limit_size
        if ex is None:
            ex = self.ex
        if n is None:
            n = self.n
        for idx, k in enumerate(kkv):
            kv = kkv[k]
            if not kv:
                continue
            self.pipe.zadd(k, kv)
            if limit_size > 0:
                self.pipe.zremrangebyrank(k, 0, -limit_size - 1)
            if limit_size < 0:
                self.pipe.zremrangebyrank(k, -limit_size, -1)
            if ex is not None:
                self.pipe.expire(k, ex)
            if n is not None and idx % n == 0:
                self.pipe.execute()
        self.pipe.execute()
    
    @retry
    def get_size(self, zset_name):
        return self.redis.zcard(zset_name)
    
    @retry
    def isin(self, zset_name, k):
        rank = self.redis.zrank(k)
        if rank >= 0:
            return True
        else:
            return False
    
    def format_start_end(self, zset_name, start, end):
        if start >= 0 and end < 0:
            size = self.get_size(zset_name)
            end = size + end
        return start, end
    
    def safe_zrange(self, zset_name, start, end):
        # 采用二分递归自适应批量取值，避免单次数据量过大而失败
        try:
            vs = self.redis.zrange(zset_name, start, end)
        except ResponseError:
            start, end = self.format_start_end(zset_name, start, end)
            half = int((start + end) / 2)
            vs = self.safe_zrange(zset_name, start, half) + self.safe_zrange(zset_name, half, end)
        return vs
    
    def safe_zrangebyscore(self, zset_name, start_v, end_v):
        # 采用二分递归自适应批量取值，避免单次数据量过大而失败
        try:
            vs = self.redis.zrangebyscore(zset_name, start_v, end_v)
        except ResponseError:
            half_v = int((start_v + end_v) / 2)
            vs = \
                self.safe_zrange(zset_name, start_v, half_v) + self.safe_zrange(zset_name, half_v, end_v)
        return vs
    
    @retry
    def slicing(self, zset_name, start=0, end=-1, n=None):
        # 根据编号筛选数据
        if n is None:
            n = self.n
        if n is None:
            res = self.safe_zrange(zset_name, start, end)
        else:
            res = []
            start, end = self.format_start_end(zset_name, start, end)
            for left in range(start, end + 1, n):
                right = min(end, left + n - 1)
                res += self.safe_zrange(zset_name, left, right)
        return res
    
    @retry
    def ranging(self, zset_name, start_v, end_v):
        # 根据范围筛选数据
        res = self.safe_zrangebyscore(zset_name, start_v, end_v)
        return res
    
    @retry
    def ranging_union(self, zset_names, start_v, end_v):
        for zset_name in zset_names:
            self.pipe.zrangebyscore(zset_name, start_v, end_v)
        items_lst = self.pipe.execute()
        union_items = chain(*items_lst)
        return union_items
    
    @retry
    def get(self, zset_name, n=None):
        res = self.slicing(zset_name, 0, -1, n)
        return res
    
    @retry
    def trim(self, zset_name, limit_size=0):
        if limit_size == 0:
            limit_size = self.limit_size
        if limit_size > 0:
            self.pipe.zremrangebyrank(zset_name, 0, -limit_size - 1)
        if limit_size < 0:
            self.pipe.zremrangebyrank(zset_name, -limit_size, -1)
        self.pipe.execute()

        
class RedisTS(object):
    def __init__(self, name=None, ip=None, port=None, pwd=None, ex=3600,
                 limit_len=None, limit_time=None, n=None,
                ):
        self.name = name
        if name is not None:
            ip, port = self.address(self.name, ip, port)
        self.connect(ip, port, pwd)
        self.pwd = pwd
        self.ex = ex
        self.limit_len = limit_len
        self.limit_time = limit_time
        self.n = n
    
    def connect(self, ip, port, pwd):
        pool = redis.ConnectionPool(host=ip, port=port, password=pwd, decode_responses=True)
        self.redis = redis.Redis(connection_pool=pool)
        self.pipe = self.redis.pipeline(transaction=False)
        self.ip, self.port = ip, port
    
    def address(self, name, ip=None, port=None):
        return None, None
    
    def retry(func):
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as err:  # pylint: disable=broad-except
                print(err)
                ip, port = self.address(self.name)
                if ip is not None and port is not None:
                    self.connect(ip, port, self.pwd)
                return func(self, *args, **kwargs)
        return wrapper
    
    def datestr2timestamp(self, datestr):
        # 将字符串格式时间转换成时间戳
        if isinstance(datestr, str):
            return parse_datetime(datestr).timestamp()
        else:
            return datestr
    
    def data2kv(self, ts_data):
        # ts_data: [(t1, data1), (t2, data2), ...]
        ts_kv = {json_dumps([t, data]): self.datestr2timestamp(t)
                 for t, data in sorted(ts_data, key=lambda item: item[0])
                }
        return ts_kv
    
    def res2data(self, res, step=None):
        if step is not None:
            res = res[::step]
        ts_data = [tuple(json_loads(item)) for item in res]
        return ts_data
    
    @retry
    def record(self, ts_name, data, t=None, limit_len=None, ex=None):
        if t is None:
            t = int(time.time())
        ts_data = [(t, data)]
        ts_kv = self.data2kv(ts_data)
        self.pipe.zadd(ts_name, ts_kv)
        if limit_len is None:
            limit_len = self.limit_len
        if limit_len is not None:
            self.pipe.zremrangebyrank(ts_name, 0, -limit_len - 1)
        if ex is None:
            ex = self.ex
        if ex is not None:
            self.pipe.expire(ts_name, ex)
        self.pipe.execute()
    
    def safe_zadd(self, ts_name, kv):
        # 采用二分递归自适应批量赋值，避免单次数据量过大而失败
        try:
            self.redis.zadd(ts_name, kv)
        except ResponseError:
            items = list(kv.items())
            half = int(len(items) / 2)
            self.safe_zadd(ts_name, dict(items[0:half]))
            self.safe_zadd(ts_name, dict(items[half:]))
    
    @retry
    def update_ts(self, ts_name, ts_data, limit_len=None, ex=None, n=None):
        # 更新时间序列，不要求按时间排序
        # ts_data: [(t1, data1), (t2, data2), ...]
        if not ts_data:
            return
        if limit_len is None:
            limit_len = self.limit_len
        if ex is None:
            ex = self.ex
        if n is None:
            n = self.n
        if n is None:
            ts_kv = self.data2kv(ts_data)
            self.safe_zadd(ts_name, ts_kv)
        else:
            for idx in range(0, len(ts_data), n):
                ts_kv = self.data2kv(ts_data[idx:idx + n])
                self.safe_zadd(ts_name, ts_kv)
        if limit_len is not None:
            self.pipe.zremrangebyrank(ts_name, 0, -limit_len - 1)
        if ex is not None:
            self.pipe.expire(ts_name, ex)
        self.pipe.execute()
    
    @retry
    def record_flow(self, ts_flow, limit_len=None, ex=None, n=None):
        # 同时写入多条时间序列的流水数据，不要求按时间排序
        # ts_flow: [(ts_name1, t1, data1), (ts_name2, t2, data2), ...]
        if not ts_flow:
            return
        if limit_len is None:
            limit_len = self.limit_len
        if ex is None:
            ex = self.ex
        if n is None:
            n = self.n
        for idx, (ts_name, t, data) in enumerate(ts_flow):
            ts_data = [(t, data)]
            ts_kv = self.data2kv(ts_data)
            self.pipe.zadd(ts_name, ts_kv)
            if limit_len is not None:
                self.pipe.zremrangebyrank(ts_name, 0, -limit_len - 1)
            if ex is not None:
                self.pipe.expire(ts_name, ex)
            if n is not None and idx % n == 0:
                self.pipe.execute()
        self.pipe.execute()
    
    @retry
    def get_len(self, ts_name):
        # 获取时间序列长度
        return self.redis.zcard(ts_name)
    
    @retry
    def get_last(self, ts_name, only_timestamps=False):
        # 获取最新的数据
        if only_timestamps:
            last = self.redis.zrange(ts_name, start=-1, end=-1, withscores=True)
            if last:
                _, last_time = last[-1]
                return last_time
            else:
                return None
        else:
            last = self.redis.zrange(ts_name, start=-1, end=-1)
            last = self.res2data(last)
            if last:
                last_time, last_data = last[-1]
                return last_time, last_data
            else:
                return None, None
    
    def format_start_end(self, ts_name, start, end):
        if start >= 0 and end < 0:
            size = self.get_len(ts_name)
            end = size + end
        return start, end
    
    def safe_zrange(self, ts_name, start, end):
        # 采用二分递归自适应批量取值，避免单次数据量过大而失败
        try:
            vs = self.redis.zrange(ts_name, start, end)
        except ResponseError:
            start, end = self.format_start_end(ts_name, start, end)
            half = int((start + end) / 2)
            vs = self.safe_zrange(ts_name, start, half) + self.safe_zrange(ts_name, half, end)
        return vs
    
    def safe_zrangebyscore(self, ts_name, start_timestamp, end_timestamp):
        # 采用二分递归自适应批量取值，避免单次数据量过大而失败
        try:
            vs = self.redis.zrangebyscore(ts_name, start_timestamp, end_timestamp)
        except ResponseError:
            half_timestamp = int((start_timestamp + end_timestamp) / 2)
            vs = \
                self.safe_zrangebyscore(ts_name, start_timestamp, half_timestamp) + \
                self.safe_zrangebyscore(ts_name, half_timestamp, end_timestamp)
        return vs
    
    @retry
    def slicing(self, ts_name, start=0, end=-1, n=None, step=None):
        # 根据编号筛选数据
        if n is None:
            n = self.n
        if n is None:
            res = self.safe_zrange(ts_name, start, end)
        else:
            res = []
            start, end = self.format_start_end(ts_name, start, end)
            for left in range(start, end + 1, n):
                right = min(end, left + n - 1)
                res += self.safe_zrange(ts_name, left, right)
        ts_data = self.res2data(res, step)
        return ts_data
    
    @retry
    def ranging(self, ts_name, start_time, end_time, step=None):
        # 根据时间范围筛选数据
        start_timestamp = self.datestr2timestamp(start_time)
        end_timestamp = self.datestr2timestamp(end_time)
        res = self.safe_zrangebyscore(ts_name, start_timestamp, end_timestamp)
        ts_data = self.res2data(res, step)
        return ts_data
    
    @retry
    def get(self, ts_name, only_timestamps=False, n=None):
        if only_timestamps:
            res = self.redis.zrange(ts_name, 0, -1, withscores=True)
            if res:
                ts_data, times = zip(*res)
                return times
            else:
                return []
        else:
            ts_data = self.slicing(ts_name, 0, -1, n)
            return ts_data
        
    @retry
    def sliding_window(self, ts_name, time_window, start_time=None, end_time=None, step=None):
        # 获取滑动窗内的数据
        if start_time is not None:
            start_timestamp = self.datestr2timestamp(start_time)
            end_timestamp = start_timestamp + time_window
            return self.ranging(ts_name, start_timestamp, end_timestamp, step)
        if end_time is not None:
            end_timestamp = self.datestr2timestamp(end_time)
            start_timestamp = end_timestamp - time_window
            return self.ranging(ts_name, start_timestamp, end_timestamp, step)
        end_timestamp = self.get_last(ts_name, only_timestamps=True)
        if end_timestamp:
            start_timestamp = end_timestamp - time_window
            return self.ranging(ts_name, start_timestamp, end_timestamp, step)
        else:
            return []
        
    @retry
    def slicing_trim(self, ts_name, limit_len):
        # 裁剪时间序列，仅保留最近的 limit_len 个数据
        self.redis.zremrangebyrank(ts_name, 0, -limit_len - 1)
        
    @retry
    def ranging_trim(self, ts_name, limit_time):
        # 裁剪时间序列，仅保留最近的 limit_time 范围内的数据
        last = self.get_last(ts_name, with_timestamp=True)
        if last:
            end_timestamp, _ = last
            if end_timestamp > limit_time:
                self.redis.zremrangebyscore(ts_name, 0, end_timestamp - limit_time)
    
    @retry
    def trim(self, ts_name, limit_len=None, limit_time=None):
        # 裁剪时间序列
        if limit_len is None:
            limit_len = self.limit_len
        if limit_len is not None:
            self.slicing_trim(ts_name, limit_len)
        if limit_time is None:
            limit_time = self.limit_time
        if limit_time is not None:
            self.ranging_trim(ts_name, limit_time)
        
        
    
'''RedisTS数据结构
ts_data: [(t1, data1), 
          (t2, data2), 
          ...
         ]
ts_kv: {(t1, data1): timestamp1,
        (t2, data2): timestamp2,
        ...
       }

ts_name: {timestamp1: (t1, data1),
          timestamp2: (t2, data2),
          ...
         }
'''
    
if __name__ == '__main__':
    pass
