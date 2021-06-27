# OneRedis

简单、快速、安全的高通量python redis接口

- 支持原生Redis的KV、List、Set、ZSet类型
- 基于ZSet类型实现了时间序列类型，支持时间序列的常见操作（流水记录、滑动时间窗、切片筛选等）
- 适用于多节点Redis集群的域名解析及自动重连方法，避免节点搬迁导致连接失败
- 增加批量参数，将请求自适应分割，提供比原生Redis更高通量的数据批量写入和读取方法

安装方式

```shell
pip install oneredis
```

使用方法

```python
from oneredis import RedisKV, RedisList, RedisSet, RedisZSet, RedisTS

# 自定义Redis集群的域名解析方法
def address(name, ip=None, port=None):
    # name: 集群域名
    # ip, port: 默认节点的ip、port
    try:
        ip, port = nameapi(name)
    except Exception:
        pass
    return ip, port

# 创建重连方法，避免redis搬迁导致连接丢失
RedisKV.address = address
RedisList.address = address
RedisSet.address = address
RedisZSet.address = address
RedisTS.address = address

# 实例化类
redis_ts_obj = RedisTS(name=redis_name, 
                       ip=redis_ip, port=redis_port, 
                       pwd=redis_pwd, ex=redis_ex,
                      )

# 使用多个时间序列记录超长流水数据，采用n=1000的批处理大小
#ts_data = [(ts_name1, t1, data1), (ts_name2, t2, data2), ...]
redis_ts_obj.record_flow(ts_data, n=1000)

# 获取时间序列的一小时滑动时间窗内的数据
redis_ts_obj.sliding_window(ts_name, 3600)
```

