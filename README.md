<div align="center">

# WlfUtil

*Common utils for python.*

[![](https://img.shields.io/badge/pypi-latest-9cf.svg)](https://pypi.org/project/wlfutil/) [![](https://img.shields.io/badge/blog-@waisaa-blue.svg)](https://blog.csdn.net/qq_42761569?type=blog) [![](https://img.shields.io/badge/license-MIT-brightgreen.svg)](https://github.com/waisaa/wlf-util/blob/main/LICENSE)

</div>

WlfUtil provides a series of imperative functions that help deal with mysql, redis, influxdb and so on.

## Utils
| util | func |
|:---------:|:---------:|
| LogUtil | 日志打印工具类 |
| FileUtil | 文件目录操作工具类|
| ConfUtil | 配置文件操作工具类|
| UniUtil | 通用工具类|
| DtUtil | 日期操作工具类|
| InfluxUtil | Influxdb操作工具类|
| MysqlUtil | Mysql操作工具类|
| ShellUtil | Shell操作工具类|
| MinioUtil | Minio操作工具类|
| RedisUtil | Redis操作工具类|

## Installation
```python3
pip3 install wlfutil
```

## Quickuse
```python3
# 引入所有工具类
#from wlfutil.all import *
# 引入指定的
from wlfutil.all import LogUtil, FileUtil

log_file = 'test/run.log'
FileUtil.del_dir_or_file(log_file)
LogUtil.init(log_file, True)

LogUtil.info('title', 'this is a test')
```
