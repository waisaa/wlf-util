import datetime as dt
from dateutil.relativedelta import relativedelta
from influxdb import InfluxDBClient
import shutil
import logging
from logging import handlers
import colorlog
import os
import configparser
from copy import deepcopy
from inspect import currentframe
import time
import locale
import platform
import pymysql
import paramiko
import minio
import redis
import uuid


class UniUtil:
    """统一处理工具类"""

    @staticmethod
    def get_uuid(params: dict):
        """基于名字的MD5散列值，同一命名空间的同一名字生成相同的uuid"""
        p1 = sorted(params.items(), key=lambda x: x[0])
        p2 = [str(p) for p in p1]
        p3 = '|'.join(p2)
        return str(uuid.uuid3(uuid.NAMESPACE_OID, p3))

    @staticmethod
    def time_cost(fn):
        """这个装饰器用于统计函数运行耗时"""

        def _timer(*args, **kwargs):
            func_name = fn.__name__
            LogUtil.info('start', func_name)
            start = time.perf_counter()
            result = fn(*args, **kwargs)
            end = time.perf_counter()
            cost = _fmt(end - start)
            LogUtil.info('end', func_name)
            LogUtil.info('cost', cost)
            return result

        def _fmt(sec):
            """格式化打印时间，大于60秒打印分钟，大于60分钟打印小时"""
            return f'{round(sec, 2)}s' if sec <= 60 else f'{round(sec / 60, 2)}m' if sec <= 3600 else f'{round(sec / 3600, 2)}h'

        return _timer

    @staticmethod
    def range_partition(max, partitions=10, min=0):
        """根据数据范围划分区间
        :param min 最小值，默认值0
        :param max 最大值
        :param partitions 划分的区间数
        :return 返回划分后的区间集合 ['区间1', '区间2', '区间3', ...]
        """
        res = []
        interval = max / partitions
        while len(res) < partitions:
            start, end = min, min + interval
            res.append(f'{start:.1f} ~ {end:.1f}') if end < max else res.append(f'>= {min:.1f}')
            min += interval
        return res

    @staticmethod
    def date_partition(freq, start_time, end_time):
        """根据数据频率划分时间区间
        :param freq 数据频率
        :return 返回划分后的时间区间集合 {}
        """
        res = {}
        begin = DtUtil.day_start_of_date_str(start_time)
        end = DtUtil.day_end_of_date_str(end_time)
        while begin < end:
            res[begin] = None
            begin = begin + relativedelta(seconds=freq)
        return res

    @staticmethod
    def del_none(li):
        """删除list中None"""
        return [e for e in li if e]

    @staticmethod
    def get_os():
        """获取当前操作系统"""
        return platform.system()

    @staticmethod
    def to_str(bytes_or_str):
        """
        把byte类型转换为str
        :param bytes_or_str:
        :return:
        """
        if isinstance(bytes_or_str, bytes):
            value = bytes_or_str.decode('utf-8')
        else:
            value = bytes_or_str
        return value


class ConfUtil:
    """配置文件【config.ini】操作工具类"""
    CONN = None

    @classmethod
    def _init(cls, conf):
        if cls.CONN is None:
            cls.connect(conf)

    @classmethod
    def connect(cls, conf):
        try:
            cls.CONN = configparser.ConfigParser()
            cls.CONN.read(conf, encoding='utf-8')
        except Exception as e:
            LogUtil.error("conf init failed, please check the config", e)

    @classmethod
    def get_items(cls, conf, section):
        """获取某一章节的所有信息"""
        res = {}
        for item in cls.CONN.items(section):
            res[item[0]] = item[1]
        return res

    @classmethod
    def get_value(cls, conf, section, key):
        """根据章节id获取图片最终序号"""
        res = None
        if str(key) in cls.CONN.options(section):
            res = cls.CONN.get(section, str(key))
        return res

    @classmethod
    def set_value(cls, conf, section, key, value):
        """根据章节id设置图片最终序号"""
        cls.CONN.set(section, str(key), str(value))
        cls.CONN.write(open(conf, "w", encoding='utf-8'))


class FileUtil:
    """目录、文件操作工具类"""

    @staticmethod
    def create_dir_if_not_exist(dst_dir: str):
        """创建目录，不存在则创建，存在无操作
        :param dst_dir 要创建的目录
        """
        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir)

    @staticmethod
    def del_dir_or_file(dst_fd: str):
        """删除文件或目录
        :param src_fd 要删除的目录或文件
        """
        if os.path.isdir(dst_fd):
            shutil.rmtree(dst_fd)
        elif os.path.isfile(dst_fd):
            os.remove(dst_fd)

    @staticmethod
    def get_file_size(filepath: str):
        """获取文件或文件夹的大小
        注意：TB级别以及超过TB的数据就别用了，需要考虑性能了
        """
        res = 0
        # 判断输入是文件夹还是文件
        if os.path.isdir(filepath):
            # 如果是文件夹则统计文件夹下所有文件的大小
            for file in os.listdir(filepath):
                res += os.path.getsize(f'{filepath}/{file}')
        elif os.path.isfile(filepath):
            # 如果是文件则直接统计文件的大小
            res += os.path.getsize(filepath)
        # 格式化返回大小
        bu = 1024
        if res < bu:
            res = f'{bu}B'
        elif bu <= res < bu**2:
            res = f'{round(res / bu, 3)}KB'
        elif bu**2 <= res < bu**3:
            res = f'{round(res / bu**2, 3)}MB'
        elif bu**3 <= res < bu**4:
            res = f'{round(res / bu**3, 3)}GB'
        elif bu**4 <= res < bu**5:
            res = f'{round(res / bu**4, 3)}TB'
        return res

    @staticmethod
    def clear_dir(filepath: str):
        """清空文件夹下的所有文件，先删除文件夹再创建"""
        if not os.path.exists(filepath):
            os.mkdir(filepath)
        else:
            shutil.rmtree(filepath)
            os.mkdir(filepath)


class DtUtil:
    """日期格式化工具类"""

    # 日期格式
    DF_STD_MIC = '%Y-%m-%d %H:%M:%S.%f'
    DF_STD_SEC = '%Y-%m-%d %H:%M:%S'
    DF_STD_DAY = '%Y-%m-%d'
    DF_TRIM_DAY = '%Y%m%d'
    DF_TRIM_MON = '%Y%m'
    DF_CHN_DAY = '%Y年%m月%d日'
    DF_CHN_MON = '%Y年%m月'
    DF_CUS_MIN = '%Y_%m%d_%H%M'
    DF_INFLUX = '%Y-%m-%dT%H:%M:%SZ'

    # 日期单位
    DU_SEC = 'sec'
    DU_MIN = 'min'
    DU_HOUR = 'hour'
    DU_DAY = 'day'
    DU_YEAR = 'year'

    # windows 环境下需要配置
    if UniUtil.get_os() == 'Windows':
        locale.setlocale(locale.LC_CTYPE, 'chinese')

    @staticmethod
    def convert_date_str_format(src_dt_str: str, src_df: str = DF_STD_SEC, dst_df: str = DF_CHN_DAY):
        """把日期字符串格式化成其他格式的日期字符串
        @param src_dt_str:源日期字符串
        @param src_df:源日期字符串格式
        @param dst_df:目标日期字符串格式
        """
        return dt.datetime.strftime(dt.datetime.strptime(src_dt_str, src_df), dst_df)

    @staticmethod
    def convert_date_to_str(src_dt: dt.datetime = dt.datetime.now(), dst_df: str = DF_STD_SEC):
        """把日期式化成其他格式的日期字符串
        @param src_dt:源日期
        @param dst_df:目标日期字符串格式
        """
        return dt.datetime.strftime(src_dt, dst_df)

    @staticmethod
    def convert_str_to_date(src_dt_str: str, src_df: str = DF_STD_SEC):
        """把日期字符串格式化成日期
        @param src_dt_str:源日期字符串
        @param src_df:源日期字符串格式
        """
        res = dt.datetime.strptime(src_dt_str, src_df)
        if src_df == DtUtil.DF_INFLUX:
            res += relativedelta(hours=8)
        return res

    @staticmethod
    def shift_date(src_dt: dt.datetime = dt.datetime.now(), mon: int = 0, day: int = 0, sec: int = 0):
        """获取今天日期，根据传入的偏移量偏移
        @param mon:今天日期按月偏移量
        @param day:今天日期按日偏移量
        @param day:今天日期按秒偏移量
        """
        return src_dt + relativedelta(months=mon) + relativedelta(days=day) + relativedelta(seconds=sec)

    @staticmethod
    def diff_time(src_dt_str1: str, src_dt_str2: str, src_df: str = DF_STD_SEC, diff_unit=DU_SEC):
        """获取两个日期字符串的时间差，默认单位秒
        @param src_dt_str1:日期字符串
        @param src_dt_str2:日期字符串
        @param src_df:源日期字符串格式
        @return 正值代表第二个比第一个晚几秒，负值则相反
        """
        ts1 = DtUtil.convert_str_to_date(src_dt_str1, src_df).timestamp()
        ts2 = DtUtil.convert_str_to_date(src_dt_str2, src_df).timestamp()
        diff_secs = int(ts2 - ts1)
        if diff_unit == DtUtil.DU_SEC:
            return diff_secs
        elif diff_unit == DtUtil.DU_MIN:
            return round(diff_secs / 60, 1)
        elif diff_unit == DtUtil.DU_HOUR:
            return round(diff_secs / (60 * 60), 1)
        elif diff_unit == DtUtil.DU_DAY:
            return round(diff_secs / (60 * 60 * 24), 1)
        elif diff_unit == DtUtil.DU_YEAR:
            return round(diff_secs / (60 * 60 * 24 * 365), 1)

    @staticmethod
    def day_start_of_date_str(src_dt_str: str, src_df: str = DF_STD_SEC):
        """获取某一天的起始时间
        @param src_dt_str:日期字符串
        @param src_df:源日期字符串格式
        @return 某一天的起始时间
        """
        d1 = DtUtil.convert_str_to_date(src_dt_str, src_df)
        return DtUtil.convert_str_to_date(DtUtil.convert_date_to_str(d1, DtUtil.DF_STD_DAY), DtUtil.DF_STD_DAY)

    @staticmethod
    def day_end_of_date_str(src_dt_str: str, src_df: str = DF_STD_SEC):
        """获取某一天的结束时间，即第二天的开始时间
        @param src_dt_str:源日期字符串
        @param src_df:源日期字符串格式
        @return 某一天的起始时间
        """
        d1 = DtUtil.convert_str_to_date(src_dt_str, src_df) + relativedelta(days=1)
        return DtUtil.convert_str_to_date(DtUtil.convert_date_to_str(d1, DtUtil.DF_STD_DAY), DtUtil.DF_STD_DAY)

    @staticmethod
    def over_shift(src_dt1: dt.datetime, src_dt2: dt.datetime, shift: int = 60):
        """判断两个时间是否相差是否超过偏移量
        @param src_dt1:源日期
        @param src_dt2:源日期
        @param shift:偏移量，单位秒
        @return 返回 True | False
        """
        return abs((src_dt1 - src_dt2).total_seconds()) > shift

    @staticmethod
    def get_date_of_min(src_dt_str: str):
        """获取当前时间自定义格式-分钟级时间
        @param src_dt_str:源日期字符串
        """
        return DtUtil.convert_date_to_str(dst_df=DtUtil.DF_CUS_MIN)


class LogUtil:
    """日志工具类"""
    logger = None
    format = '%(asctime)s %(levelname)s: %(message)s'
    colors = {
        'DEBUG': 'cyan',
        'INFO': 'blue',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red,bg_white',
    }
    # 颜色格式
    fmt_colored, fmt_colorless = None, None
    # 日志输出端
    console_handler, file_handler = None, None

    @classmethod
    def init(cls, logname: str, console: bool = False):
        """使用前需要初始化，输入生成的日志文件名
        注意：默认按天生成日志，且保留最近一周的日志文件
        """
        if not cls.logger:
            pdir = '/'.join(logname.split('/')[:-1])
            if pdir:
                FileUtil.create_dir_if_not_exist(pdir)
            cls.logger = logging.getLogger(logname)
            cls.logger.setLevel(logging.DEBUG)
            # 有颜色格式
            cls.fmt_colored = colorlog.ColoredFormatter(f'%(log_color)s{cls.format}', datefmt=None, reset=True, log_colors=cls.colors)
            # 无颜色格式
            cls.fmt_colorless = logging.Formatter(cls.format)
            # 输出到控制台和文件
            if console:
                cls.console_handler = logging.StreamHandler()
            cls.file_handler = handlers.TimedRotatingFileHandler(filename=logname, when='D', backupCount=3, encoding='utf-8')

    @classmethod
    def open(cls):
        if cls.logger:
            if cls.console_handler:
                cls.console_handler.setFormatter(cls.fmt_colored)
                cls.logger.addHandler(cls.console_handler)
            if cls.file_handler:
                cls.file_handler.setFormatter(cls.fmt_colored)
                cls.logger.addHandler(cls.file_handler)
        else:
            print('Please init LogUtil first!')

    @classmethod
    def close(cls):
        if cls.console_handler:
            cls.logger.removeHandler(cls.console_handler)
        cls.logger.removeHandler(cls.file_handler)

    @classmethod
    def debug(cls, title: str = None, *msg):
        cls.open()
        lastframe = currentframe().f_back
        filepath = lastframe.f_code.co_filename
        funcn = lastframe.f_code.co_name
        lineo = lastframe.f_lineno
        cls.logger.debug("< {} >".format(title).center(100, "-"))
        cls.logger.debug(f'< {funcn} - {lineo} >')
        if msg or msg == 0 or msg is False:
            cls.logger.debug(msg)
        cls.logger.debug("")
        cls.close()

    @classmethod
    def info(cls, title: str = None, *msg):
        cls.open()
        lastframe = currentframe().f_back
        filepath = lastframe.f_code.co_filename
        funcn = lastframe.f_code.co_name
        lineo = lastframe.f_lineno
        if title:
            cls.logger.info("< {} >".format(title).center(100, "-"))
            cls.logger.info(f'< {funcn} - {lineo} >')
            if msg or msg == 0 or msg is False:
                cls.logger.info(msg)
        cls.logger.info("")
        cls.close()

    @classmethod
    def warn(cls, title: str = None, *msg):
        cls.open()
        lastframe = currentframe().f_back
        filepath = lastframe.f_code.co_filename
        funcn = lastframe.f_code.co_name
        lineo = lastframe.f_lineno
        if title:
            cls.logger.warn("< {} >".format(title).center(100, "-"))
            cls.logger.warn(f'< {funcn} - {lineo} >')
            if msg or msg == 0 or msg is False:
                cls.logger.warn(msg)
        cls.logger.warn("")
        cls.close()

    @classmethod
    def error(cls, title: str = None, *msg):
        cls.open()
        lastframe = currentframe().f_back
        filepath = lastframe.f_code.co_filename
        funcn = lastframe.f_code.co_name
        lineo = lastframe.f_lineno
        if title:
            cls.logger.error("< {} >".format(title).center(120, "#"))
            cls.logger.error(f'< {funcn} - {lineo} >')
            if msg or msg == 0 or msg is False:
                cls.logger.error(msg)
        cls.logger.error("")
        cls.close()

    @classmethod
    def critical(cls, title: str = None, *msg):
        cls.open()
        lastframe = currentframe().f_back
        filepath = lastframe.f_code.co_filename
        funcn = lastframe.f_code.co_name
        lineo = lastframe.f_lineno
        if title:
            cls.logger.critical("< {} >".format(title).center(120, "#"))
            cls.logger.critical(f'< {funcn} - {lineo} >')
            if msg or msg == 0 or msg is False:
                cls.logger.critical(msg)
        cls.logger.critical("")
        cls.close()


class InfluxUtil:
    """influxdb工具类

    conf_influx = {
        'host': '110.110.110.110',
        'port': 8086,
        'username': 'admin',
        'password': '123456',
        'database': 'db_test',
    }
    """
    TZ = "tz('Asia/Shanghai')"
    CONN = None
    CFID = None

    @classmethod
    def _init(cls, conf: dict):
        if cls.CONN is None:
            cls.connect(conf)
        elif cls.CFID != UniUtil.get_uuid(conf):
            cls.connect(conf)

    @classmethod
    def connect(cls, conf: dict):
        try:
            cls.CONN = InfluxDBClient(**conf)
            cls.CFID = UniUtil.get_uuid(conf)
        except Exception as e:
            LogUtil.error("influxdb init failed, please check the config", e)

    @classmethod
    def exec_sql(cls, conf: dict, sql: str):
        """执行influxdb查询sql"""
        cls._init(conf)
        return list(cls.CONN.query(sql).get_points())

    @classmethod
    def write_data(cls, conf: dict, tbl: str, data_list: list):
        """向influxdb写入数据
        :data_list 格式：[(time, tid, v1, v2, ...), ...]
        """
        cls._init(conf)
        json_data_list = []
        for data in data_list:
            fields = {}
            for i, v in enumerate(data[2:], 1):
                fields[f'v{i}'] = round(v, 3)
            json_data = {
                'measurement': tbl,
                'time': str(data[0]).replace(' ', 'T') + '+08:00',
                'tags': {
                    'tid': str(data[1])
                },
                'fields': fields,
            }
            json_data_list.append(json_data)
        cls.CONN.write_points(json_data_list)

    @classmethod
    def write_points(cls, conf: dict, json_data_list: list):
        """向influxdb写入数据
        :json_data_list 格式：[{
            'measurement': 'tbl',
            'time': time.replace(' ', 'T') + '+08:00',
            'tags': {
                'k': 'v'
            },
            'fields': {'k': 'v'},
        }, ...]
        """
        cls._init(conf)
        cls.CONN.write_points(json_data_list)

    @classmethod
    def create_db(cls, conf: dict, db_name: str):
        cls._init(conf)
        cls.CONN.create_database(db_name)


class MysqlUtil:
    """mysql工具类

    conf_mysql = {
        'host': '110.110.110.110',
        'port': 3306,
        'user': 'admin',
        'password': '123456',
        'database': 'db_test',
    }
    """
    CONN = None
    CFID = None

    @classmethod
    def _init(cls, conf: dict):
        if cls.CONN is None:
            cls.connect(conf)
        elif cls.CFID != UniUtil.get_uuid(conf):
            cls.connect(conf)
        else:
            try:
                cls.CONN.ping()
            except Exception as e:
                cls.connect(conf)

    @classmethod
    def connect(cls, conf: dict):
        try:
            cls.CONN = pymysql.connect(**conf)
            cls.CFID = UniUtil.get_uuid(conf)
        except Exception as e:
            LogUtil.error("mysql init failed, please check the config", e)

    @classmethod
    def get(cls, conf: dict, sql: str):
        cls._init(conf)
        res = None
        cursor = cls.CONN.cursor()
        cursor.execute(sql)
        res = cursor.fetchall()
        cursor.close()
        return res

    @classmethod
    def save(cls, conf: dict, sql: str):
        cls._init(conf)
        cursor = cls.CONN.cursor()
        cursor.execute(sql)
        cls.CONN.commit()
        cursor.close()


class ShellUtil:
    """远程连接服务器执行命令工具类型

    conf = {
        'hostname': '110.110.110.110',
        'port': 22,
        'username': 'admin',
        'password': '123456',
        'timeout': 30000,
    }
    """
    CONN = None
    CFID = None

    @classmethod
    def _init(cls, conf: dict):
        if cls.CONN is None:
            cls.connect(conf)
        elif cls.CFID != UniUtil.get_uuid(conf):
            cls.connect(conf)

    @classmethod
    def connect(cls, conf: dict):
        try:
            cls.CONN = paramiko.SSHClient()
            cls.CONN.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            cls.CONN.connect(**conf)
            cls.CFID = UniUtil.get_uuid(conf)
        except Exception as e:
            LogUtil.error("shell init failed, please check the config", e)

    @classmethod
    def exec(cls, conf: dict, cmd: str):
        cls._init(conf)
        stdin, stdout, stderr = cls.CONN.exec_command(cmd, get_pty=True)
        cls.CONN.close()
        res = UniUtil.to_str(stdout.read())
        error = UniUtil.to_str(stderr.read())
        # 如果有错误信息，返回error，否则返回res
        if error.strip():
            return {'sta': 200, 'res': error}
        else:
            return {'sta': 201, 'res': res}


class MinioUtil:
    """minio工具类

    conf = {
        'endpoint': '110.110.110.110:9000',
        'access_key': 'admin',
        'secret_key': '123456',
        'secure': False,
    }
    """
    CONN = None
    CFID = None
    POLICY = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetBucketLocation","s3:ListBucket"],"Resource":["arn:aws:s3:::%s"]},{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::%s/*"]}]}'

    @classmethod
    def _init(cls, conf: dict):
        if cls.CONN is None:
            cls.connect(conf)
        elif cls.CFID != UniUtil.get_uuid(conf):
            cls.connect(conf)

    @classmethod
    def connect(cls, conf: dict):
        try:
            cls.CONN = minio.Minio(**conf)
            cls.CFID = UniUtil.get_uuid(conf)
        except Exception as e:
            LogUtil.error("minio init failed, please check the config", e)

    @classmethod
    def upload(cls, conf: dict, bucket: str, filepath: str, filename: str):
        """上传文件，返回文件的下载地址"""
        cls._init(conf)
        endpoint = conf['endpoint']
        download_url = f'http://{endpoint}'
        cls.CONN.fput_object(bucket_name=bucket, object_name=filename, file_path=filepath)
        return f'{download_url}/{bucket}/{filename}'

    @classmethod
    def exists_bucket(cls, conf: dict, bucket: str):
        """
        判断桶是否存在
        :param bucket_name: 桶名称
        :return:
        """
        cls._init(conf)
        return cls.CONN.bucket_exists(bucket_name=bucket)

    @classmethod
    def create_bucket(cls, conf: dict, bucket: str, is_policy: bool = True):
        """
        创建桶 + 赋予策略
        :param bucket_name: 桶名
        :param is_policy: 策略
        :return:
        """
        cls._init(conf)
        if cls.exists_bucket(bucket=bucket):
            return False
        else:
            cls.CONN.make_bucket(bucket_name=bucket)
        if is_policy:
            policy = cls.POLICY % (bucket, bucket)
            cls.CONN.set_bucket_policy(bucket_name=bucket, policy=policy)
        return True

    @classmethod
    def download(cls, conf: dict, bucket: str, filepath: str, filename: str):
        """下载保存文件保存本地
        :param bucket:
        :param filepath:
        :param filename:
        :return:
        """
        cls._init(conf)
        cls.CONN.fget_object(bucket, filename, filepath)


class RedisUtil:
    """redis操作工具类

    conf = {
        'host': '',
        'port': '',
        'password': '',
        'db': '30000',
        'decode_responses': True,
    }
    """
    CONN = None
    CFID = None

    @classmethod
    def _init(cls, conf: dict):
        if cls.CONN is None:
            cls.connect(conf)
        elif cls.CFID != UniUtil.get_uuid(conf):
            cls.connect(conf)

    @classmethod
    def connect(cls, conf: dict):
        try:
            pool = redis.ConnectionPool(**conf)
            cls.CONN = redis.Redis(connection_pool=pool)
            cls.CFID = UniUtil.get_uuid(conf)
        except Exception as e:
            LogUtil.error("redis init failed, please check the config", e)

    @classmethod
    def exist(cls, key: str):
        """判断key是否存在
        """
        return cls.CONN.exists(key)

    @classmethod
    def get(cls, key: str):
        """字符串获取值
        """
        return cls.CONN.get(key)

    @classmethod
    def set(cls, key: str, val: str):
        """字符串设置值
        """
        cls.CONN.set(key, val)

    @classmethod
    def lget(cls, key: str):
        """列表获取值
        """
        cls.CONN.lrange(key, 0, -1)

    @classmethod
    def lset(cls, key: dict, vals: tuple):
        """列表设置值
        """
        cls.CONN.lpush(key, vals)
