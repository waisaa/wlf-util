import logging
from logging import handlers
import colorlog
import os
from inspect import currentframe
import shutil


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
    def init(cls, logname, console=False):
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
    def debug(cls, title=None, *msg):
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
    def info(cls, title=None, *msg):
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
    def warn(cls, title=None, *msg):
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
    def error(cls, title=None, *msg):
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
    def critical(cls, title=None, *msg):
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


class FileUtil:
    """目录、文件操作工具类"""

    @staticmethod
    def create_dir_if_not_exist(dst_dir):
        """创建目录，不存在则创建，存在无操作
        :param dst_dir 要创建的目录
        """
        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir)

    @staticmethod
    def del_dir_or_file(dst_fd):
        """删除文件或目录
        :param src_fd 要删除的目录或文件
        """
        if os.path.isdir(dst_fd):
            shutil.rmtree(dst_fd)
        elif os.path.isfile(dst_fd):
            os.remove(dst_fd)

    @staticmethod
    def get_file_size(filepath):
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
    def clear_dir(filepath):
        """清空文件夹下的所有文件，先删除文件夹再创建"""
        if not os.path.exists(filepath):
            os.mkdir(filepath)
        else:
            shutil.rmtree(filepath)
            os.mkdir(filepath)
