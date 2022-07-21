from setuptools import setup, find_packages

# 版本信息
VNU = 7
VERSION = f'0.0.{VNU}'
README = ''
with open("README.md", "r", encoding="utf-8") as fh:
    README = fh.read()

setup(name="wlfutil",
      version=VERSION,
      keywords=("UniUtil", "ConfUtil", "FileUtil", "DtUtil", "LogUtil", "InfluxUtil", "MysqlUtil", "ShellUtil", "MinioUtil", "RedisUtil"),
      description="Common utils for python.",
      long_description=README,
      long_description_content_type="text/markdown",
      license="MIT Licence",
      url="https://github.com/waisaa/wlf-util",
      author="waisaa",
      author_email="waisaa@qq.com",
      packages=find_packages(),
      include_package_data=True,
      platforms="any",
      python_requires='>=3.7',
      install_requires=['colorlog==6.6.0', 'influxdb==5.3.1', 'PyMySQL==1.0.2', 'paramiko==2.11.0', 'minio==7.1.9', 'redis==3.2.0'])

# 每次更新记得修改版本号
# python3 setup.py sdist bdist_wheel
# python3 setup.py sdist
# twine upload dist/*