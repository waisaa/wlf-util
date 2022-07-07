from setuptools import setup, find_packages

# 版本信息
VNU = 4
VERSION = f'1.0.{VNU}'
README = ''
with open("README.md", "r", encoding="utf-8") as fh:
    README = fh.read()

setup(name="wlfutil",
      version=VERSION,
      keywords=("FileUtil", "LogUtil"),
      description="Common utils for python.",
      long_description=README,
      long_description_content_type="text/markdown",
      license="MIT Licence",
      url="https://github.com/waisaa/wlfutil",
      author="waisaa",
      author_email="waisaa@qq.com",
      packages=find_packages(),
      include_package_data=True,
      platforms="any",
      python_requires='>=3.7',
      install_requires=['colorlog==6.6.0'])

# 每次更新记得修改版本号
# python3 setup.py sdist bdist_wheel
# python3 setup.py sdist
# twine upload dist/*