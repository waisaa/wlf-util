rm -rf build dist *.egg-info
echo "修改项目版本号"
python3 changeversion.py
echo "打包项目..."
python3 setup.py sdist bdist_wheel
echo "开始上传..."
twine upload dist/*
echo "上传成功！"