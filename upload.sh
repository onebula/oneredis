# 增加setup.py里的版本号
python setup.py sdist bdist_wheel
# 跳过历史版本上传最新版文件
twine upload --skip-existing dist/*