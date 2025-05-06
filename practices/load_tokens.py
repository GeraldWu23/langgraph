import os

# 向环境变量里添加第三方 token
try:
    user_home_dir = os.path.expanduser("~")
    with open(os.path.join(user_home_dir, ".ssh/third_party_token_string"), "r") as f:
        environ_key = eval(f.read())
    for k, v , in environ_key.items():
        os.environ.setdefault(k, v)
except Exception as e:
    print(e)
