from google import genai


def test_google_api():
    import os

    os.environ['http_proxy'] = 'http://127.0.0.1:7890'
    os.environ['https_proxy'] = 'http://127.0.0.1:7890'



    # 配置 API 密钥
    client = genai.Client(api_key="AIzaSyAHG8pF0IrDtxbot0eef8zyuLxtacyHsq4")

    # 生成内容
    response = client.models.generate_content(
        model="gemini-2.0-flash",
        contents=["Explain how AI works"]
    )

    # 输出结果
    print(response.text)
    return response.text

def test_proxy():
    import requests

    proxies = {
        "http": "http://127.0.0.1:7890",
        "https": "http://127.0.0.1:7890",
    }

    response = requests.get("https://www.google.com", proxies=proxies)
    print(response.status_code)
