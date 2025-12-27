#python 3.8
import os
import time
import hmac
import hashlib
import base64
import urllib.parse
import requests
from json import load
from dotenv import load_dotenv

load_dotenv()

class MessageRobot:
    def __init__(self):
        self._secret = os.getenv('MESSAGE_ROBOT_SECRET')
        self._access_token = os.getenv('MESSAGE_ROBOT_ACCESS_TOKEN')
        # 去除可能的空白字符
        self._secret = self._secret.strip()
        self._access_token = self._access_token.strip()
        # 验证环境变量是否加载成功
        if not self._secret:
            raise ValueError("MESSAGE_ROBOT_SECRET 环境变量未设置")
        if not self._access_token:
            raise ValueError("MESSAGE_ROBOT_ACCESS_TOKEN 环境变量未设置")
    
    def _generate_timestamp_and_sign(self) -> tuple[str, str]:
        time_stamp = str(round(time.time() * 1000))
        secret_enc = self._secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(time_stamp, self._secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        return time_stamp, sign

    def send_message(self, message):
        # 生成时间戳和签名
        timestamp, sign = self._generate_timestamp_and_sign()

        # 构建 URL，包含 access_token、timestamp 和 sign 参数
        url = "https://oapi.dingtalk.com/robot/send"
        params = {
            "access_token": self._access_token,
            "timestamp": timestamp,
            "sign": sign,
        }
        # 请求头
        headers = {
            "Content-Type": "application/json",
        }

        # 请求体（JSON格式）
        data = {
            "msgtype": "text",
            "text": {
                "content": message
            }
        }
    
        # 发送 POST 请求
        # 方式1：使用 params 参数（推荐，requests 会自动处理 URL 编码）
        response = requests.post(
            url,
            params=params,  # URL 查询参数
            headers=headers,
            json=data,      # JSON 数据（自动序列化并设置 Content-Type）
            timeout=10       # 超时时间
        )

        # 检查响应
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"发送消息失败: {response.errcode} {response.errmsg}")


if __name__ == "__main__":
    message_robot = MessageRobot()
    message = message_robot.send_message("Hello, World!")
    print(message)