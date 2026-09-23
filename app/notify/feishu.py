import json
import requests
from app.notify.base import BaseNotifier

class FeishuNotifier(BaseNotifier):
    def __init__(self, webhook_url: str, keyword: str = "股票监控"):
        self.webhook_url = webhook_url
        self.keyword = keyword

    def send(self, title: str, content: str) -> bool:
        # 飞书要求消息中必须包含安全关键词
        text = f"{self.keyword}\n{title}\n{content}"
        payload = {
            "msg_type": "text",
            "content": {"text": text}
        }
        try:
            resp = requests.post(
                self.webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            return resp.status_code == 200
        except Exception as e:
            print(f"[飞书推送失败] {e}")
            return False
