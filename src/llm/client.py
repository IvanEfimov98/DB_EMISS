"""Клиент DeepSeek API."""

import os
import requests
import yaml

class DeepSeekClient:
    def __init__(self, config_path="config/settings.yaml"):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        self.api_key = config['deepseek']['api_key']
        if self.api_key.startswith('${') and self.api_key.endswith('}'):
            env_var = self.api_key[2:-1]
            self.api_key = os.environ.get(env_var)
            if not self.api_key:
                raise ValueError(f"Environment variable {env_var} not set")
        self.model = config['deepseek']['model']
        self.temperature = config['deepseek']['temperature']
        self.max_tokens = config['deepseek']['max_tokens']
        self.url = "https://api.deepseek.com/v1/chat/completions"

    def generate(self, messages, **kwargs):
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": kwargs.get('temperature', self.temperature),
            "max_tokens": kwargs.get('max_tokens', self.max_tokens)
        }
        response = requests.post(self.url, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()['choices'][0]['message']['content']