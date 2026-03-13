import logging

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

YANDEX_GPT_URL = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"


class YandexGPTClient:
    def __init__(self):
        self.url = YANDEX_GPT_URL
        self.headers = {
            "Authorization": f"Api-Key {settings.yandex_gpt_api_key}",
            "Content-Type": "application/json",
        }
        self.model_uri = f"gpt://{settings.yandex_gpt_folder_id}/yandexgpt-lite"

    async def chat(self, messages: list, system_prompt: str) -> str:
        gpt_messages = [{"role": "system", "text": system_prompt}]

        for msg in messages:
            role = msg.get("role", "user")
            text = msg.get("text", "")
            if role in ("user", "assistant") and text:
                gpt_messages.append({"role": role, "text": text})

        body = {
            "modelUri": self.model_uri,
            "completionOptions": {
                "stream": False,
                "temperature": 0.3,
                "maxTokens": 2000,
            },
            "messages": gpt_messages,
        }

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(self.url, json=body, headers=self.headers)
                response.raise_for_status()
                data = response.json()

            text = (
                data.get("result", {})
                .get("alternatives", [{}])[0]
                .get("message", {})
                .get("text", "")
            )
            if not text:
                logger.warning("YandexGPT вернул пустой ответ: %s", data)
                return "Не удалось получить ответ от модели."

            logger.info("YandexGPT ответил успешно (%d символов)", len(text))
            return text

        except httpx.HTTPStatusError as e:
            logger.error(
                "HTTP ошибка YandexGPT %s: %s", e.response.status_code, e.response.text
            )
            return f"Ошибка API YandexGPT: {e.response.status_code}. Проверьте API-ключ и folder_id."
        except Exception as e:
            logger.error("Ошибка YandexGPT: %s", e)
            return f"Ошибка при обращении к YandexGPT: {e}"
