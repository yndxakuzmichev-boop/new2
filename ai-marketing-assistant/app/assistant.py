import json
import logging
import re
from datetime import date, timedelta

from app.direct_client import DirectClient
from app.gpt_client import YandexGPTClient

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "Ты — AI-маркетолог-ассистент для Яндекс Директ. "
    "Ты умеешь анализировать рекламные кампании, давать советы "
    "по оптимизации и выполнять команды пользователя. "
    "Когда пользователь просит данные — используй инструменты. "
    "Отвечай на русском языке, кратко и по делу."
)


def _parse_period(text: str) -> tuple[str, str]:
    """Парсит период из текста пользователя, возвращает (date_from, date_to) в формате YYYY-MM-DD."""
    today = date.today()
    text_lower = text.lower()

    if "сегодня" in text_lower:
        return today.isoformat(), today.isoformat()
    if "вчера" in text_lower:
        yesterday = today - timedelta(days=1)
        return yesterday.isoformat(), yesterday.isoformat()
    if "неделя" in text_lower or "неделю" in text_lower or "7 дней" in text_lower:
        return (today - timedelta(days=7)).isoformat(), today.isoformat()
    if "месяц" in text_lower or "30 дней" in text_lower:
        return (today - timedelta(days=30)).isoformat(), today.isoformat()

    # По умолчанию — последние 7 дней
    return (today - timedelta(days=7)).isoformat(), today.isoformat()


def _extract_amount(text: str) -> float | None:
    """Извлекает числовую сумму из текста."""
    match = re.search(r"(\d[\d\s]*(?:[.,]\d+)?)\s*(?:руб|р\.?|₽)?", text)
    if match:
        raw = match.group(1).replace(" ", "").replace(",", ".")
        try:
            return float(raw)
        except ValueError:
            return None
    return None


def _find_campaign_by_name(campaigns: list, text: str) -> dict | None:
    """Ищет кампанию по вхождению её имени в текст (регистронезависимо)."""
    text_lower = text.lower()
    for campaign in campaigns:
        name = campaign.get("Name", "")
        if name and name.lower() in text_lower:
            return campaign
    return None


class MarketingAssistant:
    def __init__(self, direct_client: DirectClient, gpt_client: YandexGPTClient):
        self.direct = direct_client
        self.gpt = gpt_client

    async def process_message(self, user_message: str, history: list) -> str:
        text = user_message.lower()
        context_data: str | None = None
        action_result: str | None = None

        try:
            # --- Intent: список кампаний ---
            if any(
                kw in text
                for kw in ("список кампаний", "мои кампании", "покажи кампании", "все кампании")
            ):
                campaigns = await self.direct.get_campaigns()
                if isinstance(campaigns, list) and not any("error" in c for c in campaigns):
                    if campaigns:
                        context_data = "Список кампаний:\n" + json.dumps(
                            campaigns, ensure_ascii=False, indent=2
                        )
                    else:
                        context_data = "В аккаунте Яндекс Директ кампаний не найдено. Возможно, аккаунт пуст или логин Client-Login указан неверно."
                else:
                    context_data = f"Ошибка получения кампаний: {campaigns}"

            # --- Intent: статистика ---
            elif any(kw in text for kw in ("статистика", "отчёт", "отчет", "результаты", "показатели")):
                date_from, date_to = _parse_period(user_message)
                campaigns = await self.direct.get_campaigns()
                if campaigns and "error" not in campaigns[0]:
                    campaign_ids = [c["Id"] for c in campaigns if "Id" in c]
                    stats = await self.direct.get_campaign_stats(campaign_ids, date_from, date_to)
                    context_data = (
                        f"Статистика кампаний за период {date_from} — {date_to}:\n"
                        + json.dumps(stats, ensure_ascii=False, indent=2)
                    )
                else:
                    context_data = f"Ошибка получения кампаний: {campaigns}"

            # --- Intent: остановить/пауза кампании ---
            elif any(kw in text for kw in ("останови", "пауза", "приостанови", "остановить", "поставь на паузу")):
                campaigns = await self.direct.get_campaigns()
                campaign = _find_campaign_by_name(campaigns, user_message)
                if campaign:
                    cid = campaign["Id"]
                    success = await self.direct.pause_campaign(cid)
                    if success:
                        action_result = (
                            f'Кампания "{campaign["Name"]}" (ID: {cid}) успешно приостановлена.'
                        )
                    else:
                        action_result = (
                            f'Не удалось приостановить кампанию "{campaign["Name"]}". '
                            "Возможно, она уже на паузе или нет прав."
                        )
                else:
                    campaign_names = [c.get("Name", "") for c in campaigns if "error" not in c]
                    context_data = (
                        "Не удалось найти кампанию по названию. "
                        f"Доступные кампании: {', '.join(campaign_names)}"
                    )

            # --- Intent: запустить/включить кампанию ---
            elif any(kw in text for kw in ("запусти", "включи", "возобнови", "запустить", "включить")):
                campaigns = await self.direct.get_campaigns()
                campaign = _find_campaign_by_name(campaigns, user_message)
                if campaign:
                    cid = campaign["Id"]
                    success = await self.direct.enable_campaign(cid)
                    if success:
                        action_result = (
                            f'Кампания "{campaign["Name"]}" (ID: {cid}) успешно запущена.'
                        )
                    else:
                        action_result = (
                            f'Не удалось запустить кампанию "{campaign["Name"]}". '
                            "Возможно, нет прав или кампания уже активна."
                        )
                else:
                    campaign_names = [c.get("Name", "") for c in campaigns if "error" not in c]
                    context_data = (
                        "Не удалось найти кампанию по названию. "
                        f"Доступные кампании: {', '.join(campaign_names)}"
                    )

            # --- Intent: изменить бюджет ---
            elif "бюджет" in text:
                amount = _extract_amount(user_message)
                campaigns = await self.direct.get_campaigns()
                campaign = _find_campaign_by_name(campaigns, user_message)

                if campaign and amount is not None:
                    cid = campaign["Id"]
                    success = await self.direct.update_campaign_budget(cid, amount)
                    if success:
                        action_result = (
                            f'Дневной бюджет кампании "{campaign["Name"]}" '
                            f'(ID: {cid}) обновлён до {amount:.2f} руб.'
                        )
                    else:
                        action_result = (
                            f'Не удалось обновить бюджет кампании "{campaign["Name"]}".'
                        )
                elif not campaign:
                    campaign_names = [c.get("Name", "") for c in campaigns if "error" not in c]
                    context_data = (
                        "Не удалось найти кампанию по названию. "
                        f"Доступные кампании: {', '.join(campaign_names)}"
                    )
                else:
                    context_data = (
                        "Не удалось распознать сумму бюджета. "
                        "Укажите сумму в рублях, например: «установи бюджет 1000 руб для кампании X»"
                    )

        except Exception as e:
            logger.error("Ошибка при обработке intent: %s", e)
            context_data = f"Произошла ошибка при обращении к Яндекс Директ: {e}"

        # Формируем промпт для GPT
        if action_result:
            # Для успешных действий можно сразу вернуть подтверждение через GPT
            gpt_user_message = (
                f"Выполнено действие: {action_result}\n\n"
                f"Исходный запрос пользователя: {user_message}\n\n"
                "Подтверди выполнение и дай краткий комментарий."
            )
        elif context_data:
            gpt_user_message = (
                f"Данные из Яндекс Директ:\n{context_data}\n\nВопрос: {user_message}"
            )
        else:
            gpt_user_message = user_message

        response = await self.gpt.chat(
            messages=history + [{"role": "user", "text": gpt_user_message}],
            system_prompt=SYSTEM_PROMPT,
        )

        return response
