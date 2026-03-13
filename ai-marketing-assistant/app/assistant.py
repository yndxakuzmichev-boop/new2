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
    "Отвечай на русском языке, кратко и по делу. "
    "Никогда не говори что у тебя нет доступа к Директ — данные уже переданы тебе в контексте."
)


def _parse_period(text: str) -> tuple[str, str]:
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
    return (today - timedelta(days=7)).isoformat(), today.isoformat()


def _extract_amount(text: str) -> float | None:
    match = re.search(r"(\d[\d\s]*(?:[.,]\d+)?)\s*(?:руб|р\.?|₽|тыс)?", text)
    if match:
        raw = match.group(1).replace(" ", "").replace(",", ".")
        try:
            return float(raw)
        except ValueError:
            return None
    return None


def _find_campaign_by_name(campaigns: list, text: str) -> dict | None:
    text_lower = text.lower()
    for campaign in campaigns:
        name = campaign.get("Name", "")
        if name and name.lower() in text_lower:
            return campaign
    return None


def _find_campaign_in_history(campaigns: list, history: list) -> dict | None:
    """Ищет упоминание кампании в истории диалога."""
    for msg in reversed(history):
        text = msg.get("text", "")
        found = _find_campaign_by_name(campaigns, text)
        if found:
            return found
    return None


def _resolve_campaign(campaigns: list, user_message: str, history: list) -> dict | None:
    """
    Определяет кампанию:
    1. По имени в текущем сообщении
    2. По имени в истории диалога
    3. Если кампания одна — берём её автоматически
    """
    valid = [c for c in campaigns if "error" not in c and "Id" in c]
    if not valid:
        return None

    # 1. По имени в текущем сообщении
    found = _find_campaign_by_name(valid, user_message)
    if found:
        return found

    # 2. По имени в истории
    found = _find_campaign_in_history(valid, history)
    if found:
        return found

    # 3. Если только одна кампания — выбираем её
    if len(valid) == 1:
        return valid[0]

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
            if any(kw in text for kw in (
                "список кампаний", "мои кампании", "покажи кампании",
                "все кампании", "кампании", "покажи все", "какие кампании",
            )):
                campaigns = await self.direct.get_campaigns()
                valid = [c for c in campaigns if "error" not in c]
                if valid:
                    context_data = "Список кампаний из Яндекс Директ:\n" + json.dumps(
                        valid, ensure_ascii=False, indent=2
                    )
                else:
                    context_data = "В аккаунте Яндекс Директ кампаний не найдено (пустой список)."

            # --- Intent: статистика ---
            elif any(kw in text for kw in (
                "статистика", "отчёт", "отчет", "результаты", "показатели",
                "статистику", "клики", "показы", "конверси",
            )):
                date_from, date_to = _parse_period(user_message)
                campaigns = await self.direct.get_campaigns()
                valid = [c for c in campaigns if "error" not in c and "Id" in c]
                if valid:
                    campaign_ids = [c["Id"] for c in valid]
                    stats = await self.direct.get_campaign_stats(campaign_ids, date_from, date_to)
                    context_data = (
                        f"Статистика кампаний за период {date_from} — {date_to}:\n"
                        + json.dumps(stats, ensure_ascii=False, indent=2)
                    )
                else:
                    context_data = "Нет кампаний для получения статистики."

            # --- Intent: остановить/пауза кампании ---
            elif any(kw in text for kw in (
                "останови", "пауза", "приостанови", "остановить",
                "поставь на паузу", "выключи", "отключи",
            )):
                campaigns = await self.direct.get_campaigns()
                campaign = _resolve_campaign(campaigns, user_message, history)
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
                    names = [c.get("Name", "") for c in campaigns if "error" not in c]
                    context_data = (
                        f"Укажи название кампании для остановки. "
                        f"Доступные кампании: {', '.join(names)}"
                    )

            # --- Intent: запустить/включить кампанию ---
            elif any(kw in text for kw in (
                "запусти", "включи", "возобнови", "запустить",
                "включить", "активируй", "запусти",
            )):
                campaigns = await self.direct.get_campaigns()
                campaign = _resolve_campaign(campaigns, user_message, history)
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
                    names = [c.get("Name", "") for c in campaigns if "error" not in c]
                    context_data = (
                        f"Укажи название кампании для запуска. "
                        f"Доступные кампании: {', '.join(names)}"
                    )

            # --- Intent: изменить бюджет ---
            elif any(kw in text for kw in ("бюджет", "бюджета", "бюджете")):
                amount = _extract_amount(user_message)
                campaigns = await self.direct.get_campaigns()
                campaign = _resolve_campaign(campaigns, user_message, history)

                # Если указан недельный бюджет — пересчитываем в дневной
                if amount and ("неделю" in text or "недельный" in text or "в неделю" in text):
                    daily_amount = round(amount / 7, 2)
                    logger.info("Недельный бюджет %.2f → дневной %.2f", amount, daily_amount)
                else:
                    daily_amount = amount

                if campaign and daily_amount is not None:
                    cid = campaign["Id"]
                    success = await self.direct.update_campaign_budget(cid, daily_amount)
                    if success:
                        weekly_note = f" (≈{amount:.0f} руб/нед)" if daily_amount != amount else ""
                        action_result = (
                            f'Дневной бюджет кампании "{campaign["Name"]}" '
                            f'(ID: {cid}) обновлён до {daily_amount:.2f} руб/день{weekly_note}.'
                        )
                    else:
                        action_result = (
                            f'Не удалось обновить бюджет кампании "{campaign["Name"]}".'
                        )
                elif not campaign:
                    names = [c.get("Name", "") for c in campaigns if "error" not in c]
                    context_data = (
                        f"Укажи название кампании для изменения бюджета. "
                        f"Доступные кампании: {', '.join(names)}"
                    )
                else:
                    context_data = (
                        "Не удалось распознать сумму бюджета. "
                        "Пример: «установи дневной бюджет 500 руб» или «недельный бюджет 3500 руб»"
                    )

        except Exception as e:
            logger.error("Ошибка при обработке intent: %s", e)
            context_data = f"Произошла ошибка при обращении к Яндекс Директ: {e}"

        # Формируем финальный промпт для GPT
        if action_result:
            gpt_user_message = (
                f"Действие выполнено успешно: {action_result}\n\n"
                f"Запрос пользователя: {user_message}\n\n"
                "Подтверди выполнение одним-двумя предложениями."
            )
        elif context_data:
            gpt_user_message = (
                f"Данные из Яндекс Директ:\n{context_data}\n\nВопрос пользователя: {user_message}"
            )
        else:
            gpt_user_message = user_message

        response = await self.gpt.chat(
            messages=history + [{"role": "user", "text": gpt_user_message}],
            system_prompt=SYSTEM_PROMPT,
        )

        return response
