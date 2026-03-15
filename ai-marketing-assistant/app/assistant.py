import json
import logging
import re
from datetime import date, timedelta

from app.direct_client import STRATEGY_NAMES, DirectClient
from app.gpt_client import YandexGPTClient

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "Ты — AI-маркетолог-ассистент для Яндекс Директ. "
    "Ты умеешь анализировать рекламные кампании, давать экспертные советы "
    "по оптимизации, и выполнять команды пользователя через API. "
    "Когда пользователь просит данные — используй предоставленный контекст. "
    "Отвечай на русском языке, кратко и по делу. "
    "При анализе статистики — делай выводы и давай конкретные рекомендации. "
    "Никогда не говори что у тебя нет доступа к Директ — данные уже переданы тебе в контексте."
)

# Стратегии с их ключевыми словами для распознавания
STRATEGY_KEYWORDS = [
    ("WB_MAXIMUM_CLICKS",           ["максимум кликов", "больше кликов", "клики maximize", "клики макс"]),
    ("WB_MAXIMUM_CONVERSION_RATE",  ["максимум конверсий", "больше конверсий", "конверсии maximize", "конверсии макс"]),
    ("AVERAGE_CPA",                 ["средняя цена конверсии", "средний cpa", "средняя cpa", "оптимизация по cpa"]),
    ("PAY_FOR_CONVERSION",          ["оплата за конверсию", "pay for conversion", "платить за конверсию"]),
    ("AVERAGE_CPC",                 ["средняя цена клика", "средний cpc", "средняя cpc", "оптимизация по cpc"]),
    ("AVERAGE_CRR",                 ["средний дрр", "average crr", "рентабельность расходов"]),
    ("MAX_PROFIT",                  ["максимальная прибыль", "макс прибыль", "max profit"]),
]


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
    """Извлечь числовую сумму из текста (рубли, тысячи)."""
    match = re.search(r"(\d[\d\s]*(?:[.,]\d+)?)\s*(?:тыс(?:яч)?\.?|тр\.?)", text.lower())
    if match:
        raw = match.group(1).replace(" ", "").replace(",", ".")
        try:
            return float(raw) * 1000
        except ValueError:
            pass
    match = re.search(r"(\d[\d\s]*(?:[.,]\d+)?)\s*(?:руб|р\b|₽)", text.lower())
    if match:
        raw = match.group(1).replace(" ", "").replace(",", ".")
        try:
            return float(raw)
        except ValueError:
            pass
    # Просто число в тексте
    match = re.search(r"\b(\d{2,6}(?:[.,]\d+)?)\b", text)
    if match:
        raw = match.group(1).replace(",", ".")
        try:
            return float(raw)
        except ValueError:
            pass
    return None


def _extract_goal_id(text: str) -> int | None:
    """Извлечь ID цели из текста вида 'цель 12345' или 'goal_id 12345'."""
    match = re.search(r"(?:цель|goal|goal_id)[:\s#]*(\d+)", text.lower())
    if match:
        return int(match.group(1))
    return None


def _detect_strategy_type(text: str) -> str | None:
    """Определить тип стратегии по ключевым словам в тексте."""
    text_lower = text.lower()
    for strategy_type, keywords in STRATEGY_KEYWORDS:
        if any(kw in text_lower for kw in keywords):
            return strategy_type
    return None


def _find_campaign_by_name(campaigns: list, text: str) -> dict | None:
    text_lower = text.lower()
    for campaign in campaigns:
        name = campaign.get("Name", "")
        if name and name.lower() in text_lower:
            return campaign
    return None


def _find_campaign_in_history(campaigns: list, history: list) -> dict | None:
    for msg in reversed(history):
        text = msg.get("text", "")
        found = _find_campaign_by_name(campaigns, text)
        if found:
            return found
    return None


def _resolve_campaign(campaigns: list, user_message: str, history: list) -> dict | None:
    """Определяет целевую кампанию из сообщения, истории или берёт единственную."""
    valid = [c for c in campaigns if "error" not in c and "Id" in c]
    if not valid:
        return None
    found = _find_campaign_by_name(valid, user_message)
    if found:
        return found
    found = _find_campaign_in_history(valid, history)
    if found:
        return found
    if len(valid) == 1:
        return valid[0]
    return None


def _format_strategy_info(campaign_data: dict) -> str:
    """Форматирует информацию о текущей стратегии кампании."""
    for campaign_type_key in ("UnifiedCampaign", "TextCampaign", "SmartCampaign"):
        campaign_type_data = campaign_data.get(campaign_type_key)
        if campaign_type_data:
            strategy = campaign_type_data.get("BiddingStrategy", {})
            search = strategy.get("Search", {})
            strategy_type = search.get("BiddingStrategyType", "Неизвестно")
            strategy_name = STRATEGY_NAMES.get(strategy_type, strategy_type)

            params = []
            for field, label in [
                ("WbMaximumClicks", "Максимум кликов"),
                ("WbMaximumConversionRate", "Максимум конверсий"),
                ("AverageCpc", "Средняя CPC"),
                ("AverageCpa", "Средняя CPA"),
                ("PayForConversion", "Оплата за конверсию"),
                ("AverageCrr", "Средний ДРР"),
                ("MaxProfit", "Макс прибыль"),
            ]:
                data = search.get(field)
                if data:
                    for k, v in data.items():
                        if k == "WeeklySpendLimit" and v:
                            params.append(f"Недельный бюджет: {v/1_000_000:.0f} руб")
                        elif k == "AverageCpc" and v:
                            params.append(f"Ср. цена клика: {v/1_000_000:.2f} руб")
                        elif k == "AverageCpa" and v:
                            params.append(f"Ср. цена конверсии: {v/1_000_000:.2f} руб")
                        elif k == "Cpa" and v:
                            params.append(f"Цена конверсии: {v/1_000_000:.2f} руб")
                        elif k == "GoalId" and v:
                            params.append(f"ID цели: {v}")
                        elif k == "Crr" and v:
                            params.append(f"ДРР: {v}%")

            details = "; ".join(params) if params else "без доп. параметров"
            return f"Тип: {campaign_type_key}\nСтратегия: {strategy_name}\n{details}"

    return "Данные о стратегии недоступны"


class MarketingAssistant:
    def __init__(self, direct_client: DirectClient, gpt_client: YandexGPTClient):
        self.direct = direct_client
        self.gpt = gpt_client

    async def process_message(self, user_message: str, history: list) -> str:
        text = user_message.lower()
        context_data: str | None = None
        action_result: str | None = None

        try:
            # ── Intent: список кампаний ─────────────────────────────────────────
            action_keywords = ("бюджет", "останови", "запусти", "включи", "выключи",
                               "пауза", "статистика", "стратег", "конверси", "клик", "cpc", "cpa")
            has_action_intent = any(kw in text for kw in action_keywords)

            if not has_action_intent and any(kw in text for kw in (
                "список кампаний", "мои кампании", "покажи кампании",
                "все кампании", "покажи все", "какие кампании", "список моих",
            )):
                campaigns = await self.direct.get_campaigns()
                valid = [c for c in campaigns if "error" not in c]
                if valid:
                    context_data = "Список кампаний из Яндекс Директ:\n" + json.dumps(valid, ensure_ascii=False, indent=2)
                else:
                    context_data = "В аккаунте Яндекс Директ кампаний не найдено."

            # ── Intent: текущая стратегия кампании ─────────────────────────────
            elif any(kw in text for kw in ("стратегия", "текущая стратегия", "какая стратегия", "стратегию посмотри")):
                if not any(kw in text for kw in ("измени стратегию", "поменяй стратегию", "смени стратегию",
                                                   "установи стратегию", "переключи стратегию")):
                    campaigns = await self.direct.get_campaigns()
                    campaign = _resolve_campaign(campaigns, user_message, history)
                    if campaign:
                        full = await self.direct.get_campaign_full(campaign["Id"])
                        info = _format_strategy_info(full)
                        context_data = (
                            f'Кампания: "{campaign["Name"]}" (ID: {campaign["Id"]})\n'
                            f"Текущие настройки стратегии:\n{info}"
                        )
                    else:
                        names = [c.get("Name", "") for c in campaigns if "error" not in c]
                        context_data = f"Укажи кампанию. Доступные: {', '.join(names)}"

            # ── Intent: изменить стратегию ──────────────────────────────────────
            elif any(kw in text for kw in (
                "измени стратегию", "поменяй стратегию", "смени стратегию",
                "установи стратегию", "переключи стратегию", "стратегию на",
            )):
                strategy_type = _detect_strategy_type(user_message)
                campaigns = await self.direct.get_campaigns()
                campaign = _resolve_campaign(campaigns, user_message, history)
                amount = _extract_amount(user_message)
                goal_id = _extract_goal_id(user_message)

                if not strategy_type:
                    strategy_list = "\n".join(
                        f"- **{name}**: «{kws[0]}»"
                        for _, kws in STRATEGY_KEYWORDS
                        for name in [STRATEGY_NAMES.get(_, _)]
                    )
                    context_data = (
                        "Укажи тип стратегии. Доступные:\n" + strategy_list
                    )
                elif not campaign:
                    names = [c.get("Name", "") for c in campaigns if "error" not in c]
                    context_data = f"Укажи кампанию. Доступные: {', '.join(names)}"
                else:
                    cid = campaign["Id"]
                    cpc = amount if strategy_type == "AVERAGE_CPC" else None
                    cpa = amount if strategy_type in ("AVERAGE_CPA", "PAY_FOR_CONVERSION") else None
                    weekly_budget = amount if strategy_type in ("WB_MAXIMUM_CLICKS", "WB_MAXIMUM_CONVERSION_RATE", "MAX_PROFIT") else None

                    success = await self.direct.update_campaign_strategy(
                        campaign_id=cid,
                        strategy_type=strategy_type,
                        weekly_budget=weekly_budget,
                        cpc=cpc,
                        cpa=cpa,
                        goal_id=goal_id,
                    )
                    strategy_name = STRATEGY_NAMES.get(strategy_type, strategy_type)
                    if success:
                        details = []
                        if weekly_budget:
                            details.append(f"недельный бюджет {weekly_budget:.0f} руб")
                        if cpc:
                            details.append(f"ср. CPC {cpc:.2f} руб")
                        if cpa:
                            details.append(f"ср. CPA {cpa:.2f} руб")
                        if goal_id:
                            details.append(f"цель ID={goal_id}")
                        detail_str = "; ".join(details) if details else ""
                        action_result = (
                            f'Стратегия кампании "{campaign["Name"]}" (ID: {cid}) '
                            f'изменена на "{strategy_name}"'
                            + (f" ({detail_str})" if detail_str else "") + "."
                        )
                    else:
                        action_result = (
                            f'Не удалось изменить стратегию кампании "{campaign["Name"]}" '
                            f'на "{strategy_name}". Проверь параметры (может требоваться GoalId для CPA-стратегий).'
                        )

            # ── Intent: изменить CPA (стоимость конверсии) ────────────────────
            elif any(kw in text for kw in (
                "стоимость конверсии", "цена конверсии", " cpa", "целевой cpa",
                "средний cpa", "цпа", "конверсию стоит", "конверсия стоит",
            )):
                amount = _extract_amount(user_message)
                goal_id = _extract_goal_id(user_message)
                campaigns = await self.direct.get_campaigns()
                campaign = _resolve_campaign(campaigns, user_message, history)

                if not campaign:
                    names = [c.get("Name", "") for c in campaigns if "error" not in c]
                    context_data = f"Укажи кампанию. Доступные: {', '.join(names)}"
                elif amount is None:
                    context_data = "Укажи желаемую стоимость конверсии. Пример: «установи CPA 500 руб»"
                else:
                    cid = campaign["Id"]
                    success = await self.direct.update_campaign_cpa(cid, amount, goal_id)
                    if success:
                        action_result = (
                            f'Целевая стоимость конверсии (CPA) кампании "{campaign["Name"]}" '
                            f'(ID: {cid}) установлена: {amount:.2f} руб'
                            + (f", цель ID={goal_id}" if goal_id else "") + "."
                        )
                    else:
                        action_result = (
                            f'Не удалось установить CPA для кампании "{campaign["Name"]}". '
                            "Убедись, что указан ID цели Метрики (например: «цель 12345»)."
                        )

            # ── Intent: изменить CPC (цена клика) ──────────────────────────────
            elif any(kw in text for kw in (
                "цена клика", "цену клика", "ставка клика", "ставку клика",
                " cpc", "средний cpc", "цпц", "клик стоит", "клик по",
            )):
                amount = _extract_amount(user_message)
                weekly = None
                # Проверяем не указан ли недельный бюджет тоже
                budget_match = re.search(r"бюджет\s+(\d[\d\s]*(?:[.,]\d+)?)\s*(?:руб|₽|р\.?)", user_message.lower())
                if budget_match:
                    try:
                        weekly = float(budget_match.group(1).replace(" ", "").replace(",", "."))
                    except ValueError:
                        pass

                campaigns = await self.direct.get_campaigns()
                campaign = _resolve_campaign(campaigns, user_message, history)

                if not campaign:
                    names = [c.get("Name", "") for c in campaigns if "error" not in c]
                    context_data = f"Укажи кампанию. Доступные: {', '.join(names)}"
                elif amount is None:
                    context_data = "Укажи желаемую цену клика. Пример: «установи CPC 30 руб»"
                else:
                    cid = campaign["Id"]
                    success = await self.direct.update_campaign_cpc(cid, amount, weekly)
                    if success:
                        action_result = (
                            f'Средняя цена клика (CPC) кампании "{campaign["Name"]}" '
                            f'(ID: {cid}) установлена: {amount:.2f} руб'
                            + (f", недельный бюджет {weekly:.0f} руб" if weekly else "") + "."
                        )
                    else:
                        action_result = (
                            f'Не удалось установить CPC для кампании "{campaign["Name"]}".'
                        )

            # ── Intent: статистика / отчёт ──────────────────────────────────────
            elif any(kw in text for kw in (
                "статистика", "отчёт", "отчет", "результаты", "показатели",
                "статистику", "клики", "показы", "конверси", "анализ",
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
                        + "\n\nСделай подробный анализ: найди лучшие и худшие показатели, "
                        "сравни CTR, CPC, стоимость конверсий. Дай конкретные рекомендации по оптимизации."
                    )
                else:
                    context_data = "Нет кампаний для получения статистики."

            # ── Intent: остановить кампанию ────────────────────────────────────
            elif any(kw in text for kw in (
                "останови", "пауза", "приостанови", "остановить",
                "поставь на паузу", "выключи", "отключи",
            )):
                campaigns = await self.direct.get_campaigns()
                campaign = _resolve_campaign(campaigns, user_message, history)
                if campaign:
                    cid = campaign["Id"]
                    success = await self.direct.pause_campaign(cid)
                    action_result = (
                        f'Кампания "{campaign["Name"]}" (ID: {cid}) успешно приостановлена.'
                        if success else
                        f'Не удалось приостановить кампанию "{campaign["Name"]}". '
                        "Возможно, она уже на паузе или нет прав."
                    )
                else:
                    names = [c.get("Name", "") for c in campaigns if "error" not in c]
                    context_data = f"Укажи название кампании. Доступные: {', '.join(names)}"

            # ── Intent: запустить кампанию ─────────────────────────────────────
            elif any(kw in text for kw in (
                "запусти", "включи", "возобнови", "запустить", "включить", "активируй",
            )):
                campaigns = await self.direct.get_campaigns()
                campaign = _resolve_campaign(campaigns, user_message, history)
                if campaign:
                    cid = campaign["Id"]
                    success = await self.direct.enable_campaign(cid)
                    action_result = (
                        f'Кампания "{campaign["Name"]}" (ID: {cid}) успешно запущена.'
                        if success else
                        f'Не удалось запустить кампанию "{campaign["Name"]}". '
                        "Возможно, нет прав или кампания уже активна."
                    )
                else:
                    names = [c.get("Name", "") for c in campaigns if "error" not in c]
                    context_data = f"Укажи название кампании. Доступные: {', '.join(names)}"

            # ── Intent: изменить бюджет ────────────────────────────────────────
            elif any(kw in text for kw in ("бюджет", "бюджета", "бюджете")):
                amount = _extract_amount(user_message)
                campaigns = await self.direct.get_campaigns()
                campaign = _resolve_campaign(campaigns, user_message, history)

                if campaign and amount is not None:
                    cid = campaign["Id"]
                    success = await self.direct.update_campaign_budget(cid, amount)
                    action_result = (
                        f'Бюджет кампании "{campaign["Name"]}" (ID: {cid}) обновлён до {amount:.2f} руб.'
                        if success else
                        f'Не удалось обновить бюджет кампании "{campaign["Name"]}".'
                    )
                elif not campaign:
                    names = [c.get("Name", "") for c in campaigns if "error" not in c]
                    context_data = f"Укажи название кампании. Доступные: {', '.join(names)}"
                else:
                    context_data = (
                        "Не удалось распознать сумму. Пример: «установи бюджет 500 руб»"
                    )

        except Exception as e:
            logger.error("Ошибка при обработке intent: %s", e)
            context_data = f"Произошла ошибка при обращении к Яндекс Директ: {e}"

        # ── Формируем промпт для GPT ─────────────────────────────────────────
        if action_result:
            is_success = not any(w in action_result for w in ("Не удалось", "не удалось", "ошибка"))
            if is_success:
                gpt_user_message = (
                    f"Действие УСПЕШНО выполнено в Яндекс Директ через API: {action_result}\n"
                    "Сообщи пользователю об успехе одним предложением."
                )
            else:
                gpt_user_message = (
                    f"Действие НЕ выполнено. Ошибка: {action_result}\n"
                    "Объясни пользователю что именно не получилось и что можно сделать."
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
