import logging
from io import StringIO

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

SANDBOX_BASE_URL = "https://api-sandbox.direct.yandex.com/json/v5/"
PROD_BASE_URL = "https://api.direct.yandex.com/json/v5/"

# Маппинг типов стратегий для отображения пользователю
STRATEGY_NAMES = {
    "WB_MAXIMUM_CLICKS": "Максимум кликов",
    "WB_MAXIMUM_CONVERSION_RATE": "Максимум конверсий",
    "AVERAGE_CPC": "Средняя цена клика (CPC)",
    "AVERAGE_CPA": "Средняя цена конверсии (CPA)",
    "PAY_FOR_CONVERSION": "Оплата за конверсию",
    "AVERAGE_CRR": "Средний ДРР",
    "MAX_PROFIT": "Максимум прибыли",
    "HIGHEST_POSITION": "Наивысшая позиция (ручная)",
    "SERVING_OFF": "Показы отключены",
}


class DirectClient:
    def __init__(self):
        self.base_url = SANDBOX_BASE_URL if settings.direct_sandbox else PROD_BASE_URL
        self.headers = {
            "Authorization": f"Bearer {settings.direct_token}",
            "Accept-Language": "ru",
            "Content-Type": "application/json",
        }
        if settings.direct_client_login:
            self.headers["Client-Login"] = settings.direct_client_login

    async def _post(self, endpoint: str, body: dict) -> dict:
        url = self.base_url + endpoint
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, json=body, headers=self.headers)
            response.raise_for_status()
            return response.json()

    async def _post_v501(self, endpoint: str, body: dict) -> dict:
        """POST к v501 endpoint — для Единой перформанс-кампании (ЕПК).
        Документация: https://yandex.ru/dev/direct/doc/ru/campaigns/get-unified-campaign
        """
        base = self.base_url.replace("/json/v5/", "/v501/")
        url = base + endpoint
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, json=body, headers=self.headers)
            response.raise_for_status()
            return response.json()

    def _check_update_result(self, data: dict, campaign_id: int) -> tuple[bool, list]:
        """Проверяет результат операции update. Возвращает (success, errors)."""
        update_results = data.get("result", {}).get("UpdateResults", [])
        if not update_results:
            return False, [{"Message": "UpdateResults пустой"}]
        errors = update_results[0].get("Errors") or []
        return len(errors) == 0, errors

    def _build_strategy_body(
        self,
        strategy_type: str,
        weekly_budget: int = None,
        cpc: int = None,
        cpa: int = None,
        goal_id: int = None,
        crr: int = None,
    ) -> dict:
        """Строит тело стратегии показа для campaigns.update.
        Все суммы передавать в микрорублях (умножить на 1_000_000).
        """
        search = {"BiddingStrategyType": strategy_type}
        network = {"BiddingStrategyType": "SERVING_OFF"}

        if strategy_type == "WB_MAXIMUM_CLICKS":
            params = {}
            if weekly_budget:
                params["WeeklySpendLimit"] = weekly_budget
            search["WbMaximumClicks"] = params

        elif strategy_type == "WB_MAXIMUM_CONVERSION_RATE":
            params = {}
            if weekly_budget:
                params["WeeklySpendLimit"] = weekly_budget
            if goal_id:
                params["GoalId"] = goal_id
            search["WbMaximumConversionRate"] = params

        elif strategy_type == "AVERAGE_CPC":
            params = {"AverageCpc": cpc or 1_000_000}
            if weekly_budget:
                params["WeeklySpendLimit"] = weekly_budget
            search["AverageCpc"] = params

        elif strategy_type == "AVERAGE_CPA":
            params = {
                "AverageCpa": cpa or 1_000_000,
                "GoalId": goal_id or 0,
            }
            if weekly_budget:
                params["WeeklySpendLimit"] = weekly_budget
            search["AverageCpa"] = params

        elif strategy_type == "PAY_FOR_CONVERSION":
            params = {
                "Cpa": cpa or 1_000_000,
                "GoalId": goal_id or 0,
            }
            if weekly_budget:
                params["WeeklySpendLimit"] = weekly_budget
            search["PayForConversion"] = params

        elif strategy_type == "AVERAGE_CRR":
            params = {
                "Crr": crr or 10,
                "GoalId": goal_id or 0,
            }
            if weekly_budget:
                params["WeeklySpendLimit"] = weekly_budget
            search["AverageCrr"] = params

        elif strategy_type == "MAX_PROFIT":
            params = {}
            if weekly_budget:
                params["WeeklySpendLimit"] = weekly_budget
            search["MaxProfit"] = params

        return {"Search": search, "Network": network}

    async def _update_strategy_with_fallback(
        self, campaign_id: int, strategy_body: dict, label: str
    ) -> bool:
        """Пробует обновить стратегию через UnifiedCampaign/v501, затем TextCampaign/v5."""
        attempts = [
            ("UnifiedCampaign", self._post_v501),
            ("TextCampaign", self._post_v501),
            ("TextCampaign", self._post),
        ]
        for campaign_type, post_fn in attempts:
            body = {
                "method": "update",
                "params": {
                    "Campaigns": [{
                        "Id": campaign_id,
                        campaign_type: {"BiddingStrategy": strategy_body},
                    }]
                },
            }
            try:
                data = await post_fn("campaigns", body)
                ok, errors = self._check_update_result(data, campaign_id)
                if ok:
                    logger.info("[%s] %s обновлена через %s", label, campaign_id, campaign_type)
                    return True
                codes = [int(e.get("Code", 0)) for e in errors]
                logger.warning("[%s] %s ошибки %s через %s: %s", label, campaign_id, codes, campaign_type, errors)
                # Если тип кампании не совпадает — пробуем следующий вариант
                if any(c in codes for c in (4002, 4003, 4004, 9000, 9001)):
                    continue
                # Для других ошибок — не пробуем дальше
                return False
            except Exception as e:
                logger.warning("[%s] Ошибка через %s: %s", label, campaign_type, e)
        return False

    # ──────────────────────────────────────────────
    # ПОЛУЧЕНИЕ ДАННЫХ
    # ──────────────────────────────────────────────

    async def get_campaigns(self) -> list:
        body = {
            "method": "get",
            "params": {
                "SelectionCriteria": {
                    "Statuses": ["DRAFT", "ACCEPTED", "MODERATION", "REJECTED"],
                },
                "FieldNames": ["Id", "Name", "Status", "State", "Statistics", "DailyBudget", "Type"],
            },
        }
        try:
            data = await self._post("campaigns", body)
            campaigns = data.get("result", {}).get("Campaigns", [])
            logger.info("Получено кампаний: %d", len(campaigns))
            return campaigns
        except httpx.HTTPStatusError as e:
            logger.error("Ошибка HTTP при получении кампаний: %s", e)
            return [{"error": f"HTTP ошибка: {e.response.status_code}"}]
        except Exception as e:
            logger.error("Ошибка при получении кампаний: %s", e)
            return [{"error": str(e)}]

    async def get_campaign_full(self, campaign_id: int) -> dict:
        """Получить полные данные кампании включая стратегию."""
        body = {
            "method": "get",
            "params": {
                "SelectionCriteria": {"Ids": [campaign_id]},
                "FieldNames": ["Id", "Name", "Status", "State", "Type", "DailyBudget"],
                "TextCampaignFieldNames": ["BiddingStrategy"],
                "UnifiedCampaignFieldNames": ["BiddingStrategy"],
                "SmartCampaignFieldNames": ["BiddingStrategy"],
            },
        }
        try:
            # Пробуем v501 (поддерживает ЕПК)
            data = await self._post_v501("campaigns", body)
            campaigns = data.get("result", {}).get("Campaigns", [])
            if campaigns:
                return campaigns[0]
        except Exception:
            pass
        try:
            data = await self._post("campaigns", body)
            campaigns = data.get("result", {}).get("Campaigns", [])
            return campaigns[0] if campaigns else {}
        except Exception as e:
            logger.error("Ошибка get_campaign_full %d: %s", campaign_id, e)
            return {}

    async def get_campaign_stats(self, campaign_ids: list, date_from: str, date_to: str) -> list:
        reports_url = self.base_url.replace("/json/v5/", "") + "/json/v5/reports"
        body = {
            "params": {
                "SelectionCriteria": {
                    "Filter": [{"Field": "CampaignId", "Operator": "IN", "Values": [str(cid) for cid in campaign_ids]}],
                    "DateFrom": date_from,
                    "DateTo": date_to,
                },
                "FieldNames": [
                    "CampaignName", "Impressions", "Clicks", "Ctr",
                    "AvgCpc", "Cost", "Conversions", "CostPerConversion",
                ],
                "ReportName": f"stats_{date_from}_{date_to}",
                "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
                "DateRangeType": "CUSTOM_DATE",
                "Format": "TSV",
                "IncludeVAT": "YES",
                "IncludeDiscount": "NO",
            }
        }
        headers = dict(self.headers)
        headers["skipReportHeader"] = "true"
        headers["skipReportSummary"] = "true"
        headers["returnMoneyInMicros"] = "false"
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(reports_url, json=body, headers=headers)
                response.raise_for_status()
                tsv_text = response.text
            rows = []
            lines = StringIO(tsv_text).readlines()
            if not lines:
                return []
            field_names = lines[0].rstrip("\n").split("\t")
            for line in lines[1:]:
                line = line.rstrip("\n")
                if not line or line.startswith("Total") or line.startswith("Итого"):
                    continue
                rows.append(dict(zip(field_names, line.split("\t"))))
            logger.info("Получено строк статистики: %d", len(rows))
            return rows
        except httpx.HTTPStatusError as e:
            logger.error("Ошибка HTTP при получении статистики: %s", e)
            return [{"error": f"HTTP ошибка: {e.response.status_code}"}]
        except Exception as e:
            logger.error("Ошибка при получении статистики: %s", e)
            return [{"error": str(e)}]

    # ──────────────────────────────────────────────
    # УПРАВЛЕНИЕ КАМПАНИЯМИ
    # ──────────────────────────────────────────────

    async def pause_campaign(self, campaign_id: int) -> bool:
        body = {"method": "suspend", "params": {"SelectionCriteria": {"Ids": [campaign_id]}}}
        try:
            data = await self._post("campaigns", body)
            suspended = data.get("result", {}).get("SuspendResults", [])
            if suspended and not suspended[0].get("Errors"):
                logger.info("Кампания %d приостановлена", campaign_id)
                return True
            logger.warning("Не удалось приостановить %d: %s", campaign_id, suspended)
            return False
        except Exception as e:
            logger.error("Ошибка паузы кампании %d: %s", campaign_id, e)
            return False

    async def enable_campaign(self, campaign_id: int) -> bool:
        body = {"method": "resume", "params": {"SelectionCriteria": {"Ids": [campaign_id]}}}
        try:
            data = await self._post("campaigns", body)
            resumed = data.get("result", {}).get("ResumeResults", [])
            if resumed and not resumed[0].get("Errors"):
                logger.info("Кампания %d запущена", campaign_id)
                return True
            logger.warning("Не удалось запустить %d: %s", campaign_id, resumed)
            return False
        except Exception as e:
            logger.error("Ошибка запуска кампании %d: %s", campaign_id, e)
            return False

    async def update_campaign_budget(self, campaign_id: int, amount: float) -> bool:
        """Обновить бюджет кампании. Автоматически выбирает DailyBudget или WeeklySpendLimit."""
        amount_micros = int(amount * 1_000_000)

        # Шаг 1: DailyBudget для ручных стратегий
        body = {
            "method": "update",
            "params": {"Campaigns": [{"Id": campaign_id, "DailyBudget": {"Amount": amount_micros, "Mode": "STANDARD"}}]},
        }
        try:
            data = await self._post("campaigns", body)
            logger.info("Ответ DailyBudget для кампании %d: %s", campaign_id, data)
            ok, errors = self._check_update_result(data, campaign_id)
            if ok:
                logger.info("Бюджет %d обновлён через DailyBudget до %.2f руб.", campaign_id, amount)
                return True
            codes = [int(e.get("Code", 0)) for e in errors]
            logger.info("Коды ошибок для кампании %d: %s", campaign_id, codes)
            if 6000 in codes or 8000 in codes:
                # Автостратегия — используем WeeklySpendLimit
                logger.info("Переключаемся на WeeklySpendLimit для кампании %d", campaign_id)
                strategy_body = self._build_strategy_body("WB_MAXIMUM_CLICKS", weekly_budget=amount_micros)
                return await self._update_strategy_with_fallback(campaign_id, strategy_body, "budget")
            logger.error("Ошибки DailyBudget для %d: %s", campaign_id, errors)
            return False
        except Exception as e:
            logger.error("Ошибка обновления бюджета %d: %s", campaign_id, e)
            return False

    async def update_campaign_strategy(
        self,
        campaign_id: int,
        strategy_type: str,
        weekly_budget: float = None,
        cpc: float = None,
        cpa: float = None,
        goal_id: int = None,
        crr: int = None,
    ) -> bool:
        """Изменить стратегию показа кампании.
        
        strategy_type: WB_MAXIMUM_CLICKS | WB_MAXIMUM_CONVERSION_RATE | AVERAGE_CPC |
                       AVERAGE_CPA | PAY_FOR_CONVERSION | AVERAGE_CRR | MAX_PROFIT
        Суммы в рублях (конвертируются в микрорубли внутри).
        """
        weekly_micros = int(weekly_budget * 1_000_000) if weekly_budget else None
        cpc_micros = int(cpc * 1_000_000) if cpc else None
        cpa_micros = int(cpa * 1_000_000) if cpa else None

        strategy_body = self._build_strategy_body(
            strategy_type,
            weekly_budget=weekly_micros,
            cpc=cpc_micros,
            cpa=cpa_micros,
            goal_id=goal_id,
            crr=crr,
        )
        logger.info("Обновление стратегии кампании %d: %s", campaign_id, strategy_type)
        return await self._update_strategy_with_fallback(campaign_id, strategy_body, f"strategy:{strategy_type}")

    async def update_campaign_cpa(self, campaign_id: int, cpa: float, goal_id: int = None) -> bool:
        """Изменить целевую стоимость конверсии (CPA)."""
        return await self.update_campaign_strategy(
            campaign_id,
            strategy_type="AVERAGE_CPA",
            cpa=cpa,
            goal_id=goal_id,
        )

    async def update_campaign_cpc(self, campaign_id: int, cpc: float, weekly_budget: float = None) -> bool:
        """Изменить среднюю цену клика (CPC)."""
        return await self.update_campaign_strategy(
            campaign_id,
            strategy_type="AVERAGE_CPC",
            cpc=cpc,
            weekly_budget=weekly_budget,
        )
