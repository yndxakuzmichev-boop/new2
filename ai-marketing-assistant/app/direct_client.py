import logging
from io import StringIO

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

SANDBOX_BASE_URL = "https://api-sandbox.direct.yandex.com/json/v5/"
PROD_BASE_URL = "https://api.direct.yandex.com/json/v5/"


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

    async def get_campaigns(self) -> list:
        body = {
            "method": "get",
            "params": {
                "SelectionCriteria": {
                    "Statuses": ["DRAFT", "ACCEPTED", "MODERATION", "REJECTED"],
                },
                "FieldNames": [
                    "Id",
                    "Name",
                    "Status",
                    "State",
                    "Statistics",
                    "DailyBudget",
                ],
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

    async def get_campaign_stats(
        self, campaign_ids: list, date_from: str, date_to: str
    ) -> list:
        reports_url = self.base_url.replace("/json/v5/", "") + "/json/v5/reports"
        body = {
            "params": {
                "SelectionCriteria": {
                    "Filter": [
                        {
                            "Field": "CampaignId",
                            "Operator": "IN",
                            "Values": [str(cid) for cid in campaign_ids],
                        }
                    ],
                    "DateFrom": date_from,
                    "DateTo": date_to,
                },
                "FieldNames": [
                    "CampaignName",
                    "Impressions",
                    "Clicks",
                    "Ctr",
                    "AvgCpc",
                    "Cost",
                    "Conversions",
                    "CostPerConversion",
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
            reader = StringIO(tsv_text)
            lines = reader.readlines()

            if not lines:
                return []

            header_line = lines[0].rstrip("\n")
            field_names = header_line.split("\t")

            for line in lines[1:]:
                line = line.rstrip("\n")
                if not line or line.startswith("Total") or line.startswith("Итого"):
                    continue
                values = line.split("\t")
                row = dict(zip(field_names, values))
                rows.append(row)

            logger.info("Получено строк статистики: %d", len(rows))
            return rows
        except httpx.HTTPStatusError as e:
            logger.error("Ошибка HTTP при получении статистики: %s", e)
            return [{"error": f"HTTP ошибка: {e.response.status_code}"}]
        except Exception as e:
            logger.error("Ошибка при получении статистики: %s", e)
            return [{"error": str(e)}]

    async def pause_campaign(self, campaign_id: int) -> bool:
        body = {
            "method": "suspend",
            "params": {
                "SelectionCriteria": {"Ids": [campaign_id]},
            },
        }
        try:
            data = await self._post("campaigns", body)
            result = data.get("result", {})
            suspended = result.get("SuspendResults", [])
            if suspended and suspended[0].get("Errors") is None:
                logger.info("Кампания %d приостановлена", campaign_id)
                return True
            errors = suspended[0].get("Errors", []) if suspended else []
            logger.warning("Не удалось приостановить кампанию %d: %s", campaign_id, errors)
            return False
        except Exception as e:
            logger.error("Ошибка при остановке кампании %d: %s", campaign_id, e)
            return False

    async def enable_campaign(self, campaign_id: int) -> bool:
        body = {
            "method": "resume",
            "params": {
                "SelectionCriteria": {"Ids": [campaign_id]},
            },
        }
        try:
            data = await self._post("campaigns", body)
            result = data.get("result", {})
            resumed = result.get("ResumeResults", [])
            if resumed and resumed[0].get("Errors") is None:
                logger.info("Кампания %d запущена", campaign_id)
                return True
            errors = resumed[0].get("Errors", []) if resumed else []
            logger.warning("Не удалось запустить кампанию %d: %s", campaign_id, errors)
            return False
        except Exception as e:
            logger.error("Ошибка при запуске кампании %d: %s", campaign_id, e)
            return False

    async def _post_v501(self, endpoint: str, body: dict) -> dict:
        """POST к v501 endpoint — используется для Единой перфоманс-кампании (ЕПК/UPC)."""
        base = self.base_url.replace("/json/v5/", "/v501/")
        url = base + endpoint
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, json=body, headers=self.headers)
            response.raise_for_status()
            return response.json()

    async def _update_via_weekly_spending(self, campaign_id: int, amount_micros: int, amount: float) -> bool:
        """Обновление бюджета через WeeklySpendLimit для автостратегий (ЕПК, TextCampaign).
        
        Документация: https://yandex.ru/dev/direct/doc/ru/campaigns/update-text-campaign
        Для ЕПК используется endpoint /v501/ и структура UnifiedCampaign.
        Правильное поле: WeeklySpendLimit (не WeeklySpendingLimit!).
        """
        strategy_body = {
            "BiddingStrategyType": "WB_MAXIMUM_CLICKS",
            "WbMaximumClicks": {
                "WeeklySpendLimit": amount_micros,
            },
        }

        attempts = [
            # Первый приоритет: ЕПК через v501
            ("UnifiedCampaign", self._post_v501),
            # Второй: TextCampaign через v501
            ("TextCampaign", self._post_v501),
            # Третий: TextCampaign через json/v5 (обратная совместимость)
            ("TextCampaign", self._post),
        ]

        for campaign_type, post_fn in attempts:
            body = {
                "method": "update",
                "params": {
                    "Campaigns": [
                        {
                            "Id": campaign_id,
                            campaign_type: {
                                "BiddingStrategy": {
                                    "Search": strategy_body,
                                    "Network": {
                                        "BiddingStrategyType": "SERVING_OFF",
                                    },
                                }
                            },
                        }
                    ]
                },
            }
            try:
                data = await post_fn("campaigns", body)
                logger.info("Ответ %s weekly budget (fn=%s) для %d: %s",
                            campaign_type, post_fn.__name__, campaign_id, data)
                update_results = data.get("result", {}).get("UpdateResults", [])
                if update_results and not update_results[0].get("Errors"):
                    logger.info("Бюджет кампании %d обновлён через %s до %.2f руб.",
                                campaign_id, campaign_type, amount)
                    return True
                errors = update_results[0].get("Errors") if update_results else []
                logger.warning("Ошибки %s для %d: %s", campaign_type, campaign_id, errors)
            except Exception as e:
                logger.warning("Не удалось обновить через %s: %s", campaign_type, e)
        return False

    async def update_campaign_budget(self, campaign_id: int, amount: float) -> bool:
        # Директ API принимает суммы в микрорублях (умножаем на 1_000_000)
        amount_micros = int(amount * 1_000_000)

        # Шаг 1: пробуем DailyBudget (ручные стратегии)
        body = {
            "method": "update",
            "params": {
                "Campaigns": [
                    {
                        "Id": campaign_id,
                        "DailyBudget": {
                            "Amount": amount_micros,
                            "Mode": "STANDARD",
                        },
                    }
                ]
            },
        }
        try:
            data = await self._post("campaigns", body)
            logger.info("Ответ DailyBudget для кампании %d: %s", campaign_id, data)
            result = data.get("result", {})
            update_results = result.get("UpdateResults", [])
            if update_results:
                errors = update_results[0].get("Errors")
                if not errors:
                    logger.info("Бюджет кампании %d обновлён через DailyBudget до %.2f руб.", campaign_id, amount)
                    return True
                codes = [int(e.get("Code", 0)) for e in errors]
                logger.info("Коды ошибок для кампании %d: %s", campaign_id, codes)
                # Код 8000 — автостратегия, DailyBudget не поддерживается
                if 6000 in codes or 8000 in codes:
                    logger.info("DailyBudget не поддерживается (автостратегия), пробуем WeeklySpendingLimit")
                    return await self._update_via_weekly_spending(campaign_id, amount_micros, amount)
                logger.error("Ошибки DailyBudget для %d: %s", campaign_id, errors)
                return False
            return False
        except Exception as e:
            logger.error("Ошибка при обновлении бюджета кампании %d: %s", campaign_id, e)
            return False
