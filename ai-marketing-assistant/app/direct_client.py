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
            "Client-Login": settings.direct_client_login,
            "Accept-Language": "ru",
            "Content-Type": "application/json",
        }

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
                "SelectionCriteria": {},
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

    async def update_campaign_budget(self, campaign_id: int, amount: float) -> bool:
        # Директ API принимает суммы в микрорублях (умножаем на 1_000_000)
        amount_micros = int(amount * 1_000_000)
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
            result = data.get("result", {})
            update_results = result.get("UpdateResults", [])
            if update_results and update_results[0].get("Errors") is None:
                logger.info(
                    "Бюджет кампании %d обновлён до %.2f руб.", campaign_id, amount
                )
                return True
            errors = update_results[0].get("Errors", []) if update_results else []
            logger.warning(
                "Не удалось обновить бюджет кампании %d: %s", campaign_id, errors
            )
            return False
        except Exception as e:
            logger.error("Ошибка при обновлении бюджета кампании %d: %s", campaign_id, e)
            return False
