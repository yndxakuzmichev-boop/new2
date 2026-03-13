import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from app.assistant import MarketingAssistant
from app.config import settings
from app.direct_client import DirectClient
from app.gpt_client import YandexGPTClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

direct_client: DirectClient | None = None
gpt_client: YandexGPTClient | None = None
assistant: MarketingAssistant | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global direct_client, gpt_client, assistant
    logger.info("Инициализация клиентов...")
    direct_client = DirectClient()
    gpt_client = YandexGPTClient()
    assistant = MarketingAssistant(direct_client, gpt_client)
    logger.info(
        "Клиенты инициализированы. Режим Директ: %s",
        "sandbox" if settings.direct_sandbox else "production",
    )
    yield
    logger.info("Завершение работы приложения.")


app = FastAPI(
    title="AI Marketing Assistant",
    description="AI-ассистент для управления Яндекс Директ на базе YandexGPT",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ChatRequest(BaseModel):
    message: str
    history: list = []


class ChatResponse(BaseModel):
    response: str


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/chat", response_model=ChatResponse)
async def chat(body: ChatRequest):
    if not assistant:
        raise HTTPException(status_code=503, detail="Ассистент не инициализирован")
    if not body.message or not body.message.strip():
        raise HTTPException(status_code=400, detail="Сообщение не может быть пустым")
    try:
        response = await assistant.process_message(
            user_message=body.message.strip(),
            history=body.history,
        )
        return ChatResponse(response=response)
    except Exception as e:
        logger.error("Ошибка в /chat: %s", e)
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка: {e}")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/campaigns")
async def get_campaigns():
    if not direct_client:
        raise HTTPException(status_code=503, detail="DirectClient не инициализирован")
    try:
        campaigns = await direct_client.get_campaigns()
        return {"campaigns": campaigns}
    except Exception as e:
        logger.error("Ошибка в /campaigns: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
