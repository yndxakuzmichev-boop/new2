from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    direct_token: str = ""
    direct_client_login: str = ""
    direct_sandbox: bool = True

    yandex_gpt_api_key: str = ""
    yandex_gpt_folder_id: str = ""

    app_host: str = "0.0.0.0"
    app_port: int = 8080


settings = Settings()
