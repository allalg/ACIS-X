from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    api_key: str
    db_path: str
    allowed_origins: list[str]



def load_settings() -> Settings:
    api_key = os.getenv('ACIS_API_KEY', 'change_me')
    db_path = os.getenv('ACIS_DB_PATH', '../acis.db')
    allowed_origins = [
        origin.strip()
        for origin in os.getenv(
            'ACIS_ALLOWED_ORIGINS',
            'http://localhost:5173,https://your-vercel-domain.vercel.app',
        ).split(',')
        if origin.strip()
    ]
    return Settings(api_key=api_key, db_path=db_path, allowed_origins=allowed_origins)
