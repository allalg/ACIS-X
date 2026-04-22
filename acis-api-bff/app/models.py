from pydantic import BaseModel


class JobState(BaseModel):
    job_id: str
    status: str
    started_at: str
