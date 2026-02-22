from pydantic import BaseModel, Field, validator
from typing import List
from datetime import date

class QuarterEntry(BaseModel):
    Quarter: int
    Quantity: float

class SchedulePush(BaseModel):
    Datum: date
    Bilanzkreis: str
    Version: int
    Quarters: List[QuarterEntry]

    @validator('Quarters')
    def validate_quarters_length(cls, v):
        if not (92 <= len(v) <= 100):
            raise ValueError('Quarters list must be between 92 and 100 items')
        return v