from enum import Enum
from typing import Dict

from pydantic import BaseModel


class AnalysisTypes(Enum):
    chemical = 'chemical'
    microbiological = 'microbiological'


class AnalysisReport(BaseModel):
    filename: str = None
    zone_id: int = None
    title: str
    sampling_address: str = None
    issue_date: str = None
    sample_date: str = None
    analysis_date: str = None
    result: Dict
    type: AnalysisTypes
