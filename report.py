from typing import Dict, Tuple, Optional

from models import AnalysisReport


def is_in_range(value: float, range_: Tuple[Optional[float], Optional[float]]) -> bool:
    low, high = range_
    if low is None:
        low = value
    if high is None:
        high = value

    return low <= value <= high


def get_abnormal_params(report: AnalysisReport) -> Dict:
    nok_params = {}
    for key, value in report.result.items():
        if isinstance(value['range'], tuple):
            if not is_in_range(value['value'], value['range']):
                nok_params[key] = value

    return nok_params
