import asyncio
import datetime
from pathlib import PurePosixPath
from typing import List, Optional, Tuple

import aiofiles
import httpx
import tabula
from pandas import DataFrame

from models import AnalysisReport, AnalysisTypes


async def download_report_file(
        zone: int,
        date: datetime.date,
        download_dir: PurePosixPath) -> Optional[PurePosixPath]:
    if date >= datetime.date(year=2019, month=7, day=8):
        url = f'https://www.apanovabucuresti.ro/descarcab?z={zone}&d={date.month}-{date.day}-{date.year}'
    else:
        url = f'https://www.apanovabucuresti.ro/assets/pdf/{zone}_{date.day:02d}-{date.month:02d}-{date.year}.pdf'
    output_file = download_dir / f'{date.year}-{date.month:02d}-{date.day:02d}_z{zone:02d}.pdf'

    async with httpx.AsyncClient() as client:
        response = await client.get(url=url, timeout=10)

        if response.status_code != 200:
            print(f'GET {url} returned code {response.status_code} '
                  f'with content: {response.content}')
            return None
        if not response.content:
            return None

        async with aiofiles.open(output_file.as_posix(), mode='wb') as output:
            await output.write(response.content)

    return output_file


async def extract_reports(report_file: PurePosixPath) -> List[AnalysisReport]:
    loop = asyncio.get_running_loop()
    dfs = await loop.run_in_executor(None, tabula.read_pdf, report_file.as_posix())
    reports = []
    for df in dfs:
        report = await parse_report(df)
        report.filename = report_file.name
        reports.append(report)
    return reports


async def parse_report(df: DataFrame) -> AnalysisReport:
    rows = await normalize_table(df)
    header = rows.pop(0)

    if header[1] == 'Indicatori microbiologici':
        report_type = AnalysisTypes.microbiological
    elif header[1] == 'Indicatori organoleptici si fizico-chimici':
        report_type = AnalysisTypes.chemical
    else:
        print(f'table could not be parsed:\n{df}')
        raise ValueError(f'Cannot identify report type from "{header[1]}"')

    assert header[2] == 'Unitate de masura'
    assert header[3] == 'Valori obtinute'
    assert (header[4].startswith('Valori maxim admise') or
            header[4].startswith('Valori admise'))

    mapping = {
        # Chemical
        'Miros*': {'key': 'smell', 'ro_name': 'Miros'},
        'Gust*': {'key': 'taste', 'ro_name': 'Gust'},
        'Culoare*': {'key': 'color', 'ro_name': 'Culoare'},
        'pH': {'key': 'ph', 'ro_name': 'pH'},
        'Conductivitate': {'key': 'conductivitate', 'ro_name': 'Conductivitate'},
        'Amoniu': {'key': 'amoniu', 'ro_name': 'Amoniu'},
        'Nitriti': {'key': 'nitriti', 'ro_name': 'Nitriti'},
        'Nitrati': {'key': 'nitrati', 'ro_name': 'Nitrati'},
        'Fier': {'key': 'fier', 'ro_name': 'Fier'},
        'Oxidabilitate': {'key': 'oxidabilitate', 'ro_name': 'Oxidabilitate'},
        'Duritate totala': {'key': 'duritate_totala', 'ro_name': 'Duritate totala'},
        'Aluminiu': {'key': 'aluminiu', 'ro_name': 'Aluminiu'},
        'Clor rezidual liber': {'key': 'clor_rezidual_liber',
                                'ro_name': 'Clor rezidual liber'},
        'Turbiditate': {'key': 'turbiditate', 'ro_name': 'Turbiditate'},
        'Cloruri': {'key': 'cloruri', 'ro_name': 'Cloruri'},
        'Calciu*': {'key': 'calcium', 'ro_name': 'Calciu'},
        'Alcalinitate*': {'key': 'alcalinitate', 'ro_name': 'Alcalinitate'},
        'Sulfat*': {'key': 'sulfat', 'ro_name': 'Sulfat'},
        'Bor*': {'key': 'bor', 'ro_name': 'Bor'},
        'Cianuri libere*': {'key': 'cianuri', 'ro_name': 'Cianuri libere'},
        'Fluoruri*': {'key': 'fluoruri', 'ro_name': 'Fluoruri'},
        'Zinc*': {'key': 'zinc', 'ro_name': 'Zinc'},
        'Arsen*': {'key': 'arsen', 'ro_name': 'Arsen'},
        'Sulfuri si hidrogen sulfurat*': {'key': 'sulfuri', 'ro_name': 'Sulfuri si hidrogen sulfurat'},
        'Substante tensio-active*': {'key': 'subst_tensio-active', 'ro_name': 'Substante tensio-active'},
        'Potasiu*': {'key': 'potasiu', 'ro_name': 'Potasiu'},
        'Fenoli*': {'key': 'fenoli', 'ro_name': 'Fenoli'},
        'Fosfati*': {'key': 'fosfati', 'ro_name': 'Fosfati'},

        # Microbiological
        'Bacteriilor coliforme': {'key': 'coliform_bacteria',
                                  'ro_name': 'Bacterii coliforme'},
        'Escherichia coli': {'key': 'escherichia_coli', 'ro_name': 'Escherichia coli'},
        'Enterococi': {'key': 'enterococcus', 'ro_name': 'Escherichia coli'},
        'Clostridium Perfringens': {'key': 'clostridium_perfringens',
                                    'ro_name': 'Clostridium Perfringens'},
        'Numar de colonii la 22° C': {'key': 'colonii_22',
                                      'ro_name': 'Numar de colonii la 22° C'},
        'Numar de colonii la 36° C': {'key': 'colonii_36',
                                      'ro_name': 'Numar de colonii la 36° C'},
        'Pseudomonas Aeruginosa': {'key': 'pseudomonas_aeruginosa', 'ro_name': 'Pseudomonas Aeruginosa'},
    }
    data = {}
    for row in rows:
        value = row[3]

        try:
            range_ = parse_range(row[4])
        except ValueError:
            range_ = row[4]
        else:
            # Only search for value if the range was determined
            try:
                value = parse_value(row[3])
            except ValueError:
                pass

        data[mapping[row[1]]['key']] = {
            'ro_name': mapping[row[1]]['ro_name'],
            'um': row[2],
            'value': value,
            'range': range_
        }

    return AnalysisReport(
        title=header[1],
        result=data,
        type=report_type
    )


async def normalize_table(df: DataFrame) -> List[List[str]]:
    """ The dataframe can contain many more rows than the actual table from the PDF file.
    This function parses the dataframe and builds a table having the same number of rows
    as the one in the PDF file.

    """
    df_dict = df.to_dict("split")
    df_dict['data'].insert(0, df_dict['columns'])
    data: List[List[str]] = df_dict['data']
    header_row = [''] * len(data[0])

    # Step 1: figure out the header
    while isinstance(data[0][0], str) and not data[0][0].isdigit():
        row = data.pop(0)  # pop first row
        for column_idx, column in enumerate(row):
            if isinstance(column, str) and not column.startswith('Unnamed'):
                header_row[column_idx] += f' {column}'
                header_row[column_idx] = header_row[column_idx].strip()

    # Step 2: figure out the parameters rows
    normalized_data: List[List[str]] = []
    current_param: List[str] = header_row

    for row in data:
        if isinstance(row[0], str) and row[0].isdigit():
            # new parameter found
            normalized_data.append(current_param)
            current_param = [''] * len(row)

        for column_idx, column in enumerate(row):
            if isinstance(column, str):
                current_param[column_idx] += f' {column}'
                current_param[column_idx] = current_param[column_idx].strip()

    # Also insert the last item in data
    normalized_data.append(current_param)

    return normalized_data


def parse_value(value: str) -> float:
    """ Takes a string and tries to convert it into a single numeric value (float). If
    the input is an infinite interval (eg. >5; < 10 etc), then the output is a number
    close to the finite edge of the interval (eg. >5 -> 5.01, <10 -> 9.99 etc).

    """
    idx = 0
    for ch in value:
        if ch.isdigit() or ch == '.':
            idx += 1
        else:
            break

    if idx:
        return float(value[:idx])

    range_ = parse_range(value)

    # if the received value is a range and it needs to be returned as single
    # value, we decrease it a bit to be sure it's smaller than the range's max
    # eg: <2.0 -> 1.99 | <0.002 -> 0.0019..
    not_none = list(filter(None, range_))
    if len(not_none) != 1:
        raise ValueError(f'"{value}" must contain exactly 1 valid value to be '
                         f'convertible from range to single value')
    val = not_none[0]
    diff = val % 1 / 100 or 0.01
    if range_[0] is not None:
        return val + diff
    return val - diff


def parse_range(value: str) -> Tuple[Optional[float], Optional[float]]:
    """ Takes a string and tries to convert it to an interval. If the input is not an
    interval but a numeric value, it will be the upper edge of the interval.
    Eg.
        >5 -> (5, None)
        >5; <10 -> (5, 10)
        <4 -> (None, 4)
        4 -> (None, 4)
    """
    try:
        parsed = float(value)
    except ValueError:
        # continue parsing the string
        pass
    else:
        # return the value as interval
        return None, parsed

    comparators = ('<', '≤', '>', '≥')
    digits = ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
    if value[0] not in comparators + digits:
        raise ValueError(f'Cannot convert value "{value}" into range.')
    range_ = ['', '']

    # Hack (add +1) to ''.find() to return 0 (aka False) if nothing found
    lt_pos = (1 + value.find(comparators[0])) or (1 + value.find(comparators[1]))
    if lt_pos:
        for i in range(lt_pos, len(value)):
            if value[i].isspace():
                continue
            if not (value[i].isdigit() or value[i] == '.'):
                break
            range_[1] += value[i]

    ht_pos = (1 + value.find(comparators[2])) or (1 + value.find(comparators[3]))
    if ht_pos:
        for i in range(ht_pos, len(value)):
            if value[i].isspace():
                continue
            if not (value[i].isdigit() or value[i] == '.'):
                break
            range_[0] += value[i]

    left = float(range_[0]) if range_[0] else None  # None is -Infinity or 0
    right = float(range_[1]) if range_[1] else None  # None is Infinity here

    if left is right is None:
        raise ValueError(f'{value} cannot be converted to interval')
    return left, right
