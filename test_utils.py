import asyncio
import datetime
import time
from pathlib import PurePosixPath
from typing import Optional, List
from unittest import TestCase

from models import AnalysisReport
from report import get_abnormal_params
from utils import download_report_file, extract_reports, parse_value, parse_range


class TestPDFParser(TestCase):
    def test_download_report_file(self):
        path = asyncio.run(
            download_report_file(zone=9, date=datetime.date(year=2020, month=1, day=22),
                                 download_dir=PurePosixPath('.')))
        print(path)

    def test_download_report_file_many(self):
        date = datetime.date(year=2020, month=2, day=25)
        min_date = datetime.date(year=2020, month=1, day=1)
        while date >= min_date:
            for zone in range(54, 55):
                path: Optional[PurePosixPath] = asyncio.run(
                    download_report_file(zone=zone, date=date,
                                         download_dir=PurePosixPath('buletine')))

                print(f'zona: {zone} / {str(date)}: {path}')
            date = date - datetime.timedelta(days=1)
            time.sleep(1)

    def test_extract_tables(self):
        reports: List[AnalysisReport] = asyncio.run(
            extract_reports(PurePosixPath('z09_2020-01-22.pdf'))
        )

        for report in reports:
            print(report.json(indent=2))

    def test_get_abnormal_params_many(self):
        from os import listdir
        from os.path import isfile, join
        mypath = 'buletine'
        files = [join(mypath, f) for f in listdir(mypath) if isfile(join(mypath, f))]
        for file in files:
            print(f'Extracting {file}')
            reports: List[AnalysisReport] = asyncio.run(
                extract_reports(PurePosixPath(file))
            )
            for report in reports:
                nok = get_abnormal_params(report)
                if nok:
                    print(f'{file}: {nok}')
                else:
                    print(f'{file} is ok!')

    def test_download_extract_check_save_report(self):
        date = datetime.date(year=2019, month=4, day=1)
        min_date = datetime.date(year=2019, month=1, day=1)

        all_reports_file = 'data/all_reports.json'
        bad_reports_file = 'data/bad_reports.json'

        while date >= min_date:
            for zone in range(1, 55):
                print(f'Getting report for {date}, zone {zone}')

                # Step 1: Download
                path: Optional[PurePosixPath] = asyncio.run(
                    download_report_file(zone=zone, date=date,
                                         download_dir=PurePosixPath('reports')))

                if not path:
                    continue

                print(f'Downloaded: {path.name}')

                # Step 2: Extract
                reports: List[AnalysisReport] = asyncio.run(extract_reports(path))

                print(f'Found {len(reports)} reports...')

                # Steps 3 & 4: Check and save
                with open(all_reports_file, 'a') as all_f:
                    for report in reports:
                        all_f.writelines([report.json()])

                        nok = get_abnormal_params(report)
                        if nok:
                            with open(bad_reports_file, 'a') as bad_f:
                                bad_f.writelines([report.json()])

            date = date - datetime.timedelta(days=1)

    def test_parse_value(self):
        self.assertEqual(parse_value('0'), 0)

        self.assertEqual(parse_value('4.3 / 4.4'), 4.3)

        self.assertEqual(parse_value('45'), 45.0)
        self.assertEqual(parse_value('<45'), 44.99)
        self.assertEqual(parse_value('< 45'), 44.99)
        self.assertEqual(parse_value('>45'), 45.01)
        self.assertEqual(parse_value('≥ 45'), 45.01)

        self.assertEqual(parse_value('0.002'), 0.002)
        self.assertEqual(parse_value('≤0.002'), 0.00198)
        self.assertEqual(parse_value('≥0.002'), 0.00202)

        with self.assertRaises(ValueError) as ctx:
            parse_value('≥6.5; ≤9.5')

        self.assertEqual(
            str(ctx.exception),
            '"≥6.5; ≤9.5" must contain exactly 1 valid value to be convertible from range to single value')

        with self.assertRaises(ValueError) as ctx:
            parse_value('Acceptabil')

        self.assertEqual(str(ctx.exception),
                         'Cannot convert value "Acceptabil" into range.')

    def test_parse_range(self):
        self.assertEqual(parse_range('0'), (None, 0))

        self.assertEqual(parse_range('≤0.002'), (None, 0.002))
        self.assertEqual(parse_range('≥0.002'), (0.002, None))
        self.assertEqual(parse_range('0.0020'), (None, 0.002))

        with self.assertRaises(ValueError) as ctx:
            parse_value('Acceptabil')

        self.assertEqual(str(ctx.exception),
                         'Cannot convert value "Acceptabil" into range.')

    def test_parse_buletins(self):

        buletin_fizic = {
            'filename': 'z9_2020-02-20.pdf',
            'address': 'Sos. Stefan cel Mare nr. 11, Spitalul de Pneumoftiziologie',
            'data_emiterii': '21-02-2020',
            'data_primirii_probei': '20-02-2020',
            'data_efectuarii_analizelor': '20-02-2020 - 20-02-2020',
            'parameters': {
                'non_bio': {
                    'smell': {
                        'ro_name': 'Miros',
                        'um': None,
                        'value': 'Acceptabila',
                        'range': 'Acceptabila consumatorilor si nici o modificare anormala',
                    },
                    'taste': {
                        'ro_name': 'Gust',
                        'um': None,
                        'value': 'Acceptabila',
                        'range': 'Acceptabila consumatorilor si nici o modificare anormala',
                    },
                    'color': {
                        'ro_name': 'Culoare',
                        'um': 'grade / nm unitati pH',
                        'value': '1 / 455 7.58/21.5°C Acceptabila',
                        'range': 'Acceptabila consumatorilor si nici o modificare anormala',
                    },
                    'ph': {
                        'ro_name': 'pH',
                        'um': 'unitati pH',
                        'value': '7.58/21.5°C',
                        'range': (6.5, 9.5),
                    },
                    'conductivitate': {
                        'ro_name': 'Conductivitate',
                        'um': 'µS/cm la 25°C',
                        'value': 340,
                        'range': 2500,
                    },
                    'amoniu': {
                        'ro_name': 'Amoniu',
                        'um': 'mg/l',
                        'value': 0.02499,  # '<0.025'
                        'range': 0.5,
                    },
                    'nitriti': {
                        'ro_name': 'Nitriti',
                        'um': 'mg/l',
                        'value': 0.00199,  # '<0.002'
                        'range': 0.5,
                    },
                    'nitrati': {
                        'ro_name': 'Nitrati',
                        'um': 'mg/l',
                        'value': 4.75,
                        'range': 50,
                    },
                    'fier': {
                        'ro_name': 'Fier',
                        'um': 'µg/l',
                        'value': 32,
                        'range': 200,
                    },
                    'oxidabilitate': {
                        'ro_name': 'Oxidabilitate',
                        'um': 'mgO2/l',
                        'value': 1.31,
                        'range': 5.0,
                    },
                    'duritate_totala': {
                        'ro_name': 'Duritate totala',
                        'um': 'grade germane',
                        'value': 7.95,
                        'range': (5,),  # '>= 5'
                    },
                    'aluminiu': {
                        'ro_name': 'Aluminiu',
                        'um': 'µg/l',
                        'value': 28,
                        'range': 200,
                    },
                    'clor_rezidual_liber': {
                        'ro_name': 'Clor rezidual liber',
                        'um': 'mg/l',
                        'value': '0.44 / 1:57¹',  # ???
                        'range': (0.1, 0.5),
                    },
                    'turbiditate': {
                        'ro_name': 'Turbiditate',
                        'um': 'UNT',
                        'value': 0.509,
                        'range': (0, 5.0),  # '<=5'
                    },
                }
            }
        }

        alt_buletin_fizic = {
            'filename': 'z9_2020-02-21.pdf',
            'address': 'Sos. Stefan cel Mare nr. 11, Spitalul de Pneumoftiziologie',
            'data_emiterii': '25-02-2020',
            'data_primirii_probei': '21-02-2020',
            'data_efectuarii_analizelor': '21-02-2020 - 24-02-2020',
            'parameters': {
                'non_bio': {
                    'smell': {
                        'ro_name': 'Miros',
                        'um': None,
                        'value': 'Acceptabila',
                        'range': 'Acceptabila consumatorilor si nici o modificare anormala',
                    },
                    'taste': {
                        'ro_name': 'Gust',
                        'um': None,
                        'value': 'Acceptabila',
                        'range': 'Acceptabila consumatorilor si nici o modificare anormala',
                    },
                    'color': {
                        'ro_name': 'Culoare',
                        'um': 'grade / nm unitati pH',
                        'value': '1 / 455 7.56/21.7°C Acceptabila',
                        'range': 'Acceptabila consumatorilor si nici o modificare anormala',
                    },
                    'ph': {
                        'ro_name': 'pH',
                        'um': 'unitati pH',
                        'value': '7.56/21.7°C',
                        'range': (6.5, 9.5),
                    },
                    'conductivitate': {
                        'ro_name': 'Conductivitate',
                        'um': 'µS/cm la 25°C',
                        'value': 332,
                        'range': 2500,
                    },
                    'amoniu': {
                        'ro_name': 'Amoniu',
                        'um': 'mg/l',
                        'value': 0.02499,  # '<0.025'
                        'range': 0.5,
                    },
                    'nitriti': {
                        'ro_name': 'Nitriti',
                        'um': 'mg/l',
                        'value': 0.00199,  # '<0.002'
                        'range': 0.5,
                    },
                    'nitrati': {
                        'ro_name': 'Nitrati',
                        'um': 'mg/l',
                        'value': 5.21,
                        'range': 50,
                    },
                    'fier': {
                        'ro_name': 'Fier',
                        'um': 'µg/l',
                        'value': 44,
                        'range': 200,
                    },
                    'oxidabilitate': {
                        'ro_name': 'Oxidabilitate',
                        'um': 'mgO2/l',
                        'value': 1.19,
                        'range': 5.0,
                    },
                    'duritate_totala': {
                        'ro_name': 'Duritate totala',
                        'um': 'grade germane',
                        'value': 7.73,
                        'range': (5,),  # '>= 5'
                    },
                    'aluminiu': {
                        'ro_name': 'Aluminiu',
                        'um': 'µg/l',
                        'value': 30,
                        'range': 200,
                    },
                    'clor_rezidual_liber': {
                        'ro_name': 'Clor rezidual liber',
                        'um': 'mg/l',
                        'value': '0.44 / 2:54¹',  # ???
                        'range': (0.1, 0.5),
                    },
                    'turbiditate': {
                        'ro_name': 'Turbiditate',
                        'um': 'UNT',
                        'value': 0.475,
                        'range': (0, 5.0),  # '<=5'
                    },
                },
                'bio': {
                    'coliform_bacteria': {
                        'ro_name': 'Bacteriilor coliforme',
                        'um': 'UFC/100 ml',
                        'value': 0,
                        'range': 0,
                    },
                    'escherichia_coli': {
                        'ro_name': 'Escherichia coli',
                        'um': 'UFC/100 ml',
                        'value': 0,
                        'range': 0,
                    },
                    'enterococcus': {
                        'ro_name': 'Enterococi',
                        'um': 'UFC/100 ml',
                        'value': 0,
                        'range': 0,
                    },
                    'clostridium_perfringens': {
                        'ro_name': 'Clostridium Perfringens',
                        'um': 'UFC/100 ml',
                        'value': 0,
                        'range': 0,
                    }
                }
            }
        }
