from dateutil import parser, tz
from datetime import datetime, timezone
import logging
import sys
import pandas as pd

logging.basicConfig(level=logging.DEBUG,
                    format=' %(asctime)s - %(levelname)s - %(message)s')


def rename_fields(df):
    """Rename fields to correspond to databases."""
    df.rename(columns={'Jurisdiction': 'PT', '# Patients Tested': 'PatientTestedCount',
                       '# Confirmed Positive': 'ConfirmedPositiveCount',
                       '# Confirmed Negative': 'ConfirmedNegativeCount',
                       'Change in # Patients Tested': 'PatientTestedCountChange',
                       'Change in # Confirmed Positive': 'ConfirmedPositiveCountChange',
                       'Change in # Confirmed Negative': 'ConfirmedNegativeCountChange',
                       'Jurisdictional and Canadian % Positivity Rates': 'PTAndCanadianPositivityRatePrct',
                       'Patients Tested per 10^{0} Canadians': 'CanadianPatientsTestedPerMil',
                       'Patients Tested per 10^{0} by Jurisdiction': 'PTPatientsTestedPerMil',
                       'Date Last Updated': 'DateUpdated'
                       }, inplace=True)
    return df


def jurisdiction(df):
    """Remove asterisk from Ontario."""
    df['PT'] = df['PT'].replace('ON*', 'ON')
    return df


def date_convert(date_to_convert):
    return datetime.strptime(date_to_convert, '%Y-%m-%d (%H:%M:%S)').replace(tzinfo=tz.timezone('Canada/Central'))


def update_date(df):
    """Reformat date to more standard format."""
    # df['DateUpdated'] = df['DateUpdated'].apply(date_convert)
    tzinfos = {"CST": tz.gettz('Canada/Central')}
    df['DateUpdated'] = df['DateUpdated'].str.replace(
        '(', '').str.replace(')', ' CST').apply(lambda x: parser.parse(x, tzinfos=tzinfos))
    return df


def date_loaded(df):
    """Add a new column recording when PHAC received the record."""
    df['DateLoaded'] = datetime.now().replace(tzinfo=timezone.utc)
    return df


def main(data_file):
    logging.info(f'Loading file and transforming fields...')
    dtypes = {
        'Jurisdiction': 'str',
        '# Patients Tested': 'int64',
        '# Confirmed Positive': 'int64',
        '# Confirmed Negative': 'int64',
        'Change in # Patients Tested': 'int64',
        'Change in # Confirmed Positive': 'int64',
        'Change in # Confirmed Negative': 'int64',
        'Jurisdictional and Canadian % Positivity Rates': 'float64',
        'Patients Tested per 10^{0} Canadians': 'float64',
        'Patients Tested per 10^{0} by Jurisdiction': 'float64'
    }
    df = pd.read_csv(data_file, dtype=dtypes, skipfooter=4, engine='python')
    df = rename_fields(df)
    df = jurisdiction(df)
    df = update_date(df)

    df = date_loaded(df)
    logging.info(f'Transform complete. Exporting to csv...')
    df.to_csv(data_file.replace(".csv", "_transformed.csv"), index=False)


if __name__ == '__main__':
    data_file = sys.argv[1]
    logging.info(f'Starting program. File to transform: {data_file}')
    main(data_file)
