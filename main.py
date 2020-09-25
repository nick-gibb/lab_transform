from dateutil import parser
import logging
import sys
import pandas as pd

logging.basicConfig(level=logging.DEBUG,
                    format=' %(asctime)s - %(levelname)s - %(message)s')

def rename_fields(df):
    """Rename fields to correspond to databases."""
    df.rename(columns={'Jurisdiction': 'jurisdiction', '# Patients Tested': 'num_pat_tested',
                       '# Confirmed Positive': 'num_confirmed_pos',
                       '# Confirmed Negative': 'num_confirmed_neg',
                       'Change in # Patients Tested': 'change_pat_tested',
                       'Change in # Confirmed Positive': 'change_confirmed_pos',
                       'Change in # Confirmed Negative': 'change_confirmed_neg',
                       'Jurisdictional and Canadian % Positivity Rates': 'jurisdictional_canada_pos_rt',
                       'Patients Tested per 10^{0} Canadians': 'tests_per_capita_canada',
                       'Patients Tested per 10^{0} by Jurisdiction': 'tests_per_capita_jurisdiction',
                       'Date Last Updated': 'update_date'
                       }, inplace=True)
    return df


def truncate(df):
    """Remove the extra rows that are not needed."""
    df = df[df['jurisdiction'].isin(
        ['AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'NT', 'NU', 'ON', 'PE', 'QC', 'SK', 'YT', 'Total'])]
    return df


def jurisdiction(df):
    """Remove asterisk from Ontario."""
    df['jurisdiction'] = df['jurisdiction'].replace('ON*', 'ON')
    return df


def update_date(df):
    """Reformat date to more standard format."""
    df['update_date'] = df['update_date'].str.replace(
        '(', '').str.replace(')', '').apply(lambda x: parser.parse(x))
    return df

def phac_date(df):
    """Add a new column recording when PHAC received the record."""
    df['phac_date'] = pd.datetime.now()
    return df

def main(data_file):
    logging.info(f'Loading file and transforming fields...')
    df = pd.read_csv(data_file)
    df = rename_fields(df)
    df = jurisdiction(df)
    df = truncate(df)
    df = update_date(df)
    df = phac_date(df)
    logging.info(f'Transform complete. Exporting to csv...')
    print(df)
    df.to_csv(data_file.replace(".csv", "_transformed.csv"), index=False)


if __name__ == '__main__':
    data_file = sys.argv[1]
    logging.info(f'Starting program. File to transform: {data_file}')
    main(data_file)
