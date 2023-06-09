import pandas as pd
import json
import csv
import json
import gzip
import io
import requests

csv.field_size_limit(500 * 1024 * 1024)


def fetch(url):
    response = requests.get(url)
    return response.content


def decompress_gzip(url):
    content = fetch(url)
    print("Downloaded")
    decompressed_data = gzip.decompress(content)
    content = io.StringIO(decompressed_data.decode())
    print("Decompressed")
    return content


def parse_format_from_url(url):
    if format is None or format != 'CSV' or format != 'PARQUET':
        if 'csv' in url:
            return 'CSV'
        elif 'parquet' in url:
            return 'PARQUET'
        else:
            return None


def read_as_dataframe(url, format):
    if format is None or format != 'CSV' or format != 'PARQUET':
        format = parse_format_from_url(url)

    if format is None:
        raise ValueError(f'Invalid or empty format has been provided:{format}')

    df = None

    if format == 'CSV':
        csv_file = decompress_gzip(url)
        df = pd.read_csv(csv_file, delimiter="\t")
        print('Downloaded CSV')
    elif format == 'PARQUET':
        df = pd.read_parquet(url)
        print('Downloaded PARQUET')

    bool_columns = ['TARGETED_PROMOTION', 'SKY_ULTRA_HD']
    df[bool_columns] = df[bool_columns].astype(bool)

    json_columns = [
        'SESSION_START',
        'SESSION_END',
        'HOUSEHOLD',
        'DEVICE',
        'PANEL_VIEWERS',
        'GUEST_VIEWERS',
        'PROGRAMMES_VIEWED',
        'SPOTS_VIEWED',
        'PANEL',
        'VIEWING_STATION',
        'START_OF_RECORDING',
        'VOD_PROVIDER']

    for column in json_columns:
        df[column] = df[column].apply(json.loads)

    return df


