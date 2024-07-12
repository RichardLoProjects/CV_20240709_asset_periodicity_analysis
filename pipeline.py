import requests
import pandas as pd
import numpy as np
from scipy.fft import fft, fftfreq, ifft
from scipy.signal import find_peaks
import os
from dotenv import load_dotenv # type: ignore
import psycopg2 as psql # type: ignore
import warnings


class EnvSecrets:
    def __init__(self) -> None:
        '''Fetch all sensitive data and load into variables for later use.'''
        load_dotenv()
        self.db_name = os.getenv('DATABASE_NAME')
        self.db_host = os.getenv('DATABASE_HOST')
        self.db_port = int(os.getenv('DATABASE_PORT'))
        self.sql_user = os.getenv('SQL_USERNAME')
        self.sql_pass = os.getenv('SQL_PASSWORD')
        self.tablename = os.getenv('SQL_TABLENAME')

class DatabaseConnection:
    def __init__(self, secret:EnvSecrets) -> None:
        '''Open database connection.'''
        try:
            self.connection = psql.connect(
                database = secret.db_name
                , host = secret.db_host
                , port = secret.db_port
                , user = secret.sql_user
                , password = secret.sql_pass
            )
            self.cursor = self.connection.cursor()
            self.valid = True
        except:
            '''Possible expansion for email sending, notifying database failure or invalid credentials.'''
            self.valid = False
    def close(self) -> None:
        '''Close database connection.'''
        try:
            self.connection.close()
        except:
            pass

class DataPipeline:
    def __init__(self, secret) -> None:
        self.new_data = pd.DataFrame()
        self.secret:EnvSecrets = secret
    def extract(self) -> None:
        '''Extract from API, and insert into dataframe.'''
        item_id = 13190
        timestep = '5m'
        intentions = {
            'User-Agent': 'Need to find optimal price to buy CHEAP bonds.',
            'From': 'A poor osrs player with 400 total level.'
        }
        url = f'https://prices.runescape.wiki/api/v1/osrs/timeseries?timestep={timestep}&id={item_id}'
        json_data:list[dict] = requests.get(url, headers=intentions).json()['data']
        for data_point in json_data:
            row = {
                'unix_time': data_point.get('timestamp')
                , 'bid_price': data_point.get('avgLowPrice')
                , 'bid_volume': data_point.get('lowPriceVolume', 0)
                , 'ask_price': data_point.get('avgHighPrice')
                , 'ask_volume': data_point.get('highPriceVolume', 0)
            }
            temp_df = pd.DataFrame.from_dict([row], orient='columns')
            self.new_data = pd.concat([self.new_data, temp_df], ignore_index=True)
    def transform(self) -> None:
        '''Feature Engineering.'''
        self.new_data['time_stamp'] = pd.to_datetime(self.new_data['unix_time'], unit='s')
        self.new_data['bid_delta'] = self.new_data['bid_price'].astype(float).diff()
        self.new_data['bid_delta'].fillna(0, inplace=True)
        delta_signal = self.new_data['bid_delta'].values
        n = len(delta_signal)
        timestep = (self.new_data['time_stamp'][1] - self.new_data['time_stamp'][0]).total_seconds()
        self.new_data['bid_freq'] = fftfreq(n, d=timestep)
        amplitude = np.abs(fft(delta_signal)) / (n/2)
        self.new_data['bid_amplitude'] = amplitude
        amp_mean = amplitude.mean()
        amp_std = amplitude.std()
        self.new_data['bid_amp_clean'] = np.where(amplitude > (3*amp_std+amp_mean), amplitude, 0)
        clean_fft = fft(delta_signal)
        clean_fft[self.new_data['bid_amp_clean'] == 0] = 0
        self.new_data['bid_delta_clean'] = ifft(clean_fft).real
        peaks, _ = find_peaks(-self.new_data['bid_delta_clean'])
        self.new_data['signal'] = None
        self.new_data.loc[peaks, 'signal'] = 'buy'
        self.new_data['ask_delta'] = self.new_data['ask_price'].astype(float).diff()
        self.new_data['ask_delta'].fillna(0, inplace=True)
        delta_signal = self.new_data['ask_delta'].values
        n = len(delta_signal)
        timestep = (self.new_data['time_stamp'][1] - self.new_data['time_stamp'][0]).total_seconds()
        self.new_data['ask_freq'] = fftfreq(n, d=timestep)
        amplitude = np.abs(fft(delta_signal)) / (n/2)
        self.new_data['ask_amplitude'] = amplitude
        amp_mean = amplitude.mean()
        amp_std = amplitude.std()
        self.new_data['ask_amp_clean'] = np.where(amplitude > (3*amp_std+amp_mean), amplitude, 0)
        clean_fft = fft(delta_signal)
        clean_fft[self.new_data['ask_amp_clean'] == 0] = 0
        self.new_data['ask_delta_clean'] = ifft(clean_fft).real
        peaks, _ = find_peaks(-self.new_data['ask_delta_clean'])
        self.new_data['signal'] = None
        self.new_data.loc[peaks, 'signal'] = 'sell'
    def load(self, database) -> None:
        primary_key = 'transaction_id'
        create_table_sql = f'''
CREATE TABLE IF NOT EXISTS {self.secret.tablename} (
    {primary_key} SERIAL PRIMARY KEY
    , time_stamp timestamp
    , unix_time int
    , bid_price int
    , ask_price int
    , bid_volume int
    , ask_volume int
    , bid_delta float
    , bid_freq float
    , bid_amplitude float
    , bid_amp_clean float
    , bid_delta_clean float
    , signal varchar(12)
    , ask_delta float
    , ask_freq float
    , ask_amplitude float
    , ask_amp_clean float
    , ask_delta_clean float
);'''
        database.cursor.execute(create_table_sql)
        _df = _df.replace({np.nan: None})
        query = f'SELECT * FROM {self.secret.tablename}'
        existing_data = pd.read_sql(query, database.connection)
        pass
    def db_emergency(self) -> None:
        '''Try to open existing data and add to it, otherwise create new file.'''
        file_name = 'timeseries_2024_07_12.csv'
        try:
            existing_data = pd.read_csv(file_name)
            for column_name in existing_data.columns:
                if 'Unnamed' in column_name:
                    existing_data.drop(columns=column_name, inplace=True)
            incoming_data = pd.concat([existing_data, self.new_data], ignore_index=True)
            incoming_data.set_index('unix_time', inplace=True)
            incoming_data.drop_duplicates(keep='first', inplace=True)
            incoming_data.sort_index(inplace=True)
            incoming_data.reset_index(inplace=True)
            incoming_data.to_csv(file_name)
        except FileNotFoundError:
            self.new_data.to_csv(file_name)


def main() -> None:
    warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy connectable.*')
    my_secrets = EnvSecrets()
    my_database = DatabaseConnection(my_secrets)
    foo = DataPipeline()
    foo.extract()
    foo.transform()
    try:
        foo.load(my_database)
    except:
        foo.db_emergency()
    finally:
        my_database.close()

if __name__ == '__main__':
    main()
