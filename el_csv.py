import requests
import pandas as pd


class DataPipeline:
    def __init__(self) -> None:
        self.new_data = pd.DataFrame()
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
        pass
    def load(self) -> None:
        '''Try to open existing data and add to it, otherwise create new file.'''
        file_name = 'bond_timeseries.csv'
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
    foo = DataPipeline()
    foo.extract()
    foo.transform()
    foo.load()

if __name__ == '__main__':
    main()
