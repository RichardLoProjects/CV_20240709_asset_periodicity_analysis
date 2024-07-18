# CV_20240709_AssetPeriodicityAnalysis
# Optimal Bidding

### Motivation

As part of an operations team in an airline company, you are tasked with purchasing fuel. The fuel is derived from oil found on the commodity market which has been known to fluctuate wildly, especially due to the recent energy crisis. An opportunity presents itself here because a lot of money can be saved by stockpiling inventory when oil prices are low, and waiting while prices are high. A way to do this is by placing bids at optimal prices within the limit order book which handles trading the oil/fuel. Care must be exercised here as placing a bid too low will be too unrealistic and never be filled, leaving you and your company without the necessary oil to fly your planes and keep your business running. The problem statement manifests as the following.

_Given an API to a timeseries data set which prices a desired asset, where in the order book does the optimal bid price lie that ensures a high purchase chance for this asset within a given deadline? An additional constraint is a strictly positive inventory must be maintained which adds urgency to the optimal bid as inventory dwindles, pushing your bid up._

### Disclaimer

Due to legal and monetary issues with API keys, the project will be remodelled to consider purchasing Oldschool Bonds (the desired asset) from Oldschool Runescape Exchange. The problem statement remains unchanged, as a player within the game must maintain Bonds and can save a lot of money if they manage to buy at optimal prices.

### Project Aims

- Track an important asset.
- Investigate periodicity within said asset.
- Generate a trading strategy.
- Backtest said strategy.

### Project Outcomes

- `eda.ipynb`: an ET data pipeline for feature engineering, visualisation, and discussion, intended for a data analyst or quant with further research in mind.
- `fetch.py`: an EL data pipeline for compressed proprietary storage, intended for manual use at home.
- `bond_timeseries.csv`: the data.
- `pipeline.py`: a modified version of `fetch.py` with feature engineering and database handling, intended to be run on a remote server using `.env`.
- `backtest.ipynb`: simulation of a trading bot and backtest.

### Conclusion

While the exploratory data analysis does indeed return evidence for periodicity in stock price, the implementation of feature engineering proved to be quite poor, losing quite a significant amount of money. In other words, while project aims were met, more work must be done to tackle the problem statement.
