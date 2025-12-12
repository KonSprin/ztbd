# ztbd

## Datasets

[Games](https://www.kaggle.com/datasets/artermiloff/steam-games-dataset/data?select=games_march2025_cleaned.csv)

[Reviews](https://www.kaggle.com/datasets/najzeko/steam-reviews-2021)

## Installation

`poetry install`

## Usage 
```
poetry run python mongo.py
poetry run python postgre.py
poetry run python neo.py
```

## IMPORT SUMMARY

```
MONGODB:
 Games: 89618
 Reviews: 100000

  Timings:
   Import Time: 56.70826959609985
   Verify Time: 0.18144679069519043
   Drop Time: 0.07973814010620117
```
```
POSTGRESQL:
 Games: 89618
 Reviews: 1000000

  Timings:
   Import Time: 90.76917624473572
   Verify Time: N/A
   Drop Time: 0.30413246154785156
```
