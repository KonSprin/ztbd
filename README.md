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
```
 NEO4J:
   Games: 89618
   Reviews: 1851779
   Developers: 60096
   Genres: 33
  Timings:
   Import Time: 584.9438633918762
   Verify Time: 71.1950330734253
   Drop Time: 40.20628261566162
```
```
MYSQL:
  Games: 89618
  Reviews: 1000000
  HLTBs: 147474
 Timings:
  Import Time: 211.13s
  Verify Time: 0.00s
  Drop Time: 0.62s
```
