from datetime import datetime, date
from typing import Literal


def teste():
    start_date = date(2023, 1, 20)
    end_date = date(2023, 10, 12)

    for year in range(start_date.year, end_date.year + 1):
        print(year)

if __name__ == "__main__":
    teste()