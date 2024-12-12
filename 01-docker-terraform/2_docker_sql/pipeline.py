import sys

import pandas as pd

# for build docker use "docker build -t test:pandas ."
# for run docker use "docker run -it test:pandas"

print(sys.argv)

day = sys.argv[1]


print(f"Pandas version: {pd.__version__}")

print(f"Job finished successfully for day = {day}")