import os

from pathlib import Path
from multiprocessing import Pool

import numpy as np
import pandas as pd


is_sorted = lambda x: np.all(x[:-1] <= x[1:])

def check(csvFile: Path):
    df = pd.read_csv(csvFile)

    s = df['Amount']
    return is_sorted(s.values), csvFile


def checkDir(csvDir: Path):
    for csvFile in csvDir.iterdir():
        if not check(csvFile)[0]:
            print(csvFile)


def checkLog():
    lines1 = set(open('a.log').readlines())
    lines2 = set(open('a.log.bak').readlines())

    for line in (lines1):
        df = pd.read_csv(line.strip())
        if not is_sorted(df.Amount.values):
            print(line.strip())


def f():
    lines = open('a.log').readlines()
    dates = [line.split('/')[-4] for line in lines]
    with open('date.log', 'w') as f:
        for line in lines:
            f.write(f'{line.split("/")[-4]}\n')


if __name__ == '__main__':
    # csvFiles = []
    # for root, dirs, files in os.walk('/home/data/my_test/ytliu/SZ_L1_L2/extract'):
    #     csvFiles.extend([Path(os.path.join(root, file)) for file in filter(lambda x: x.endswith('csv'), files)])
    
    # with Pool() as p:
    #     for each in filter(lambda x: x[0], p.map(check, csvFiles)):
    #         print(each[1])

    # csvDirs = []
    # for root, dirs, files in os.walk('/home/data/my_test/ytliu/SZ_L1_L2/fixed'):
    #     if len(files) > 0 and all([file.endswith('csv') for file in files]):
    #         csvDirs.append(Path(root))
        
    # with Pool() as p:
    #     list(p.map(checkDir, csvDirs))

    f()