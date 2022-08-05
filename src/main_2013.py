import os
import subprocess
from pathlib import Path
from multiprocessing import Pool

import pandas as pd
import krtools


L1_SORTING_COLUMNS = [
    'OpenPx', 'HighPx', 'LowPx', 'LastPx', 'ClosePx', 'Volume', 'Amount', 'NumTrades'
]
L2_SORTING_COLUMNS = [
    'OpenPx', 'HighPx', 'LowPx', 'LastPx', 'ClosePx', 'Volume', 'Amount', 'NumTrades', 'TotalBidQty', 'WeightedAvgBidPx', 'TotalOfferQty', 'WeightedAvgOfferPx'
]

L1_PRECISIONS = {
    0: [
        'Volume', 'NumTrades'
    ],
    3: [
        'PreClosePx', 'OpenPx', 'HighPx', 'LowPx', 'LastPx', 'ClosePx',
        'Amount', 'AveragePx', 'WeightedAvgPxChg',
        'PreWeightedAvgPx',
    ],
    6: [
        'PeRatio1', 'PeRatio2', 'TotalLongPosition'
    ]
}

L2_PRECISIONS = {
    0: [
        'Volume', 'BidNumOrders', 'OfferNumOrders', 'NumTrades'
    ],
    3: [
        'PreClosePx', 'OpenPx', 'HighPx', 'LowPx', 'LastPx', 'ClosePx',
        'Amount', 'AveragePx', 'WeightedAvgBidPx',
        'WeightedAvgOfferPx', 'WeightedAvgPxChg', 'PreWeightedAvgPx'
    ],
    6: [
        'TotalBidQty', 'TotalOfferQty', 'Change1', 'Change2', 'TotalLongPosition',
        'PeRatio1', 'PeRatio2', 'UpperLimitPx', 'LowerLimitPx'
    ]
}

IDX_PRECISIONS = {
    0: ['Volume'],
    4: [
        'PreClosePx', 'OpenPx', 'HighPx', 'LowPx', 'LastPx',
        'ClosePx', 'AveragePx', 'WeightedAvgPxChg', 'PreWeightedAvgPx'
    ],
    5: [
        'Amount'
    ],
    6: [
        'PeRatio1', 'PeRatio2',
    ]
}

baseDir = Path('/home/data/my_test/ytliu/SZ_L1_L2')
inDir = Path()
extractDir = baseDir.joinpath('extract')
fixedDir = baseDir.joinpath('fixed')
outDir = baseDir.joinpath('out')
logDir = baseDir.joinpath('log')

inDir.mkdir(parents=True, exist_ok=True)
extractDir.mkdir(parents=True, exist_ok=True)
fixedDir.mkdir(parents=True, exist_ok=True)
outDir.mkdir(parents=True, exist_ok=True)
logDir.mkdir(parents=True, exist_ok=True)


def extract7z(_7zFile: Path) -> str:
    try:
        print(f'Extracting   {_7zFile}')
        extractDst = extractDir.joinpath(
        _7zFile.relative_to(inDir).parent
        )
        extractDst.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ['7z', 'x', '-aos', _7zFile, f'-o{extractDst}'],
            stdout=subprocess.DEVNULL
        )
        print(f'Extracted to {extractDst}')
        return f'INFO: {_7zFile} succeed'
    except Exception as e:
        return f'ERROR: {_7zFile} Failed\n\t{e}'


def extract():
    _7zFiles = []
    for root, dirs, files in os.walk(inDir):
        _7zFiles.extend([
            Path(os.path.join(root, f))
            for f in filter(lambda x: x.endswith('7z'), files)
        ])

    with Pool() as p:
        with open(logDir.joinpath('extract.log'), 'w') as log:
            for each in p.map(extract7z, _7zFiles):
                print(each, file=log)


def fixPrice(x) -> str:
    precision = 3
    if isinstance(x, str) and '[' in x:
        nums = x.split(',')
        nums[0] = nums[0][1:]
        nums[-1] = nums[-1][:-1]
        return '[' + ','.join([krtools.getRound(each.strip(), precision) for each in nums]) + ']'
    if pd.isna(x):
        return ''
    if isinstance(x, float):
        return krtools.getRound(str(x).strip(), precision)
    return x


def myRound(x: str, precision: int) -> str:
    try:
        return krtools.getRound(x, precision)
    except:
        return ''


def fixCSV(csvFilename: Path) -> str:
    try:
        print(f'Fixing  {csvFilename}')
        csvDst = fixedDir.joinpath(
            csvFilename.relative_to(extractDir)
        )
        csvDst.parent.mkdir(parents=True, exist_ok=True)
        if csvDst.exists():
            print(f'{csvDst} exists, skipping')
            return f'WARNING: {csvDst} exists'

        if 'L1' in str(csvFilename.parent.name):
            sortingColumns = L1_SORTING_COLUMNS
        else:
            sortingColumns = L2_SORTING_COLUMNS

        df = pd.read_csv(csvFilename)
        df = df.fillna('')
        df['Volume'] = df['Volume'].astype(int)

        df.loc[df['Volume'] == 0, sortingColumns] = 0

        df[sortingColumns] = df[sortingColumns].sort_values(by=['Volume']).values
        closePxVal = df.loc[df['Volume'].idxmax(), 'ClosePx']
        if int(closePxVal) == 0:
            closePxVal = df['ClosePx'].max()

        df['ClosePx'] = df['ClosePx'].map(lambda x: x if x == 0 else closePxVal)
        
        df['BidPrice'] = df['BidPrice'].map(fixPrice)
        df['OfferPrice'] = df['OfferPrice'].map(fixPrice)

        if 'IDX' in str(csvFilename):
            precisions = IDX_PRECISIONS
        elif 'L1' in str(csvFilename.parent.name):
            precisions = L1_PRECISIONS
        else:
            precisions = L2_PRECISIONS
        
        for precision, columns in precisions.items():
            if precision != 0:
                df[columns] = df[columns].applymap(lambda x: '' if pd.isna(x) else myRound(str(x), precision))
            else:
                df[columns] = df[columns].applymap(lambda x: '' if pd.isna(x) else str(int(x)))

        df.to_csv(csvDst, index=False)
        print(f'Written {csvDst}')
        return f'INFO: {csvFilename} succeed'
        
    except Exception as e:
        return f'ERROR: {csvFilename} Failed\n\t{e}'


def fix():
    csvFiles = []
    for root, dirs, files in os.walk(extractDir):
        csvFiles.extend(
            [Path(os.path.join(root, file)) for file in filter(lambda x: x.endswith('csv'), files)]
        )
    
    with Pool() as p:
        with open(logDir.joinpath('fix.log'), 'w') as log:
            for each in p.map(fixCSV, csvFiles):
                print(each, file=log, flush=True)


def compress7z(_7zFile: Path) -> str:
    try:
        srcDir = fixedDir.joinpath(
            _7zFile.relative_to(inDir)
        )
        srcDir = Path(str(srcDir).split('.')[0])
        print(f'Compressing {srcDir}')

        dstFile = outDir.joinpath(
            _7zFile.relative_to(inDir)
        )
        dstFile.parent.mkdir(parents=True, exist_ok=True)

        subprocess.run(
            ['7z', 'u', dstFile, srcDir],
            stdout=subprocess.DEVNULL
        )

        print(f'Compressed {dstFile}')
        return f'INFO: {dstFile} succeed'
    except Exception as e:
        return f'ERROR: {srcDir} failed\n\t{e}'
    

def compress():
    _7zFiles = []
    for root, dirs, files in os.walk(inDir):
        _7zFiles.extend([
            Path(os.path.join(root, f))
            for f in filter(lambda x: x.endswith('7z'), files)
        ])
    
    with open(logDir.joinpath('compress.log'), 'w') as log:
        for each in _7zFiles:
            print(compress7z(each), file=log, flush=True)

    # with Pool() as p:
    #     with open(logDir.joinpath('compress.log'), 'w') as log:
    #         for each in p.map(compress7z, _7zFiles):
    #             print(each, file=log, flush=True)
        

def extractSingleDay(date: str):
    _7zDir = inDir.joinpath(f'{date}/HFM')
    with Pool() as p:
        print('*' * 100)
        print(p.map(extract7z, _7zDir.iterdir()))


if __name__ == '__main__':
    # extract()
    # fix()
    compress()
    pass
