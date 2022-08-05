import os
import re
import subprocess
import traceback
from pathlib import Path
from multiprocessing import Pool

import numpy as np
import pandas as pd
import krtools


L1_SORTING_COLUMNS = [
    'PreClosePx', 'OpenPx', 'HighPx', 'LowPx', 'LastPx', 'ClosePx', 'Volume', 'Amount', 'AveragePx', 'NumTrades', 'PeRatio1', 'PeRatio2', 'WeightedAvgPxChg', 'PreWeightedAvgPx', 'TotalLongPosition', 'TradingPhaseCode'
]
L2_SORTING_COLUMNS = [
    'PreClosePx', 'OpenPx', 'HighPx', 'LowPx', 'LastPx', 'ClosePx', 'Volume', 'Amount', 'AveragePx', 'NumTrades', 'TotalBidQty', 'WeightedAvgBidPx', 'TotalOfferQty', 'WeightedAvgOfferPx', 'Change1', 'Change2', 'TotalLongPosition', 'PeRatio1', 'Peratio2', 'UpperLimitPx', 'LowerLimitPx', 'WeightedAvgPxChg', 'PreWeightedAvgPx', 'TradingPhaseCode'
]

L1_PRECISIONS = {
    0: [
        'Volume', 'NumTrades', 'TotalLongPosition'
    ],
    3: [
        'PreClosePx', 'OpenPx', 'HighPx', 'LowPx', 'LastPx', 'ClosePx',
        'Amount', 'AveragePx', 'WeightedAvgPxChg',
        'PreWeightedAvgPx',
    ],
    6: [
        'PeRatio1', 'PeRatio2'
    ]
}

L2_PRECISIONS = {
    0: [
        'Volume', 'BidNumOrders', 'OfferNumOrders', 'NumTrades', 'TotalBidQty', 'TotalOfferQty', 'TotalLongPosition', 
    ],
    3: [
        'PreClosePx', 'OpenPx', 'HighPx', 'LowPx', 'LastPx', 'ClosePx',
        'Amount', 'AveragePx', 'WeightedAvgBidPx',
        'WeightedAvgOfferPx', 'WeightedAvgPxChg', 'PreWeightedAvgPx'
    ],
    6: [
        'Change1', 'Change2',
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

BASE_DIR = Path('/home/data/my_test/ytliu/SZ_L1_L2')
IN_DIR = BASE_DIR.joinpath('in')
EXTRACT_DIR = BASE_DIR.joinpath('extract')
FIXED_DIR = BASE_DIR.joinpath('fixed')
OUT_DIR = BASE_DIR.joinpath('out')
LOG_DIR = BASE_DIR.joinpath('log')
TICK_DIR = BASE_DIR.joinpath('tick')

IN_DIR.mkdir(parents=True, exist_ok=True)
EXTRACT_DIR.mkdir(parents=True, exist_ok=True)
FIXED_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
TICK_DIR.mkdir(parents=True, exist_ok=True)


def get_tick(csv_file: Path) -> Path:
    '''
    csv_file:       /home/data/my_test/ytliu/SZ_L1_L2/extract/20151106/HFM/L2_new_STK_SZ_20151106/000001.csv
    tick_file:      /home/data/my_test/ytliu/SZ_L1_L2/tick/20151106/HFM/tick_new_STK_SZ_20151106/000001.csv
    tick_7z_file:   /home/data/my_test/ytliu/SZ_tick/20151106/HFM/tick_new_STK_SZ_20151106.7z
    cmd: 7z x /home/data/my_test/ytliu/SZ_tick/20151106/HFM/tick_new_STK_SZ_20151106.7z tick_new_STK_SZ_20151106/000001.csv -otick
    '''
    
    tick_file = Path(re.sub(r'/L[12]', '/tick',str(csv_file).replace('extract', 'tick')))
    if not tick_file.exists():
        tick_file.parent.mkdir(parents=True, exist_ok=True)
        tick_7z_file = Path((str(tick_file.parent) + '.7z').replace('L1_L2/', ''))
        tick_file_in_7z = '/'.join(tick_file.parts[-2:])
        cmd = f'7z x -y {tick_7z_file} {tick_file_in_7z} -o{TICK_DIR.joinpath(*tick_file.parts[-4:-2])} > /dev/null'
        os.system(cmd)

    return tick_file


def extract7z(_7zFile: Path) -> str:
    try:
        print(f'Extracting   {_7zFile}')
        extractDst = EXTRACT_DIR.joinpath(
        _7zFile.relative_to(IN_DIR).parent
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
    for root, dirs, files in os.walk(IN_DIR):
        _7zFiles.extend([
            Path(os.path.join(root, f))
            for f in filter(lambda x: x.endswith('7z'), files)
        ])

    with Pool() as p:
        with open(LOG_DIR.joinpath('extract.log'), 'w') as log:
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


def fixCSV(src_csv_file: Path) -> str:
    try:
        print(f'INFO: Fixing  {src_csv_file}')
        dst_csv_file = OUT_DIR.joinpath(
            src_csv_file.relative_to(EXTRACT_DIR)
        )
        dst_csv_file.parent.mkdir(parents=True, exist_ok=True)
        if dst_csv_file.exists():
            print(f'WARNING: {dst_csv_file} exists, skipping')
            return f'WARNING: {dst_csv_file} exists'

        if 'L1' in str(src_csv_file.parent.name):
            sortingColumns = L1_SORTING_COLUMNS
        else:
            sortingColumns = L2_SORTING_COLUMNS

        df = pd.read_csv(src_csv_file)
        df = df.fillna('')
        df['Volume'] = df['Volume'].astype(int)

        df.loc[df['Volume'] == 0, sortingColumns] = 0

        df[sortingColumns] = df[sortingColumns].sort_values(by=['Volume']).values

        if np.all(each == 0 for each in df['ClosePx'].astype(float).values):
            pass
        else:
            closePxVal = df.loc[df['Volume'].idxmax(), 'ClosePx']
            if int(closePxVal) == 0:
                closePxVal = df['ClosePx'].max()
            else:
                df['ClosePx'] = df['ClosePx'].map(lambda x: x if x == closePxVal else '')

        
        df['BidPrice'] = df['BidPrice'].map(fixPrice)
        df['OfferPrice'] = df['OfferPrice'].map(fixPrice)

        if 'IDX' in str(src_csv_file):
            precisions = IDX_PRECISIONS
        elif 'L1' in str(src_csv_file.parent.name):
            precisions = L1_PRECISIONS
        else:
            precisions = L2_PRECISIONS

        is_sorted  = df['HighPx'].is_monotonic_increasing, df['LowPx'].is_monotonic_decreasing, df['Amount'].is_monotonic_increasing, df['Volume'].is_monotonic_increasing
        if is_sorted[0] and is_sorted[1] and is_sorted[2] and not is_sorted[3]:
            # 只有NumTrades有问题
            print(f'INFO: {src_csv_file} 只有NumTrades有问题')
            df_tick = pd.read_csv(get_tick(src_csv_file))
            df_tick = df_tick[df_tick['ExecType'] == 'F'].fillna('')
            df_tick['cum_amt'] = df_tick['Amt'].cumsum()
            df_tick['cum_qty'] = df_tick['Qty'].cumsum() 
            df_tick['cum_low'] = df_tick['Price'].cummin()
            df_tick['cum_high'] = df_tick['Price'].cummax()

            for idx, row in df.iterrows():
                row['NumTrades'] = df_tick[df_tick['TransactTime'] <= row['QuotTime']].shape[0]
        else:
            df[sortingColumns] = df[sortingColumns].sort_values(by=['NumTrades']).values

        # if not df['HighPx'].is_monotonic_increasing \
        #         or not df['LowPx'].is_monotonic_decreasing \
        #         or not df['Amount'].is_monotonic_increasing \
        #         or not df['NumTrades'].is_monotonic_increasing:
            df_tick = pd.read_csv(get_tick(src_csv_file))
            df_tick = df_tick[df_tick['ExecType'] == 'F'].fillna('')
            df_tick['cum_amt'] = df_tick['Amt'].cumsum()
            df_tick['cum_qty'] = df_tick['Qty'].cumsum() 
            df_tick['cum_low'] = df_tick['Price'].cummin()
            df_tick['cum_high'] = df_tick['Price'].cummax()

            for idx, row in df.iterrows():
                if row['NumTrades'] == 0:
                    df.loc[idx, ['HighPx', 'LowPx', 'Amount', 'Volume']] = 0
                else:
                    df.loc[idx, ['HighPx', 'LowPx', 'Amount', 'Volume']] = df_tick[['cum_high', 'cum_low', 'cum_amt', 'cum_qty']].iloc[row['NumTrades'] - 1].values.tolist()

                # df_tick_before_quotTime =  df_tick.loc[df_tick['TransactTime'] <= row['QuotTime']]
                # df.loc[idx, 'HighPx'] = df_tick_before_quotTime['cum_high'].max()
                # df.loc[idx, 'LowPx'] = df_tick_before_quotTime['cum_low'].min()
                # df.loc[idx, 'Amount'] = df_tick_before_quotTime['cum_amt'].max()
                # df.loc[idx, 'Volume'] = df_tick_before_quotTime['cum_qty'].max()
                # df.loc[idx, 'NumTrades'] = df_tick_before_quotTime['cum_qty'].count()

        print(df)
        for precision, columns in precisions.items():
            if precision != 0:
                df[columns] = df[columns].applymap(lambda x: '' if pd.isna(x) or x == '' else myRound(str(x), precision))
            else:
                df[columns] = df[columns].applymap(lambda x: '' if pd.isna(x) or x == '' else str(int(x)))

        print(df)
        df.to_csv(dst_csv_file, index=False)
        print(f'INFO: Written {dst_csv_file}')
        return f'INFO: {src_csv_file} succeed'
        
    except Exception:
        errorMessage = f'ERROR: {src_csv_file} Failed'
        print(errorMessage)
        traceback.print_exc()
        return errorMessage


def fix():
    csvFiles = []
    for root, dirs, files in os.walk(EXTRACT_DIR):
        if '2015' not in root and '2016' not in root:
            continue
        csvFiles.extend(
            [Path(os.path.join(root, file)) for file in filter(lambda x: x.endswith('csv'), files)]
        )
    
    with Pool() as p:
        list(p.map(fixCSV, csvFiles))


def fix_single_day(date):
    csvFiles = []
    for root, dirs, files in os.walk(EXTRACT_DIR.joinpath(date, 'HFM')):
        if len(files) > 0 and files[0].endswith('csv'):
            csvFiles.extend(
                Path(root, file) for file in files
            )
    
    with Pool(24) as p:
        list(p.map(fixCSV, csvFiles))


def compress7z(_7zFile: Path) -> str:
    try:
        srcDir = FIXED_DIR.joinpath(
            _7zFile.relative_to(IN_DIR)
        )
        srcDir = Path(str(srcDir).split('.')[0])
        print(f'Compressing {srcDir}')

        dstFile = OUT_DIR.joinpath(
            _7zFile.relative_to(IN_DIR)
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
    for root, dirs, files in os.walk(IN_DIR):
        _7zFiles.extend([
            Path(os.path.join(root, f))
            for f in filter(lambda x: x.endswith('7z'), files)
        ])
    
    with open(LOG_DIR.joinpath('compress.log'), 'w') as log:
        for each in _7zFiles:
            print(compress7z(each), file=log, flush=True)

    # with Pool() as p:
    #     with open(logDir.joinpath('compress.log'), 'w') as log:
    #         for each in p.map(compress7z, _7zFiles):
    #             print(each, file=log, flush=True)
        

def extractSingleDay(date: str):
    _7zDir = IN_DIR.joinpath(f'{date}/HFM')
    with Pool() as p:
        print('*' * 100)
        print(p.map(extract7z, _7zDir.iterdir()))


if __name__ == '__main__':
    fixCSV(Path('/home/data/my_test/ytliu/SZ_L1_L2/extract/20151106/HFM/L2_new_STK_SZ_20151106/000001.csv'))
    # fixCSV(Path('/home/data/my_test/ytliu/SZ_L1_L2/extract/20151111/HFM/L1_new_STK_SZ_20151111/000767.csv'))
    # fixCSV(Path('/home/data/my_test/ytliu/SZ_L1_L2/extract/20151106/HFM/L2_new_STK_SZ_20151106/000488.csv'))
    # fixCSV(Path('/home/data/my_test/ytliu/SZ_L1_L2/extract/20151127/HFM/L1_new_STK_SZ_20151127/002336.csv'))
    # fix_single_day('20151106')
    pass
