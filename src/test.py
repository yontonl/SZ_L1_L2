from pathlib import Path
from main import *

import krtools


if __name__ == '__main__':
    # dates = ['20130308', '20150506']
    # for date in dates:
    #     extractSingleDay(date)
    
    # fix()
    # print(krtools.getRound('0',3))

    print(fixCSV(Path('/home/data/my_test/ytliu/SZ_L1_L2/extract/20130308/HFM/L2_new_STK_SZ_20130308/002409.csv')))