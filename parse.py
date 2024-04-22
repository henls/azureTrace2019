import pandas as pd
from glob import glob 
import os
from multiprocessing import Pool

save_dir = 'AzureData'

def split2days(file):
    # Read the csv file
    df = pd.read_csv(file)
    vmID = df['machine ID'].unique()
    assert len(vmID) == 1
    vmID = vmID[0]
    vmID = vmID.replace(" ","").replace("/","")
    
    # Split the data into days
    # 从2019年1月一日开始
    start_date = pd.Timestamp('2019-01-01')
    start_date_timestamp = start_date.timestamp()

    df['timestamp'] = df['timestamp'] + start_date_timestamp

    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    df['date'] = df['timestamp'].dt.date
    dates = df['date'].unique()
    for j in range(len(dates)):
        df2 = df[df['date'] == dates[j]]
        date = str(dates[j]).replace("-","")
        save = f'{save_dir}/{date}/{vmID}'
        if len(df2) != 288 or os.path.exists(save):
            continue
        print(f"save to file: {save_dir}/{dates[j]}/{vmID}", end = '\r')
        df2 = df2.drop(columns = ['date'])
        os.makedirs(f'{save_dir}/{date}', exist_ok = True)
        a = df2['avg CPU usage'].astype(int)
        with open(save, 'w') as f:
            for item in a:
                f.write("%s\n" % item)

if __name__ == '__main__':

    files = glob('single_machine/*.csv')

    with Pool(8) as p:
        p.map(split2days, files)
