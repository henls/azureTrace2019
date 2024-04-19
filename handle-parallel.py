import pandas as pd
import os
from multiprocessing import Pool

# ps aux | grep parallel | cut -c 9-16 | xargs kill -9

selected_columns = ['machine ID', 'timestamp', 'avg CPU usage']
all_data = pd.DataFrame(columns=selected_columns)

cpus = 40

with open('machine_id.txt', 'r') as txt_file:
    machine_ids = txt_file.readlines()
machine_ids = [machine_id.strip() for machine_id in machine_ids]

file_names = ['./data/vm_cpu_readings-file-{}-of-195.csv'.format(i) for i in range(1, 196)]

def process_file_with_filter(filename):
    print(f"{os.getpid()}: Processing file: {filename}")
    
    df = pd.read_csv(filename, header=None, usecols=[0, 1, 4])
    df.rename(columns={0: 'timestamp', 1: 'machine ID', 4: 'avg CPU usage'}, inplace=True)

    b = df[df['machine ID'].isin(machine_ids)]

    del df

    return b

with Pool(cpus) as p:
    dfs = p.map(process_file_with_filter, file_names)

print("Concatenating data")
all_data = pd.concat(dfs, ignore_index=True)

all_data.to_csv('all_data.csv', index=False)

# all_data = pd.read_csv('all_data.csv')

# with open('machine_id.txt', 'w') as txt_file:
#     for i, machine_id in enumerate(machine_ids, start=1):
#         txt_file.write(f"{i}\n")

# def process_machine_id(machine_id):
#     if os.path.exists(f"./single_machine/{machine_id}.csv"):
#         print(f"Machine ID {machine_id} already processed")
#         return
#     print(f"{os.getpid()}: Processing machine ID: {machine_id}")
#     machine_data = all_data[all_data['machine ID'] == machine_id]
#     machine_data = machine_data.sort_values(by='timestamp')
#     os.makedirs('single_machine', exist_ok=True)
#     file_name = f"./single_machine/{i}.csv"
#     machine_data.to_csv(file_name, index=False)


# with Pool(cpus) as p:
#     p.map(process_machine_id, machine_ids)