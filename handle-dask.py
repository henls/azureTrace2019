import pandas as pd
import dask.dataframe as dd
import os

selected_columns = ['machine ID', 'timestamp', 'avg CPU usage']
all_data = dd.from_pandas(pd.DataFrame(columns=selected_columns), npartitions=10)


# machine_ids = all_data['machine ID'].unique().compute()
# machine_ids = machine_ids[:20000]
with open('machine_id.txt', 'r') as txt_file:
    machine_ids = txt_file.readlines()
machine_ids = [machine_id.strip() for machine_id in machine_ids]

file_names = ['./data/vm_cpu_readings-file-{}-of-195.csv'.format(i) for i in range(1, 196)]

for idx, filename in enumerate(file_names):
    print(f"Processing file: {filename}")
    df = dd.read_csv(filename, header=None)
    df = df.rename(columns={0: 'timestamp', 1: 'machine ID', 4: 'avg CPU usage'})
    df = df.drop([2, 3], axis=1)
    df = df[df['machine ID'].isin(machine_ids)]
    all_data = dd.concat([all_data, df], ignore_index=True)
    if idx % 20 == 0:
        all_data = all_data.compute()
        all_data = dd.from_pandas(all_data, npartitions=10)

with open('machine_id.txt', 'w') as txt_file:
    for i, machine_id in enumerate(machine_ids, start=1):
        txt_file.write(f"{machine_id}\n")

for i, machine_id in enumerate(machine_ids, start=1):
    os.makedirs('single_machine', exist_ok=True)
    file_name = f"./single_machine/{i}.csv"
    if os.path.exists(file_name):
        print(f"File {file_name} already exists, skipping...")
        continue
    print(f"Processing machine ID: {machine_id}")
    machine_data = all_data[all_data['machine ID'] == machine_id]
    machine_data = machine_data.sort_values(by='timestamp')
    machine_data.to_csv(file_name, index=False)
    all_data = all_data[all_data['machine ID'] != machine_id]