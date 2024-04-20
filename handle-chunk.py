import pandas as pd
import os
import tracemalloc
from multiprocessing import Pool

selected_columns = ['machine ID', 'timestamp', 'avg CPU usage']
all_data = pd.DataFrame(columns=selected_columns)

with open('machine_id.txt', 'r') as txt_file:
    machine_ids = txt_file.readlines()
machine_ids = [machine_id.strip() for machine_id in machine_ids]

# 构建文件名列表
file_names = ['./data/vm_cpu_readings-file-{}-of-195.csv'.format(i) for i in range(1, 196)]

# 一共时10e7行
total_chunksize = 1e7
chunksize = 10e5 # 读十万行
cpus = 20

# 遍历每个不同的machine，并将数据存储到相应的CSV文件中
def process_machine_id(args):
    i, machine_id = args
    chunks = []
    os.makedirs('single_machine', exist_ok=True)
    file_name = f"./single_machine/{i}.csv"
    if os.path.exists(file_name):
        print(f"File {file_name} already exists, skipping...")
        return
    print(f"Processing machine ID: {machine_id}")

    # 加载这台机器的全部数据
    # 遍历文件名列表
    for filename in file_names:
        
        # Start tracing memory allocations
        tracemalloc.start()

        print(f"Processing file: {filename}")
        
        # # 读取CSV文件时，只读取指定机器ID范围的数据
        for chunk in pd.read_csv(filename, header=None, usecols=[0, 1, 4], chunksize=chunksize):
            chunk.rename(columns={0: 'timestamp', 1: 'machine ID', 4: 'avg CPU usage'}, inplace=True)
            chunk = chunk[chunk['machine ID'] == machine_id]
            if not chunk.empty:
                chunks.append(chunk)
            del chunk

        # Get the current memory usage
        current, peak = tracemalloc.get_traced_memory()
        print(f"Current memory usage: {current / 1024 / 1024}MB")
        print(f"Peak memory usage: {peak / 1024 / 1024}MB")

        # Stop tracing memory allocations
        tracemalloc.stop()

    all_data = pd.concat(chunks, axis=0, ignore_index=True)

    machine_data = all_data
    machine_data = machine_data.sort_values(by='timestamp')
    machine_data.to_csv(file_name, index=False)

if __name__ == "__main__":
    with Pool(cpus) as p:
        p.map(process_machine_id, enumerate(machine_ids, start=1))