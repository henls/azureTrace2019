import pandas as pd
import os
import tracemalloc
import time
from concurrent.futures import ProcessPoolExecutor

selected_columns = ['machine ID', 'timestamp', 'avg CPU usage']
all_data = pd.DataFrame(columns=selected_columns)

with open('machine_id.txt', 'r') as txt_file:
    machine_ids = txt_file.readlines()
machine_ids = [machine_id.strip() for machine_id in machine_ids]

# 构建文件名列表
file_names = ['./data/vm_cpu_readings-file-{}-of-195.csv'.format(i) for i in range(1, 196)]

# 一共时10e7行
total_chunksize = 1e7
chunksize = 1e6 # 读十万行
cpus = 40

def process_file(filename):
    global start, end
    chunks = []
    filename = filename
    start, end = start, end
    print(f"Processing file: {filename}")

    # Start tracing memory allocations
    tracemalloc.start()



    for chunk in pd.read_csv(filename, header=None, usecols=[0, 1, 4], chunksize=chunksize):
        chunk.rename(columns={0: 'timestamp', 1: 'machine ID', 4: 'avg CPU usage'}, inplace=True)

        filtered_chunk = chunk[chunk['machine ID'].isin([machine_ids[k] for k in range(start,end)])]
        if not filtered_chunk.empty:
            chunks.append(filtered_chunk)
        del chunk

    chunks = pd.concat(chunks, axis=0, ignore_index=True)

    # Get the current memory usage
    current, peak = tracemalloc.get_traced_memory()
    print(f"Current memory usage: {current / 1024 / 1024}MB")
    print(f"Peak memory usage: {peak / 1024 / 1024}MB")

    # Stop tracing memory allocations
    tracemalloc.stop()

    return chunks

if __name__ == "__main__":
    machine_size = 400
    machine_total = 20000
    max_proceed_file_id = 1201
    for chunk_id in range(machine_total // machine_size):
        start_time = time.time()
        start, end = chunk_id * machine_size, (chunk_id + 1) * machine_size
        if end < max_proceed_file_id:
            continue
        start = max(start, max_proceed_file_id)
        end = max(end, max_proceed_file_id)
        print(f"{start} to {end}")
        with ProcessPoolExecutor(max_workers=cpus) as executor:
            results = executor.map(process_file, file_names)

        all_data = pd.concat(results, axis=0, ignore_index=True)

        for i, machine_id in enumerate([machine_ids[k] for k in range(start,end)], start = 1):
            os.makedirs('single_machine', exist_ok=True)
            file_name = f"./single_machine/{i + start}.csv"
            if os.path.exists(file_name):
                print(f"File {file_name} already exists, skipping...")
                continue
            print(f"Processing machine ID: {machine_id}")
            machine_data = all_data[all_data['machine ID'] == machine_id]
            machine_data = machine_data.sort_values(by='timestamp')
            machine_data.to_csv(file_name, index=False)
            all_data = all_data[all_data['machine ID'] != machine_id]
        print(f"elapse: {time.time() - start_time}")