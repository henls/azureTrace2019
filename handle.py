import pandas as pd
import os
import psutil

selected_columns = ['machine ID', 'timestamp', 'avg CPU usage']
all_data = pd.DataFrame(columns=selected_columns)


# first_file_names = ['./data/vm_cpu_readings-file-{}-of-195.csv'.format(i) for i in range(1, 21)]

# for filename in first_file_names:
#     print(f"Processing file: {filename}")
    
#     # 读取CSV文件时，只读取指定机器ID范围的数据
#     df = pd.read_csv(filename, header=None)

#     df.rename(columns={0: 'timestamp', 1: 'machine ID', 4: 'avg CPU usage'}, inplace=True)

#     df.drop([2, 3], axis=1, inplace=True)
#     # 将处理后的数据追加到总数据中
#     all_data = pd.concat([all_data, df], ignore_index=True)
    
#     # 释放内存
#     del df

# # 选择前 10000 个机器的数据
# machine_ids = all_data['machine ID'].unique()[:20000]

# # 存储每个machine的数据到txt文件
# with open('machine_id.txt', 'w') as txt_file:
#     for i, machine_id in enumerate(machine_ids, start=1):
#         txt_file.write(f"{machine_id}\n")

with open('machine_id.txt', 'r') as txt_file:
    machine_ids = txt_file.readlines()
machine_ids = [machine_id.strip() for machine_id in machine_ids]

# 构建文件名列表
file_names = ['./data/vm_cpu_readings-file-{}-of-195.csv'.format(i) for i in range(1, 196)]



# 遍历文件名列表
for filename in file_names:
    mem = psutil.virtual_memory()
    ysy = float(mem.used) / 1024 / 1024 / 1024
    print(f"Processing file: {filename}")
    
    # 读取CSV文件时，只读取指定机器ID范围的数据
    df = pd.read_csv(filename, header=None, usecols=[0, 1, 4])

    df.rename(columns={0: 'timestamp', 1: 'machine ID', 4: 'avg CPU usage'}, inplace=True)

    df = df[df['machine ID'].isin(machine_ids)]

    # 将处理后的数据追加到总数据中
    all_data = pd.concat([all_data, df], ignore_index=True)
    
    # 释放内存
    del df

    mem = psutil.virtual_memory()
    tmp_mem = float(mem.used) / 1024 / 1024 / 1024

    print(f"Memory used: {tmp_mem} incremental :{tmp_mem - ysy}")

# 遍历每个不同的machine，并将数据存储到相应的CSV文件中
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