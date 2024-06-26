import pandas as pd
import os

selected_columns = ['machine ID', 'timestamp', 'avg CPU usage']
all_data = pd.DataFrame(columns=selected_columns)

first_file_names = ['./vm_cpu_readings-file-{}-of-125.csv'.format(i) for i in range(1, 20)]

for filename in first_file_names:
    print(f"Processing file: {filename}")
    
    # 读取CSV文件时，只读取指定机器ID范围的数据
    df = pd.read_csv(filename)

    # 将处理后的数据追加到总数据中
    all_data = pd.concat([all_data, df], ignore_index=True)
    
    # 释放内存
    del df

# 选择前 10000 个机器的数据
machine_ids = all_data['machine ID'].unique()[:20000]

# 构建文件名列表
file_names = ['./vm_cpu_readings-file-{}-of-125.csv'.format(i) for i in range(21, 126)]

# 遍历文件名列表
for filename in file_names:
    print(f"Processing file: {filename}")
    
    # 读取CSV文件时，只读取指定机器ID范围的数据
    df = pd.read_csv(filename)

    df = df[df['machine ID'].isin(machine_ids)]

    # 将处理后的数据追加到总数据中
    all_data = pd.concat([all_data, df], ignore_index=True)
    
    # 释放内存
    del df

# 存储每个machine的数据到txt文件
with open('machine_id.txt', 'w') as txt_file:
    for i, machine_id in enumerate(machine_ids, start=1):
        txt_file.write(f"{i}\n")

# 遍历每个不同的machine，并将数据存储到相应的CSV文件中
for i, machine_id in enumerate(machine_ids, start=1):
    print(f"Processing machine ID: {machine_id}")
    machine_data = all_data[all_data['machine ID'] == machine_id]
    machine_data = machine_data.sort_values(by='timestamp')
    file_name = f"../single_machine/{i}.csv"
    machine_data.to_csv(file_name, index=False)
    all_data = all_data[all_data['machine ID'] != machine_id]