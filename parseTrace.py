import pandas as pd
import os
from glob import glob

# 处理Azure cluster数据集

data_dir = "./data"
f_lists = glob(os.path.join(data_dir, "vm_cpu*.csv"))

def process_azure_trace_data(file_path):
    # Read the data from the CSV file
    df = pd.read_csv(file_path)
    
    # Assuming the CSV has columns 'vmid', 'cpu_util', and 'timestamp'
    # Group the data by 'vmid'
    grouped = df.groupby('vmid')
    
    # Create a directory to save the files
    os.makedirs('vm_data', exist_ok=True)
    
    # Process each group and save to a file
    for vmid, group in grouped:
        # Sort the group by timestamp
        group = group.sort_values('timestamp')
        
        # Set the timestamp as the index
        group.set_index('timestamp', inplace=True)
        
        # Drop the 'vmid' column as it's redundant in the individual file
        group.drop('vmid', axis=1, inplace=True)
        
        # Save the processed data to a file named after the vmid
        group.to_csv(f'vm_data/{vmid}.csv')
        
        # Print a success message for each VM
        print(f"Processed time series data for VM {vmid} saved to vm_data/{vmid}.csv")
