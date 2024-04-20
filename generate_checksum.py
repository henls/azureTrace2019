import hashlib
import glob
import json

def generate_checksum(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path,"rb") as f:
        # Read and update hash in chunks of 4K
        for byte_block in iter(lambda: f.read(4096),b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

pth = r'data'
flists = glob.glob(pth + '/*.csv')

dict_checksum = {}

for file in flists:
    print(f"Generating checksum for {file}")
    dict_checksum[file] = generate_checksum(file)

# 保存为json
with open('checksums.json', 'w') as f:
    json.dump(dict_checksum, f, indent=4)