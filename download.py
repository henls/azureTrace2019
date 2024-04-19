import os

def download(link):
    link = link.replace('\n','')
    result = os.system(f"wget --timeout=30 {link} -P ./data")
    if result != 0:
        return False
    return True

pth = r'AzurePublicDatasetLinksV2.txt'

with open(pth, "r") as f:
    links = f.readlines()

unfinished_links = []
for link in links:
    if 'ipynb' in link:
        continue
    if not download(link):
        unfinished_links.append(link)

print("Unfinished downloads:")
for link in unfinished_links:
    print(link)