import os

directory_path = '/Users/kevin/test/ray/doccache'
destination_path = "/Users/kevin/test/ray/"
for root, _, files in os.walk(directory_path):
    for file in files:
        full_path = os.path.join(root, file)
        print(full_path.replace(directory_path + "/", ""))
        dest_path = os.path.join(destination_path, "doc", full_path.replace(directory_path + "/", ""))
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        os.rename(full_path, dest_path)
        print(dest_path)