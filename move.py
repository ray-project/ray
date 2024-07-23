import os

directory_path = '/Users/kevin/ray/doccache'
destination_path = "/Users/kevin/ray/"
for root, _, files in os.walk(directory_path):
    for file in files:
        full_path = os.path.join(root, file)
        print(full_path.replace(directory_path + "/", ""))
        dest_path = os.path.join(destination_path, full_path.replace(directory_path + "/", ""))
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        if "Algorithm.save_to_path" in full_path:
            os.rename(full_path, dest_path)