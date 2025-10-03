path = "bigfile.bin"
chunk_size = 10 * 1024 * 1024  # 10 MiB block
chunk = b"\0" * chunk_size
written = 0

print(f"Writing to {path} ...")
with open(path, "wb") as f:
    while True:
        f.write(chunk)  # will raise OSError(ENOSPC) when space runs out
        written += chunk_size
        if written % (100 * chunk_size) == 0:
            print(f"Written {written / chunk_size} chunks of 10 MiB!")
