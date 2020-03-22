def get_arr_partition(arr, n, i):
    per_part = len(arr) // n
    extra = len(arr) % n
    if i < extra:
        start = (per_part + 1) * i
        return arr[start: start + per_part + 1]
    else:
        start = (per_part + 1) * extra + per_part * (i - extra)
        return arr[start : start + per_part]

