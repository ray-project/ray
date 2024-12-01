import itertools

def generate_permutations(n, k):
    numbers = range(1, n + 1)
    return list(itertools.permutations(numbers, k))

n = 5  # Total numbers to choose from
k = 3  # Number of items to choose

permutations = generate_permutations(n, k)
for perm in permutations:
    print(perm)
    print(type(perm))