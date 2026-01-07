import random
import string
import ray

def random_text(length: int) -> str:
    """Generate random text of specified length."""
    if length <= 0:
        return ""

    if length <= 3:
        return "".join(random.choices(string.ascii_lowercase, k=length))

    words = []
    current_length = 0

    while current_length < length:
        remaining = length - current_length
        
        if remaining <= 4:
            word_length = remaining
            word = "".join(random.choices(string.ascii_lowercase, k=word_length))
            words.append(word)
            break
        else:
            max_word_length = min(10, remaining - 1)
            if max_word_length >= 3:
                word_length = random.randint(3, max_word_length)
            else:
                word_length = remaining
            word = "".join(random.choices(string.ascii_lowercase, k=word_length))
            words.append(word)
            current_length += len(word) + 1

    text = " ".join(words)
    return text[:length]

def random_label() -> int:
    """Pick a random label."""
    labels = [0, 1, 2, 3, 4, 5, 6, 7]
    return random.choice(labels)

def create_mock_ray_text_dataset(dataset_size: int = 96, min_len: int = 5, max_len: int = 100):
    """Create a mock Ray dataset with random text and labels."""
    numbers = random.choices(range(min_len, max_len + 1), k=dataset_size)
    ray_dataset = ray.data.from_items(numbers)

    def map_to_text_and_label(item):
        length = item['item']
        text = random_text(length)
        label = random_label()
        return {
            "length": length,
            "text": text,
            "label": label
        }

    text_dataset = ray_dataset.map(map_to_text_and_label)
    return text_dataset