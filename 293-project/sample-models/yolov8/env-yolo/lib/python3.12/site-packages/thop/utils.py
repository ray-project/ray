from collections.abc import Iterable

COLOR_RED = "91m"
COLOR_GREEN = "92m"
COLOR_YELLOW = "93m"


def colorful_print(fn_print, color=COLOR_RED):
    """A decorator to print text in the specified terminal color by wrapping the given print function."""

    def actual_call(*args, **kwargs):
        print(f"\033[{color}", end="")
        fn_print(*args, **kwargs)
        print("\033[00m", end="")

    return actual_call


prRed = colorful_print(print, color=COLOR_RED)
prGreen = colorful_print(print, color=COLOR_GREEN)
prYellow = colorful_print(print, color=COLOR_YELLOW)


def clever_format(nums, format="%.2f"):
    """Formats numbers into human-readable strings with units (K for thousand, M for million, etc.)."""
    if not isinstance(nums, Iterable):
        nums = [nums]
    clever_nums = []

    for num in nums:
        if num > 1e12:
            clever_nums.append(format % (num / 1e12) + "T")
        elif num > 1e9:
            clever_nums.append(format % (num / 1e9) + "G")
        elif num > 1e6:
            clever_nums.append(format % (num / 1e6) + "M")
        elif num > 1e3:
            clever_nums.append(format % (num / 1e3) + "K")
        else:
            clever_nums.append(format % num + "B")

    return clever_nums[0] if len(clever_nums) == 1 else (*clever_nums,)


if __name__ == "__main__":
    prRed("hello", "world")
    prGreen("hello", "world")
    prYellow("hello", "world")
