"""Collection of Carla scenarios, including those from the CoRL 2017 paper."""

TEST_WEATHERS = [0, 2, 5, 7, 9, 10, 11, 12, 13]
TRAIN_WEATHERS = [1, 3, 4, 6, 8, 14]


def build_scenario(city, start, end, vehicles, pedestrians, max_steps,
                   weathers):
    return {
        "city": city,
        "num_vehicles": vehicles,
        "num_pedestrians": pedestrians,
        "weather_distribution": weathers,
        "start_pos_id": start,
        "end_pos_id": end,
        "max_steps": max_steps,
    }


# Simple scenario for Town02 that involves driving down a road
DEFAULT_SCENARIO = build_scenario(
    city="Town02",
    start=36,
    end=40,
    vehicles=20,
    pedestrians=40,
    max_steps=200,
    weathers=[0])

# Simple scenario for Town02 that involves driving down a road
LANE_KEEP = build_scenario(
    city="Town02",
    start=36,
    end=40,
    vehicles=0,
    pedestrians=0,
    max_steps=2000,
    weathers=[0])

# Scenarios from the CoRL2017 paper
POSES_TOWN1_STRAIGHT = [[36, 40], [39, 35], [110, 114], [7, 3], [0, 4], [
    68, 50
], [61, 59], [47, 64], [147, 90], [33, 87], [26, 19], [80, 76], [45, 49], [
    55, 44
], [29, 107], [95, 104], [84, 34], [53, 67], [22, 17], [91, 148], [20, 107],
                        [78, 70], [95, 102], [68, 44], [45, 69]]

POSES_TOWN1_ONE_CURVE = [[138, 17], [47, 16], [26, 9], [42, 49], [140, 124], [
    85, 98
], [65, 133], [137, 51], [76, 66], [46, 39], [40, 60], [0, 29], [4, 129], [
    121, 140
], [2, 129], [78, 44], [68, 85], [41, 102], [95, 70], [68, 129], [84, 69],
                         [47, 79], [110, 15], [130, 17], [0, 17]]

POSES_TOWN1_NAV = [[105, 29], [27, 130], [102, 87], [132, 27], [24, 44], [
    96, 26
], [34, 67], [28, 1], [140, 134], [105, 9], [148, 129], [65, 18], [21, 16], [
    147, 97
], [42, 51], [30, 41], [18, 107], [69, 45], [102, 95], [18, 145], [111, 64],
                   [79, 45], [84, 69], [73, 31], [37, 81]]

POSES_TOWN2_STRAIGHT = [[38, 34], [4, 2], [12, 10], [62, 55], [43, 47], [
    64, 66
], [78, 76], [59, 57], [61, 18], [35, 39], [12, 8], [0, 18], [75, 68], [
    54, 60
], [45, 49], [46, 42], [53, 46], [80, 29], [65, 63], [0, 81], [54, 63],
                        [51, 42], [16, 19], [17, 26], [77, 68]]

POSES_TOWN2_ONE_CURVE = [[37, 76], [8, 24], [60, 69], [38, 10], [21, 1], [
    58, 71
], [74, 32], [44, 0], [71, 16], [14, 24], [34, 11], [43, 14], [75, 16], [
    80, 21
], [3, 23], [75, 59], [50, 47], [11, 19], [77, 34], [79, 25], [40, 63],
                         [58, 76], [79, 55], [16, 61], [27, 11]]

POSES_TOWN2_NAV = [[19, 66], [79, 14], [19, 57], [23, 1], [53, 76], [42, 13], [
    31, 71
], [33, 5], [54, 30], [10, 61], [66, 3], [27, 12], [79, 19], [2, 29], [16, 14],
                   [5, 57], [70, 73], [46, 67], [57, 50], [61, 49], [21, 12],
                   [51, 81], [77, 68], [56, 65], [43, 54]]

TOWN1_STRAIGHT = [
    build_scenario("Town01", start, end, 0, 0, 300, TEST_WEATHERS)
    for (start, end) in POSES_TOWN1_STRAIGHT
]

TOWN1_ONE_CURVE = [
    build_scenario("Town01", start, end, 0, 0, 600, TEST_WEATHERS)
    for (start, end) in POSES_TOWN1_ONE_CURVE
]

TOWN1_NAVIGATION = [
    build_scenario("Town01", start, end, 0, 0, 900, TEST_WEATHERS)
    for (start, end) in POSES_TOWN1_NAV
]

TOWN1_NAVIGATION_DYNAMIC = [
    build_scenario("Town01", start, end, 20, 50, 900, TEST_WEATHERS)
    for (start, end) in POSES_TOWN1_NAV
]

TOWN2_STRAIGHT = [
    build_scenario("Town02", start, end, 0, 0, 300, TRAIN_WEATHERS)
    for (start, end) in POSES_TOWN2_STRAIGHT
]

TOWN2_STRAIGHT_DYNAMIC = [
    build_scenario("Town02", start, end, 20, 50, 300, TRAIN_WEATHERS)
    for (start, end) in POSES_TOWN2_STRAIGHT
]

TOWN2_ONE_CURVE = [
    build_scenario("Town02", start, end, 0, 0, 600, TRAIN_WEATHERS)
    for (start, end) in POSES_TOWN2_ONE_CURVE
]

TOWN2_NAVIGATION = [
    build_scenario("Town02", start, end, 0, 0, 900, TRAIN_WEATHERS)
    for (start, end) in POSES_TOWN2_NAV
]

TOWN2_NAVIGATION_DYNAMIC = [
    build_scenario("Town02", start, end, 20, 50, 900, TRAIN_WEATHERS)
    for (start, end) in POSES_TOWN2_NAV
]

TOWN1_ALL = (TOWN1_STRAIGHT + TOWN1_ONE_CURVE + TOWN1_NAVIGATION +
             TOWN1_NAVIGATION_DYNAMIC)

TOWN2_ALL = (TOWN2_STRAIGHT + TOWN2_ONE_CURVE + TOWN2_NAVIGATION +
             TOWN2_NAVIGATION_DYNAMIC)
