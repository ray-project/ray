"""Implementation of the CART algorithm to train decision tree classifiers."""
import numpy as np
import ray
from sklearn import datasets, metrics
import time
import tempfile
import os
import json

"""Binary tree with decision tree semantics and ASCII visualization."""


class Node:
    """A decision tree node."""

    def __init__(self, gini, num_samples, num_samples_per_class, predicted_class):
        self.gini = gini
        self.num_samples = num_samples
        self.num_samples_per_class = num_samples_per_class
        self.predicted_class = predicted_class
        self.feature_index = 0
        self.threshold = 0
        self.left = None
        self.right = None

    def debug(self, feature_names, class_names, show_details):
        """Print an ASCII visualization of the tree."""
        lines, _, _, _ = self._debug_aux(
            feature_names, class_names, show_details, root=True
        )
        for line in lines:
            print(line)

    def _debug_aux(self, feature_names, class_names, show_details, root=False):
        # See https://stackoverflow.com/a/54074933/1143396 for similar code.
        is_leaf = not self.right
        if is_leaf:
            lines = [class_names[self.predicted_class]]
        else:
            lines = [
                "{} < {:.2f}".format(feature_names[self.feature_index], self.threshold)
            ]
        if show_details:
            lines += [
                "gini = {:.2f}".format(self.gini),
                "samples = {}".format(self.num_samples),
                str(self.num_samples_per_class),
            ]
        width = max(len(line) for line in lines)
        height = len(lines)
        if is_leaf:
            lines = ["║ {:^{width}} ║".format(line, width=width) for line in lines]
            lines.insert(0, "╔" + "═" * (width + 2) + "╗")
            lines.append("╚" + "═" * (width + 2) + "╝")
        else:
            lines = ["│ {:^{width}} │".format(line, width=width) for line in lines]
            lines.insert(0, "┌" + "─" * (width + 2) + "┐")
            lines.append("└" + "─" * (width + 2) + "┘")
            lines[-2] = "┤" + lines[-2][1:-1] + "├"
        width += 4  # for padding

        if is_leaf:
            middle = width // 2
            lines[0] = lines[0][:middle] + "╧" + lines[0][middle + 1 :]
            return lines, width, height, middle

        # If not a leaf, must have two children.
        left, n, p, x = self.left._debug_aux(feature_names, class_names, show_details)
        right, m, q, y = self.right._debug_aux(feature_names, class_names, show_details)
        top_lines = [n * " " + line + m * " " for line in lines[:-2]]
        # fmt: off
        middle_line = x * " " + "┌" + (
            n - x - 1) * "─" + lines[-2] + y * "─" + "┐" + (m - y - 1) * " "
        bottom_line = x * " " + "│" + (
            n - x - 1) * " " + lines[-1] + y * " " + "│" + (m - y - 1) * " "
        # fmt: on
        if p < q:
            left += [n * " "] * (q - p)
        elif q < p:
            right += [m * " "] * (p - q)
        zipped_lines = zip(left, right)
        lines = (
            top_lines
            + [middle_line, bottom_line]
            + [a + width * " " + b for a, b in zipped_lines]
        )
        middle = n + width // 2
        if not root:
            lines[0] = lines[0][:middle] + "┴" + lines[0][middle + 1 :]
        return lines, n + m + width, max(p, q) + 2 + len(top_lines), middle


class DecisionTreeClassifier:
    def __init__(self, max_depth=None, tree_limit=5000, feature_limit=2000):
        self.max_depth = max_depth
        self.tree_limit = tree_limit
        self.feature_limit = feature_limit

    def fit(self, X, y):
        """Build decision tree classifier."""
        self.n_classes_ = len(set(y))  # classes are assumed to go from 0 to n-1
        self.n_features_ = X.shape[1]
        self.tree_ = self._grow_tree(X, y)

    def predict(self, X):
        """Predict class for X."""
        return [self._predict(inputs) for inputs in X]

    def debug(self, feature_names, class_names, show_details=True):
        """Print ASCII visualization of decision tree."""
        self.tree_.debug(feature_names, class_names, show_details)

    def _gini(self, y):
        """Compute Gini impurity of a non-empty node.

        Gini impurity is defined as Σ p(1-p) over all classes, with p the freq
        class within the node. Since Σ p = 1, this is equivalent to 1 - Σ p^2.
        """
        m = y.size
        return 1.0 - sum((np.sum(y == c) / m) ** 2 for c in range(self.n_classes_))

    def _best_split(self, X, y):
        return best_split(self, X, y)

    def _grow_tree(self, X, y, depth=0):
        future = grow_tree_remote.remote(self, X, y, depth)
        return ray.get(future)

    def _predict(self, inputs):
        """Predict class for a single sample."""
        node = self.tree_
        while node.left:
            if inputs[node.feature_index] < node.threshold:
                node = node.left
            else:
                node = node.right
        return node.predicted_class


def grow_tree_local(tree, X, y, depth):
    """Build a decision tree by recursively finding the best split."""
    # Population for each class in current node. The predicted class is the one
    # largest population.
    num_samples_per_class = [np.sum(y == i) for i in range(tree.n_classes_)]
    predicted_class = np.argmax(num_samples_per_class)
    node = Node(
        gini=tree._gini(y),
        num_samples=y.size,
        num_samples_per_class=num_samples_per_class,
        predicted_class=predicted_class,
    )

    # Split recursively until maximum depth is reached.
    if depth < tree.max_depth:
        idx, thr = tree._best_split(X, y)
        if idx is not None:
            indices_left = X[:, idx] < thr
            X_left, y_left = X[indices_left], y[indices_left]
            X_right, y_right = X[~indices_left], y[~indices_left]
            node.feature_index = idx
            node.threshold = thr
            node.left = grow_tree_local(tree, X_left, y_left, depth + 1)
            node.right = grow_tree_local(tree, X_right, y_right, depth + 1)
    return node


@ray.remote
def grow_tree_remote(tree, X, y, depth=0):
    """Build a decision tree by recursively finding the best split."""
    # Population for each class in current node. The predicted class is the one
    # largest population.
    num_samples_per_class = [np.sum(y == i) for i in range(tree.n_classes_)]
    predicted_class = np.argmax(num_samples_per_class)
    node = Node(
        gini=tree._gini(y),
        num_samples=y.size,
        num_samples_per_class=num_samples_per_class,
        predicted_class=predicted_class,
    )

    # Split recursively until maximum depth is reached.
    if depth < tree.max_depth:
        idx, thr = tree._best_split(X, y)
        if idx is not None:
            indices_left = X[:, idx] < thr
            X_left, y_left = X[indices_left], y[indices_left]
            X_right, y_right = X[~indices_left], y[~indices_left]
            node.feature_index = idx
            node.threshold = thr
            if len(X_left) > tree.tree_limit or len(X_right) > tree.tree_limit:
                left_future = grow_tree_remote.remote(tree, X_left, y_left, depth + 1)
                right_future = grow_tree_remote.remote(
                    tree, X_right, y_right, depth + 1
                )
                node.left = ray.get(left_future)
                node.right = ray.get(right_future)
            else:
                node.left = grow_tree_local(tree, X_left, y_left, depth + 1)
                node.right = grow_tree_local(tree, X_right, y_right, depth + 1)
    return node


def best_split_original(tree, X, y):
    """Find the best split for a node."""
    # Need at least two elements to split a node.
    m = y.size
    if m <= 1:
        return None, None

    # Count of each class in the current node.
    num_parent = [np.sum(y == c) for c in range(tree.n_classes_)]

    # Gini of current node.
    best_gini = 1.0 - sum((n / m) ** 2 for n in num_parent)
    best_idx, best_thr = None, None

    # Loop through all features.
    for idx in range(tree.n_features_):
        # Sort data along selected feature.
        thresholds, classes = zip(*sorted(zip(X[:, idx], y)))
        # print("Classes are: ", classes, " ", thresholds)
        # We could actually split the node according to each feature/threshold
        # and count the resulting population for each class in the children,
        # instead we compute them in an iterative fashion, making this for loop
        # linear rather than quadratic.
        num_left = [0] * tree.n_classes_
        num_right = num_parent.copy()
        for i in range(1, m):  # possible split positions
            c = classes[i - 1]
            # print("c is ", c, "num left is", len(num_left))
            num_left[c] += 1
            num_right[c] -= 1
            gini_left = 1.0 - sum(
                (num_left[x] / i) ** 2 for x in range(tree.n_classes_)
            )
            gini_right = 1.0 - sum(
                (num_right[x] / (m - i)) ** 2 for x in range(tree.n_classes_)
            )

            # The Gini impurity of a split is the weighted average of the Gini
            # impurity of the children.
            gini = (i * gini_left + (m - i) * gini_right) / m

            # The following condition is to make sure we don't try to split two
            # points with identical values for that feature, as it is impossibl
            # (both have to end up on the same side of a split).
            if thresholds[i] == thresholds[i - 1]:
                continue

            if gini < best_gini:
                best_gini = gini
                best_idx = idx
                best_thr = (thresholds[i] + thresholds[i - 1]) / 2  # midpoint

    return best_idx, best_thr


def best_split_for_idx(tree, idx, X, y, num_parent, best_gini):
    """Find the best split for a node and a given index"""
    # Sort data along selected feature.
    thresholds, classes = zip(*sorted(zip(X[:, idx], y)))
    # print("Classes are: ", classes, " ", thresholds)
    # We could actually split the node according to each feature/threshold pair
    # and count the resulting population for each class in the children, but
    # instead we compute them in an iterative fashion, making this for loop
    # linear rather than quadratic.
    m = y.size
    num_left = [0] * tree.n_classes_
    num_right = num_parent.copy()
    best_thr = float("NaN")
    for i in range(1, m):  # possible split positions
        c = classes[i - 1]
        # print("c is ", c, "num left is", len(num_left))
        num_left[c] += 1
        num_right[c] -= 1
        gini_left = 1.0 - sum((num_left[x] / i) ** 2 for x in range(tree.n_classes_))
        gini_right = 1.0 - sum(
            (num_right[x] / (m - i)) ** 2 for x in range(tree.n_classes_)
        )

        # The Gini impurity of a split is the weighted average of the Gini
        # impurity of the children.
        gini = (i * gini_left + (m - i) * gini_right) / m

        # The following condition is to make sure we don't try to split two
        # points with identical values for that feature, as it is impossible
        # (both have to end up on the same side of a split).
        if thresholds[i] == thresholds[i - 1]:
            continue

        if gini < best_gini:
            best_gini = gini
            best_thr = (thresholds[i] + thresholds[i - 1]) / 2  # midpoint

    return best_gini, best_thr


@ray.remote
def best_split_for_idx_remote(tree, idx, X, y, num_parent, best_gini):
    return best_split_for_idx(tree, idx, X, y, num_parent, best_gini)


def best_split(tree, X, y):
    """Find the best split for a node."""
    # Need at least two elements to split a node.
    m = y.size
    if m <= 1:
        return None, None
    # Count of each class in the current node.
    num_parent = [np.sum(y == c) for c in range(tree.n_classes_)]

    # Gini of current node.
    best_gini = 1.0 - sum((n / m) ** 2 for n in num_parent)
    best_idx, best_thr = -1, best_gini
    if m > tree.feature_limit:
        split_futures = [
            best_split_for_idx_remote.remote(tree, i, X, y, num_parent, best_gini)
            for i in range(tree.n_features_)
        ]
        best_splits = [ray.get(result) for result in split_futures]
    else:
        best_splits = [
            best_split_for_idx(tree, i, X, y, num_parent, best_gini)
            for i in range(tree.n_features_)
        ]

    ginis = np.array([x for (x, _) in best_splits])
    best_idx = np.argmin(ginis)
    best_thr = best_splits[best_idx][1]

    return best_idx, best_thr


@ray.remote
def run_in_cluster():
    dataset = datasets.fetch_covtype(data_home=tempfile.mkdtemp())
    X, y = dataset.data, dataset.target - 1
    training_size = 400000
    max_depth = 10
    clf = DecisionTreeClassifier(max_depth=max_depth)
    start = time.time()
    clf.fit(X[:training_size], y[:training_size])
    end = time.time()
    y_pred = clf.predict(X[training_size:])
    accuracy = metrics.accuracy_score(y[training_size:], y_pred)
    return end - start, accuracy


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--concurrency", type=int, default=1)
    args = parser.parse_args()

    ray.init(address=os.environ["RAY_ADDRESS"])

    futures = []
    for i in range(args.concurrency):
        print(f"concurrent run: {i}")
        futures.append(run_in_cluster.remote())
        time.sleep(10)

    for i, f in enumerate(futures):
        treetime, accuracy = ray.get(f)
        print(f"Tree {i} building took {treetime} seconds")
        print(f"Test Accuracy: {accuracy}")

    with open(os.environ["TEST_OUTPUT_JSON"], "w") as f:
        f.write(json.dumps({"build_time": treetime, "success": 1}))
