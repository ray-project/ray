from collections import defaultdict, deque


class DependencyGraph:
    def __init__(self):
        self.graph = defaultdict(set)  # Task ID -> Set of dependent Task IDs

    def add_dependency(self, task, dependent_task):
        self.graph[task].add(dependent_task)

    def topological_sort(self):
        in_degree = defaultdict(int)

        # Calculate in-degrees
        for node in self.graph:
            for neighbor in self.graph[node]:
                in_degree[neighbor] += 1

        # Start with nodes with no in-dependencies
        zero_in_degree = [node for node in self.graph if in_degree[node] == 0]

        sorted_tasks = []
        queue = deque(zero_in_degree)

        while queue:
            current = queue.popleft()
            sorted_tasks.append(current)

            for neighbor in self.graph[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        return sorted_tasks

    def print_dependencies(self):
        """Print all dependencies in a readable format."""
        print("Dependency Graph:")
        for parent, children in self.graph.items():
            children_str = ", ".join(str(child) for child in children)
            print(f"{parent} -> [{children_str}]")
