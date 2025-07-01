PROMPT_TEMPLATE = """

# Ray Application Performance Analysis Report

This template provides a structured framework for analyzing Ray distributed applications to identify performance bottlenecks, resource utilization patterns, and optimization opportunities. The goal is to provide actionable, developer-focused insights based on empirical data.

## 1. Performance Summary

- **Critical Findings**: Top 3-5 most significant performance issues ranked by impact
  - Performance bottlenecks with quantified impact

- **Priority Optimizations**: Highest-impact improvement opportunities with estimated gains
  - Ranked by performance improvement potential

## 2. Distributed Application Topology Analysis

- **Node Distribution**:

- **GPU Profile**:

- **Memory Profile**:

- **CPU Profile**:

## 3. Distributed Execution Analysis

- **Actor/Task System Behavior**:

- **Scalability Characteristics**:

- **Resource Scheduling Efficiency**:

## 4. Data Flow Analysis

- **Object Transfer Patterns**

## 5. Application-Specific Analysis

- **Workload Characterization**:

- **Application Architecture**:

## Recommendations

- **Architecture Optimizations**

- **Other Improvements**:

## Analysis Output Format

Please provide your analysis in Markdown format with each field containing detailed markdown-formatted content:

Each subject should have detailed description with a key insight at each large subject.

1. Include tables and mermaid graphs. For Mermaid diagrams, always use proper syntax with a specified diagram type.

USE AS MANY TYPES OF GRAPHS AS POSSIBLE.
BUT DO NOT USED BAR CHART IN MERMAID, USE TABLE INSTEAD.

2. For actors and functions and methods, do not use ids like "method1", "function1" to analyze, use name instead.

"""
