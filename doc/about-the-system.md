# About the System

This document describes the current architecture of Ray. However, some of these
decisions are likely to change.

## Components

A Ray cluster consists of several components.

- One scheduler
- Multiple workers per node
- One object store per node
- One (or more) drivers

### The scheduler

The scheduler assigns tasks to the workers.

### The workers

The workers execute tasks and submit tasks to the scheduler.

### The object store

The object store shares objects between the worker processes on the same node so
that the workers don't need to each have their own copies of the objects.

### The driver

The driver submits tasks to the scheduler. If you use Ray in a script, the
Python process running the script is the driver. If you use Ray interactively
through a shell, the shell process is the driver.
