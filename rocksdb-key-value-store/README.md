# RocksDB Key-Value Store (NoSQL Storage Layer)

## Overview
This project implements a persistent key-value store using RocksDB, focusing on
high-throughput reads/writes and storage-engine fundamentals. It demonstrates
how modern NoSQL storage layers expose simple APIs while relying on underlying
LSM-tree based design for performance and durability.

## Features
- Persistent key-value storage using RocksDB
- Basic operations: Put / Get / Delete
- Batch writes for improved write throughput
- MultiGet support for efficient point lookups
- Iterator-based scans for range traversal

## Technologies Used
- C++
- RocksDB (embedded key-value store)

## Build & Run
This project is designed to be compiled and executed in a local environment
with RocksDB installed.

> Note: Exact build/run commands depend on your local RocksDB installation and
compiler toolchain (e.g., g++, clang++).
