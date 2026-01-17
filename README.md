# TPC-H Query 5 Implementation

Implementation of TPC-H Query 5 with multithreading support in C++.

## Problem Statement

Calculate revenue by nation for customers in ASIA region between 1994-01-01 and 1995-01-01.

## Requirements

- GCC compiler with C++17 support
- TPC-H Scale Factor 2 data files

### Generate Scale Factor 0.1
```bash
cd "C:\Users\Amaan M k\OneDrive\Desktop\Zetttabolt\tpch-tools\dbgen"
dbgen.exe -s 0.1 -v
```

### Compilation

```bash
g++ -std=c++17 -O3 -o query5.exe src\main.cpp src\query5.cpp -I include
```
### Execution
```bash
query5.exe --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 1 --table_path data --result_path results\output.txt
```

### Explected Output
Nation Name|Revenue
CHINA|7822103.00
INDIA|6376121.51
JAPAN|6000077.22
INDONESIA|5580475.40
VIETNAM|4497840.55