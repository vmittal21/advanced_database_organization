# Advanced Database Organization - CS525 Spring 21

## Assignment 2 : Buffer Manager

### Team:

    1. Vidhi Mittal (A20449865)
    2. Tushar Nitave (A20444211)
    3. Souporno Ghosh (A20439047)

### Introduction:

    The goal of this assignment is to implement a simple buffer manager - a module that manages a fixed number of pages in memory that represent pages from a page  managed by the storage manager implemented in assignment 1. The buffer pool implemented in this assignments uses three replacement strategies: FIFO, LRU and CLOCK/Second Chance.

### Directory Structure:
    .
    +-- assign2
        +-- dberror.c
        +-- dberror.h
        +-- Makefile
        +-- buffer_mgr.h
        +-- buffer_mgr.c
        +-- dt.h
        +-- buffer_mgr_stat.h
        +-- storage_mgr.c
        +-- storage_mgr.h
        +-- test_assign2_1.c
        +-- test_assign2_2.c
        +-- test_helper.h
    
### Requirements
    [GNU GCC Compiler](https://gcc.gnu.org/)

### Execution
    In order to execute first and second testcase, type following command in your terminal and hit enter.

    ~$ make testcase1
    ~$ make testcase2

    Execute following command to run both the testcases.

    ~$ ./testcase1
    ~$ ./testcase2

    To remove all the compiled files execute:

    ~$: make clean

### Note
1. Implemented additional replacement strategy CLOCK/Second Chance.
2. Additional test cases for CLOCK replacement strategy is added in ```test_assign2_2.c``` file. 
3. Added additional method displayBuffer() displays current state of buffer pool.

### References
1. https://stackoverflow.com/questions/5833094/get-a-timestamp-in-c-in-microseconds
2. https://stackoverflow.com/questions/39625579/c-overflow-in-implicit-constant-conversion-woverflow/39625680
3. https://cs.stackexchange.com/questions/24011/clock-page-replacement-algorithm-already-existing-pages
4. https://www.geeksforgeeks.org/lru-cache-implementation/