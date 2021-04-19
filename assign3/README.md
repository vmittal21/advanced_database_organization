# Advanced Database Organization - CS525 Spring 21

## Assignment 3 : Record Manager

### Team:

    1. Vidhi Mittal (A20449865)
    2. Tushar Nitave (A20444211)
    3. Souporno Ghosh (A20439047)

### Introduction:

    The goal of this assignment is to implement a simple record manager

### Directory Structure:
    .
    +-- assign3
        +-- buffer_mgr_stat.c
        +-- dberror.c
        +-- dberror.h
        +-- buffer_mgr.h
        +-- buffer_mgr.c
        +-- dt.h
        +-- buffer_mgr_stat.h
        +-- expr.c
        +-- expr.h
        +-- test_helper.h
        +-- Makefile
        +-- record_mgr.h
        +-- record_mgr.c
        +-- rm_serializer.c
        +-- tables.h
        +-- storage_mgr.c
        +-- storage_mgr.h
        +-- test_expr.c
    
### Requirements
    [GNU GCC Compiler](https://gcc.gnu.org/)

### Execution
    In order to execute testcase, type following command in your terminal and hit enter.

    ~$: make testcase
    ~$: ./testcase

    ~$: make testexpr
    ~$: ./testexpr    
    To remove all the compiled files execute:

    ~$: make clean

