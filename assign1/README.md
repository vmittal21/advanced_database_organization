# Advanced Database Organization - CS525 Spring 21

## Assignment 1 : Storage Manager

### Team:
    1. Vidhi Mittal (A20449865)
    2. Tushar Nitave (A20444211)
    3. Souporno Ghosh (A20439047)
### Introduction:

    The goal of this assignment is to implement a simple storage manager - a module that is capable of reading blocks
    from a file on disk into memory and writing blocks from memory to a file on disk. The storage manager deals with
    pages (blocks) of fixed size (PAGE SIZE). In addition to reading and writing pages from a file, it provides methods for
    creating, opening, and closing files. The storage manager has to maintain several types of information for an open
    file: The number of total pages in the file, the current page position (for reading and writing), the file name, and a
    POSIX file descriptor or FILE pointer. 

### Directory Structure:
    .
    +-- assign1
        +-- dberror.c
        +-- dberror.h
        +-- makefile
        +-- storage_mgr.c
        +-- storage_mgr.h
        +-- test_assign1_1.c
        +-- test_helper.h
    
### Requirements
    [GNU GCC Compiler](https://gcc.gnu.org/)

### Execution
    Execute the main file to compile all the files. Type following command in your terminal and hit enter.

    ~$ make

    Execute following command to run the testcase.

    ~$ ./testcase

### Note
    1. We have integrated additional test cases into test_assign1_1.c


### References
1. https://www.tutorialspoint.com/c_standard_library/c_function_memset.htm
2. https://www.tutorialspoint.com/c_standard_library/c_function_fwrite.htm
3. https://man7.org/linux/man-pages/man2/unlink.2.html
4. https://man7.org/linux/man-pages/man3/fread.3.html
5. https://man7.org/linux/man-pages/man3/fseek.3.html