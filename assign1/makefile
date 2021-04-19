CC=gcc
CFLAGS=-I.

testcase: test_assign1_1.c storage_mgr.c dberror.c
	gcc -o testcase test_assign1_1.c storage_mgr.c dberror.c -I.
	
clean:
	rm -rf *.o
	rm -rf testcase

