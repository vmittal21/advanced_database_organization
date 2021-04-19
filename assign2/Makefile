CC=gcc
CFLAGS=-I.

testcase1: test_assign2_1.c storage_mgr.c dberror.c buffer_mgr.c buffer_mgr_stat.c 
	gcc -o testcase1 test_assign2_1.c storage_mgr.c dberror.c buffer_mgr.c buffer_mgr_stat.c  -I.

testcase2: test_assign2_2.c storage_mgr.c dberror.c buffer_mgr.c buffer_mgr_stat.c 
	gcc -o testcase2 test_assign2_2.c storage_mgr.c dberror.c buffer_mgr.c buffer_mgr_stat.c  -I.

clean:
	rm -rf *.o
	rm -rf testcase1 testcase2
