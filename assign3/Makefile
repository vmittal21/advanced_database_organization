CC=gcc
CFLAGS=-I.

testcase: test_assign3_1.c record_mgr.c storage_mgr.c dberror.c buffer_mgr.c buffer_mgr_stat.c expr.c rm_serializer.c
	gcc -o testcase test_assign3_1.c record_mgr.c storage_mgr.c dberror.c buffer_mgr.c buffer_mgr_stat.c expr.c  rm_serializer.c -I.

testexpr: test_expr.c record_mgr.c storage_mgr.c dberror.c buffer_mgr.c buffer_mgr_stat.c expr.c rm_serializer.c
	gcc -o testexpr test_expr.c record_mgr.c storage_mgr.c dberror.c buffer_mgr.c buffer_mgr_stat.c expr.c  rm_serializer.c -I.

clean:
	rm -rf *.o
	rm -rf testcase