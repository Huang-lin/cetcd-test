UNAME=$(shell uname)
CFLAGS= -O0 -Wall
CCFLAGS=-Wall -Wextra -Wno-unused-parameter -O3
LDFLAGS=-lcurl -lpthread -lyajl 

ifeq ($(UNAME), Darwin)
#LDFLAGS+=-Wl,-flat_namespace,-undefined,dynamic_lookup
endif

tset: main.o cetcdTest.o ../ctest/ctest.h
	gcc  -o test cetcdTest.o main.o $(LDFLAGS)
cetcdTest.o:cetcdTest.c ../cetcd.c ../cetcd_array.c ../sds/sds.c ../ctest/ctest.h
	gcc $(CCFLAGS) -c cetcdTest.c  ../cetcd.c ../cetcd_array.c ../sds/sds.c
main.o:main.c ../ctest/ctest.h
	gcc $(CCFLAGS) -c main.c
	

clean:
	rm -f test *.o
