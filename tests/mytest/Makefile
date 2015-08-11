UNAME=$(shell uname)
CFLAGS= -Wall
CCFLAGS=-Wall -Wextra -Wno-unused-parameter
LDFLAGS=-lcurl -lpthread -lyajl 

ifeq ($(UNAME), Darwin)
#LDFLAGS+=-Wl,-flat_namespace,-undefined,dynamic_lookup
endif

test: cetcdTest.o main.o
	gcc -Wall -g -o test cetcdTest.o main.o ../../cetcd/cetcd.c ../../cetcd/sds/sds.c ../../cetcd/cetcd_array.c $(LDFLAGS)
main.o: main.c ../ctest/ctest.h
cetcdTest.o: cetcdTest.c ../../cetcd/cetcd.h ../../cetcd/sds/sds.h ../../cetcd/cetcd_array.h \
	../ctest/ctest.h

clean:
	rm -f test *.o
