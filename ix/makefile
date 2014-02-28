
include ../makefile.inc

all: libix.a ixtest1 ixtest2 ixtest_me ixtest

# lib file dependencies
libix.a: libix.a(ix.o)  # and possibly other .o files

# c file dependencies
ix.o: ix.h

ixtest1.o: ixtest_util.h

ixtest2.o: ixtest_util.h

ixtest_me.o: ixtest_util.h

ixtest.o: ixtest_util.h

# binary dependencies
ixtest1: ixtest1.o libix.a $(CODEROOT)/rbf/librbf.a 

ixtest2: ixtest2.o libix.a $(CODEROOT)/rbf/librbf.a

ixtest_me: ixtest_me.o libix.a $(CODEROOT)/rbf/librbf.a 

ixtest: ixtest.o libix.a $(CODEROOT)/rbf/librbf.a 

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/rbf/librbf.a
$(CODEROOT)/rbf/librbf.a:
	$(MAKE) -C $(CODEROOT)/rbf librbf.a

.PHONY: clean
clean:
	-rm ixtest1 ixtest2 ixtest_me ixtest *.a *.o
	$(MAKE) -C $(CODEROOT)/rbf clean
