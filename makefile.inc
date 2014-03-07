## For students: change this path to the root of your code
#CODEROOT = ".."
CODEROOT = "$(realpath $(dir $(lastword $(MAKEFILE_LIST))))"

LDLIBS = -lreadline	

#CC = gcc
CC = g++

#CPPFLAGS = -Wall -I$(CODEROOT) -O3 -std=c++11 # maximal optimization
CPPFLAGS = -Wall -I$(CODEROOT) -g   -std=c++11 -DDATABASE_FOLDER=\"$(CODEROOT)/cli/\" # with debugging info
	