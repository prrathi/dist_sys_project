CXX = g++
CXXFLAGS = -std=c++11 -Wall

all: test1_1 test1_2

test1_1: test1_1.cpp
	$(CXX) $(CXXFLAGS) -o test1_1 test1_1.cpp

test1_2: test1_2.cpp
	$(CXX) $(CXXFLAGS) -o test1_2 test1_2.cpp

clean:
	rm -f test1_1 test1_2

.PHONY: all clean