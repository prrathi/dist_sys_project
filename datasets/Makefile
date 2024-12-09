CXX = g++
CXXFLAGS = -std=c++11 -Wall

all: test1_1 test1_2 test2_1 test2_2

test1_1: test1_1.cpp
	$(CXX) $(CXXFLAGS) -o test1_1 test1_1.cpp
test1_2: test1_2.cpp
	$(CXX) $(CXXFLAGS) -o test1_2 test1_2.cpp
test2_1: test2_1.cpp
	$(CXX) $(CXXFLAGS) -o test2_1 test2_1.cpp
test2_2: test2_2.cpp
	$(CXX) $(CXXFLAGS) -o test2_2 test2_2.cpp

clean:
	rm -f test1_1 test1_2 test2_1 test2_2

.PHONY: all clean