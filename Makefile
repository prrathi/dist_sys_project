CXX = g++
CXXFLAGS = -std=c++11 -Wall -Wextra -pthread
INCLUDES = -I.
SOURCES = common.cpp worker.cpp listener.cpp talker.cpp
OBJECTS = $(SOURCES:.cpp=.o)
EXECUTABLE = swim

all: $(EXECUTABLE)

$(EXECUTABLE): main.o $(OBJECTS)
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ -o $@

debug: CXXFLAGS += -DDEBUG
debug: $(EXECUTABLE)

test: test.o $(OBJECTS)
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ -o $@

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

common.o: common.cpp common.h json.hpp
worker.o: worker.cpp worker.h common.h talker.h
listener.o: listener.cpp listener.h common.h worker.h
talker.o: talker.cpp talker.h listener.h worker.h
main.o: main.cpp common.h worker.h talker.h
test.o: test.cpp common.h worker.h talker.h


clean:
	rm -f $(OBJECTS) $(EXECUTABLE) main test

.PHONY: all clean debug

