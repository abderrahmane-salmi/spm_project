CXX = g++
CXXFLAGS = -std=c++17 -O2
INCLUDES = -Iinclude

SRCS = main.cpp
OBJS = $(SRCS:.cpp=.o)

all: sorter

sorter: $(OBJS)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $@ $^

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $<

clean:
	rm -f *.o sorter
