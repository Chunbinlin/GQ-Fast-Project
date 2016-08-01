CXX = g++
CXXFLAGS = -g -Wall -w -std=c++11 -pthread
LFLAGS = -Wall
CPPFLAGS =
LDFLAGS = -g -w -std=c++11 -export-dynamic -O3
EXEC = loader
RM = rm -f

SRCS = ../encodings.cpp graph_main.cpp
OBJS = $(subst .cpp,.o,$(SRCS))

all: $(EXEC)
$(EXEC): $(OBJS)
	$(CXX) $(LDFLAGS) -o $(EXEC) $(OBJS) -ldl -lpthread

%.o: %.c
	$(CXX) $(CXXFLAGS) -c $< -ldl

clean:
	$(RM) $(EXEC) $(OBJS)

make_graph: make_graph.o
	g++ -Wall -o make_graph make_graph.o 

make_graph.o: make_graph.cpp
	g++ -Wall -std=c++11 -c make_graph.cpp




