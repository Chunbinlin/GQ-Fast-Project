all: make_graph

make_graph: make_graph.o
	g++ -Wall -o make_graph make_graph.o 

make_graph.o: make_graph.cpp
	g++ -Wall -std=c++11 -c make_graph.cpp


clean:
	rm make_graph make_graph.o


