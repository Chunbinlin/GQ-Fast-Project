all: test_db_maker

test_db_maker: test_db_maker.o
	g++ -Wall -o test_db_maker test_db_maker.o 


test_db_maker.o: test_db_maker.cpp
	g++ -Wall -std=c++11 -c test_db_maker.cpp


clean:
	rm test_db_maker test_db_maker.o


