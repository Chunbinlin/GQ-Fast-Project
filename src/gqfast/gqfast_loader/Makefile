CXX = g++
CXXFLAGS = -g -Wall -w -std=c++11 -pthread
LFLAGS = -Wall
CPPFLAGS = 
LDFLAGS = -g -w -std=c++11 -export-dynamic -O3 
EXEC = loader
RM = rm -f

SRCS = main.cpp
OBJS = $(subst .cpp,.o,$(SRCS))

all: $(EXEC)
$(EXEC): $(OBJS)      
	$(CXX) $(LDFLAGS) -o $(EXEC) $(OBJS) -ldl -lboost_serialization -lpthread 

%.o: %.c
	$(CXX) $(CXXFLAGS) -c $< -ldl

clean:
	$(RM) $(EXEC) $(OBJS)

smdb_array: smdb_array.o
	 g++ -shared -o smdb_array.so smdb_array.o


smdb_array.o: smdb_array.cpp
	$(CXX) -Wall -std=c++11 -fPIC -c smdb_array.cpp
	
q5_bitmap_threaded: q5_bitmap_threaded.o
	g++ -shared -o q5_bitmap_threaded.so q5_bitmap_threaded.o


q5_bitmap_threaded.o: q5_bitmap_threaded.cpp
	$(CXX) -Wall -std=c++11 -fPIC -c q5_bitmap_threaded.cpp


q5_bitmap: q5_bitmap.o
	g++ -shared -std=c++11 -o q5_bitmap.so q5_bitmap.o


q5_bitmap.o: q5_bitmap.cpp
	$(CXX) -Wall -std=c++11 -fPIC -c q5_bitmap.cpp


q5_array: q5_array.o
	g++ -shared -o q5_array.so q5_array.o


q5_array.o: q5_array.cpp
	$(CXX) -Wall -std=c++11 -fPIC -c q5_array.cpp








 

