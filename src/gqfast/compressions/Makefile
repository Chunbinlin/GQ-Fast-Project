CXX = g++
CXXFLAGS = -g -Wall -w -std=c++11
LFLAGS = -Wall
EXEC = compressions.exe
RM = rm -f

SRCS = dictionary.cpp bitmap.cpp main.cpp
OBJS = $(subst .cpp,.o,$(SRCS))

all: $(EXEC)
$(EXEC): $(OBJS)      
	$(CXX) $(LFLAGS) -o $(EXEC) $(OBJS) 

%.o: %.c
	$(CXX) $(CXXFLAGS) -c $<

clean:
	$(RM) $(EXEC) $(OBJS)








 

