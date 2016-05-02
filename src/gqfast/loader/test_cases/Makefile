CXX = g++
CXXFLAGS = -Wall -std=c++11 -fPIC
LFLAGS = -shared

RM = rm -f

SRCS := $(wildcard *.cpp)
OBJS := $(patsubst %.cpp,%.o,$(SRCS))
LIBS := $(patsubst %.o, %.so,$(OBJS))

all: $(LIBS)

.cpp.o:
	$(CXX) -c $(CXXFLAGS) $< -o $@

%.so: %.o
	$(CXX) $(LFLAGS) -o $@ $<

clean:
	$(RM) $(OBJS) $(LIBS)





 

