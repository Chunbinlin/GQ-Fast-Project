XX = g++
CXXFLAGS = -Wall -std=c++11 -fPIC
LFLAGS = -shared

RM = rm -f

SRCS := ./gqfast/loader/encodings.cpp gqfast_global_JNILoader.cpp
OBJS := encodings.o gqfast_global_JNILoader.o 
LIBS := libgqfast_global_JNILoader.so

all: $(LIBS)

.cpp.o:
	$(CXX) -c $(CXXFLAGS) $< -o $@

$(LIBS): $(OBJS)
	$(CXX) $(LFLAGS) -o $@ $<

clean:
	$(RM) $(OBJS) $(LIBS)









 

