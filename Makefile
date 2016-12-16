CXXFLAGS =	-O2 -g -Wall -fmessage-length=0 -std=c++11 -lstdc++ -pthread -lsqlparser -L/usr/local/lib/ \
			-I/sql-parser -I/usr/local/boost_1_61_0 -I/server

OBJS =		TestCpp.o Table.o Dictionary.o Column.o ColumnBase.o PackedArray.o Util.o Transaction.o GarbageCollector.o Logging.o \
			$(patsubst %.o,server/%.o,ServerSocket.o Socket.o) server_main.o 

LIBS =		-L/usr/local/lib/ -lsqlparser -lboost_system -lboost_filesystem

OTHERS =	-pthread -std=c++11

TARGET =	TestServer

$(TARGET):	$(OBJS)
	$(CXX) -o $(TARGET) $(OBJS) $(LIBS) $(OTHERS)

all:	$(TARGET)

clean:
	rm -f $(OBJS) $(TARGET)
