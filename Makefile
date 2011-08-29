all:
	g++ -g -O2 -o pzq src/main.cpp src/store.cpp src/receiver.cpp src/sender.cpp -I/opt/kyotocabinet/include -L/opt/kyotocabinet/lib -I/opt/local/include -L/opt/local/lib -lzmq -lkyotocabinet -lboost_thread-mt

clean:
	rm pzq