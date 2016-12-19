
#include "ClientSocket.h"
#include "SocketException.h"
#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <thread>

using namespace std;

void startNewClient(int interval, std::ofstream& out) {
	std::thread([interval, &out]()
	{
		while (true) {
			try {
				// Create the Socket
				ClientSocket c_socket("localhost", 30000);
				std::string command;
				std::string reply;

				cout << "Client command: ";
				getline(cin, command);
				if(command != "") {
					try {
						out << "[Sending]\t" + command + "\n";
						std::cout << "[Sending]\t" + command + "\n";
						c_socket << command;
						// receive reply from server
						c_socket >> reply;
					} catch (SocketException&) {}
					out << "[Response]\t" << reply << "\n";
					std::cout << "[Response]\t" << endl << reply << "\n";
				}
			} catch(SocketException& e) {
				std::cout << "Exception caught: " << e.description() << std::endl;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(interval));
		}
	}).detach();
}

int main(int argc, char* argv[]) {
	std::ofstream out("client.log");
	startNewClient(1000, out);

	// loop forever
	while(true);

    return 0;
}
