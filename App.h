/*
 * TestCpp.h
 *
 *  Created on: Nov 11, 2016
 *      Author: duclv
 */

#ifndef APP_H_
#define APP_H_

#include <string>
#include <vector>
#include "Table.h"
#include "GarbageCollector.h"
#include "Transaction.h"
#include "Logging.h"
#include "server/ServerSocket.h"

using namespace std;

Table* createTable(string createQuery);
string updateCommand(ServerSocket* client, Table* table, Transaction* transaction, Logging* logging, vector<string> command, size_t txIdx = -1);
string insertCommand(Table* table, Transaction* transaction, Logging* logging, vector<string> command);
string scanCommand(Table* table, Transaction* transaction, vector<string> command);
string abortCommand(Table* table, Transaction* transaction, vector<string> command);
void printTableInfo(Table* table);

#endif /* APP_H_ */
