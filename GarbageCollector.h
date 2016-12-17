/*
 * GarbageCollector.h
 *
 *  Created on: Nov 13, 2016
 *      Author: duclv
 */

#ifndef GARBAGECOLLECTOR_H_
#define GARBAGECOLLECTOR_H_

#include <vector>
#include <string>
#include <thread>
#include <chrono>
#include <stdio.h>

namespace std {

class Table;
class Transaction;
class GarbageCollector {
public:
	// list of recently updated rids
	static vector<size_t>* recentlyUpdatedRids;
private:
	// Transaction and Table
	Transaction* transaction;
	Table* table;

public:
	GarbageCollector() {
		transaction = NULL;
		table = NULL;
	}
	GarbageCollector(Table* table, Transaction* transaction) {
		this->table = table;
		this->transaction = transaction;
	}
	virtual ~GarbageCollector() {}

	static void updateRecentlyUpdateRids(vector<size_t> updateRids) {
		recentlyUpdatedRids->insert(recentlyUpdatedRids->end(), updateRids.begin(), updateRids.end());
	}

	void setTransaction(Transaction* transaction) {
		this->transaction = transaction;
	}

	void setTable(Table * table) {
		this->table = table;
	}

	// run garbage collection
	void run();

	void start(int interval);
};

} /* namespace std */

#endif /* GARBAGECOLLECTOR_H_ */
