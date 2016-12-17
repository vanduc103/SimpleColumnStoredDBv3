/*
 * GarbageCollector.cpp
 *
 *  Created on: Nov 13, 2016
 *      Author: duclv
 */
#include "GarbageCollector.h"
#include "Table.h"
#include <iostream>

namespace std {
	vector<size_t>* GarbageCollector::recentlyUpdatedRids = new vector<size_t>();

	// run garbage collection
	void GarbageCollector::run() {
		// if no recently updated rid => stop
		if (recentlyUpdatedRids->size() == 0) {
			return;
		}
		cout << "Garbage Collector running ! with #rids = " << recentlyUpdatedRids->size() << endl;

		table->processGarbageCollect(recentlyUpdatedRids, transaction);
		cout << "Garbage Collector run finished !" << endl;

		// clear recently updated rid
		recentlyUpdatedRids->clear();
	};

	void GarbageCollector::start(int interval)
	{
		std::thread([this, interval]()
		{
			while (true) {
				run();
				std::this_thread::sleep_for(
				std::chrono::milliseconds(interval));
			}
		}).detach();
	};
}
