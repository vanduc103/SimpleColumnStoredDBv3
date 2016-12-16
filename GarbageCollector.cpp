/*
 * GarbageCollector.cpp
 *
 *  Created on: Nov 13, 2016
 *      Author: duclv
 */
#include "GarbageCollector.h"

namespace std {
	vector<size_t>* GarbageCollector::recentlyUpdatedRids = new vector<size_t>();
};
