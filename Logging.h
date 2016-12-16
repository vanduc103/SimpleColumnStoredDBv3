/*
 * Logging.h
 *
 *  Created on: Dec 9, 2016
 *      Author: duclv
 */

#ifndef LOGGING_H_
#define LOGGING_H_

#include <string>
#include <vector>

namespace std {

class Table;
class Logging {
public:
	enum LOG_OBJECT {INSERT=1, DELTA_SPACE=2, VERSION_VECVALUE=3, HASHTABLE=4, VERSION_COLUMN=5};
	enum LOG_ACTION {TX_START=1, TX_COMMIT=2, TX_END=3, COLUMN_START=11, COLUMN_END=12};
	struct logging {
		size_t txIdx;
		string colName;
		vector<string> logContent;
		LOG_OBJECT objType;
		LOG_ACTION txAction;
	};
	static vector<logging>* publicLogBuffer;
	static const size_t PUBLIC_LOG_BUFFER_LIMIT = 1000000;
private:
	string logPath = "log";
	vector<logging>* privateLogBuffer;

public:
	Logging();
	virtual ~Logging();

	void saveCheckpoint(Table* table);
	void redoLogUpdate(size_t txIdx, LOG_ACTION txAction);
	void redoLogUpdate(size_t txIdx, LOG_ACTION txAction, string colName);
	void redoLogAdd(size_t txIdx, LOG_OBJECT objType, vector<string> logContent);
	void redoLogPublicMerge();
	void redoLogSave();

	//restore
	void restore(Table* table);
};

} /* namespace std */

#endif /* LOGGING_H_ */
