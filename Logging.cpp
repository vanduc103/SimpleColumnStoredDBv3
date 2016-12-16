/*
 * Logging.cpp
 *
 *  Created on: Dec 9, 2016
 *      Author: duclv
 */

#include "Logging.h"
#include "Util.h"
#include "Table.h"
#include <stdio.h>

namespace std {

Logging::Logging() {
	privateLogBuffer = new vector<logging>();
}

Logging::~Logging() {
	if (privateLogBuffer != NULL) {
		delete privateLogBuffer;
	}
}

vector<Logging::logging>* Logging::publicLogBuffer = new vector<Logging::logging>();


void Logging::saveCheckpoint(Table* table) {
	/*// create folder log
	Util::createFolder(this->logPath);*/

	// write content
	string fileName = this->logPath + "/checkpoint_" + to_string(Util::currentMilisecond());
	if (table != NULL) {
		vector<string>* content = new vector<string>();
		content->push_back("Checkpoint start " + to_string(Util::currentMilisecond()));
		content->push_back(table->saveToDisk(this->logPath));
		content->push_back("Checkpoint end " + to_string(Util::currentMilisecond()));
		Util::saveToDisk(content, fileName);
	}
}

void Logging::redoLogUpdate(size_t txIdx, LOG_ACTION txAction){
	logging newLog;
	newLog.txIdx = txIdx;
	newLog.txAction = txAction;
	privateLogBuffer->push_back(newLog);
}

void Logging::redoLogUpdate(size_t txIdx, LOG_ACTION txAction, string colName){
	logging newLog;
	newLog.txIdx = txIdx;
	newLog.txAction = txAction;
	newLog.colName = colName;
	privateLogBuffer->push_back(newLog);
}

void Logging::redoLogAdd(size_t txIdx, LOG_OBJECT objType, vector<string> logContent) {
	logging newLog;
	newLog.txIdx = txIdx;
	newLog.objType = objType;
	newLog.logContent = logContent;
	privateLogBuffer->push_back(newLog);
}

void Logging::redoLogPublicMerge() {
	// save this private log buffer to public buffer
	publicLogBuffer->insert(publicLogBuffer->end(), this->privateLogBuffer->begin(), this->privateLogBuffer->end());
}

void Logging::redoLogSave() {
	string logFileName = this->logPath + "/redo_log_" + to_string(Util::currentMilisecond());
	vector<string>* content = new vector<string>();
	for (size_t i = 0; i < publicLogBuffer->size(); i++) {
		logging log = publicLogBuffer->at(i);
		string contentValue = to_string(log.txIdx);
		switch (log.txAction) {
			case TX_START: {
				contentValue += "|start";
				break;
			}
			case TX_COMMIT: {
				contentValue += "|commit";
				break;
			}
			case TX_END: {
				contentValue += "|end";
				break;
			}
			case COLUMN_START: {
				contentValue += "|column_start";
				break;
			}
			case COLUMN_END: {
				contentValue += "|column_end";
				break;
			}
		}
		if (log.colName != "") {
			contentValue += "|" + log.colName;
		}
		switch (log.objType) {
			case INSERT:
				contentValue += "|insert|";
				break;
			case DELTA_SPACE:
				contentValue += "|delta_space|";
				break;
			case VERSION_VECVALUE:
				contentValue += "|version_vec_value|";
				break;
			case HASHTABLE:
				contentValue += "|hashtable|";
				break;
			case VERSION_COLUMN:
				contentValue += "|version_column|";
				break;
		}
		if (log.logContent.size() > 0) {
			string tmpValue = "";
			for (size_t i = 0; i < log.logContent.size(); i++) {
				string value = log.logContent.at(i);
				tmpValue += value + "|";
			}
			contentValue += tmpValue;
			content->push_back(contentValue);
		}
	}
	if (publicLogBuffer->size() > 0) {
		cout << "Start log save" << endl;
		// save to disk
		Util::saveToDisk(content, logFileName);
		publicLogBuffer->resize(0);
		cout << "End log save" << endl;
	}
}

void Logging::restore(Table* table) {
	cout << "here -1" << endl;
	// get latest checkpoint
	string latestCkpt = Util::getLatestFile(logPath, "checkpoint");
	if (latestCkpt != "") {
		cout << "Start redo log restore !" << endl;
		// restore from checkpoint
		vector<string>* content = new vector<string>();
		Util::readFromDisk(content, latestCkpt);
		if (content->size() >= 3) {
			string tableFile = content->at(1);
			table->restore(tableFile);
		}
		delete content;
		// get redo log from checkpoint
		long ckptTime = stol(latestCkpt.substr(latestCkpt.find("checkpoint") + 1));
		vector<string> allRedoLogs = Util::getNewestFiles(logPath, "redo_log", ckptTime);
		// sort by oldest to newest file name
		std::sort(allRedoLogs.begin(), allRedoLogs.end());
		// restore redo log
		for (string redoFile : allRedoLogs) {
			vector<string>* redoContent = new vector<string>();
			Util::readFromDisk(redoContent, redoFile);
			vector<string>* deltaSpaceLog = new vector<string>();
			vector<string>* versionVecValueLog = new vector<string>();
			vector<string>* insertLog = new vector<string>();
			vector<string>* versionColumnLog = new vector<string>();
			vector<string>* hashtableLog = new vector<string>();

			for (size_t i = 0; i < redoContent->size(); i++) {
				string log = redoContent->at(i);
				string colName = "";
				if (log.find("column_start") != string::npos) {
					colName = log.substr(log.find("column_start")+1);
					// re-init log vector
					deltaSpaceLog = new vector<string>();
					versionVecValueLog = new vector<string>();
					insertLog = new vector<string>();
					versionColumnLog = new vector<string>();
					hashtableLog = new vector<string>();
				}
				if (log.find("delta_space") != string::npos) {
					string logContent = log.substr(log.find("delta_space")+1);
					Util::parseContentToVector(deltaSpaceLog, logContent, "|");
				}
				else if (log.find("insert") != string::npos) {
					string logContent = log.substr(log.find("insert")+1);
					Util::parseContentToVector(insertLog, logContent, "|");
				}
				else if (log.find("version_vec_value") != string::npos) {
					string logContent = log.substr(log.find("version_vec_value")+1);
					Util::parseContentToVector(versionVecValueLog, logContent, "|");
				}
				else if (log.find("version_column") != string::npos) {
					string logContent = log.substr(log.find("version_column")+1);
					Util::parseContentToVector(versionColumnLog, logContent, "|");
				}
				else if (log.find("hashtable") != string::npos) {
					string logContent = log.substr(log.find("hashtable")+1);
					Util::parseContentToVector(hashtableLog, logContent, "|");
				}
				if (log.find("column_end") != string::npos && table != NULL && colName != "") {
					// restore
					table->redoLogRestore(colName, deltaSpaceLog, versionVecValueLog,
							insertLog, versionColumnLog, hashtableLog);
				}
			}
			delete redoContent;
		}
		cout << "End redo log restore !" << endl;
	}
}

} /* namespace std */
