/*
 * Table.h
 *
 *  Created on: Sep 26, 2016
 *      Author: duclv
 */

#ifndef TABLE_H_
#define TABLE_H_

#include <string>
#include <tuple>
#include <vector>
#include <iostream>
#include "Column.h"
#include "ColumnBase.h"
#include "Util.h"

namespace std {

class Table {
private:
	vector<ColumnBase*>* m_columns;
	string name;
public:
	virtual ~Table() {
		for (ColumnBase* column : (*m_columns)) {
			delete column;
		}
		delete m_columns;
	}

	Table(vector<ColumnBase*>* columns) {
		if (columns == NULL) columns = new vector<ColumnBase*>();
		m_columns = columns;
	}

	Table() {
		m_columns = new vector<ColumnBase*>();
	}

	vector<ColumnBase*>* columns() {
		return m_columns;
	}

	string getName() {
		return name;
	}
	void setName(string tableName) {
		name = tableName;
	}

	int numOfColumns() {
		return (int) m_columns->size();
	}

	ColumnBase* getColumnByName(string colName) {
		for (size_t i = 0; i < m_columns->size(); i++) {
			ColumnBase* column = m_columns->at(i);
			if (column->getName() == colName)
				return column;
		}
		return NULL;
	}

	// do some processes on all columns
	void processColumn(uint64_t csn = 0) {
		for (ColumnBase* colBase : *m_columns) {
			if (colBase->getType() == ColumnBase::intType) {
				Column<int>* col = (Column<int>*) colBase;
				if (col->isBulkInsert())
					col->bulkBuildVecVector(csn);
				col->getDictionary()->clearTemp();
				col->bitPackingVecValue();
			}
			else if (colBase->getType() == ColumnBase::charType ||
					 colBase->getType() == ColumnBase::varcharType) {
				Column<string>* col = (Column<string>*) colBase;
				if (col->isBulkInsert()) {
					col->bulkBuildVecVector(csn);
				}
				if (col->isCreateInvertedIndex()) {
					col->createInvertedIndex();
				}
				col->getDictionary()->clearTemp();
				col->bitPackingVecValue();
			}
		}
	}

	// save checkpoint
	string saveToDisk(string logPath) {
		vector<string> listFileName;
		string tableFileName = logPath + "/table_" + to_string(Util::currentMilisecond());
		vector<string>* content = new vector<string>();
		content->push_back(this->name);
		// save column
		for (ColumnBase* colBase : *m_columns) {
			if (colBase->getType() == ColumnBase::intType) {
				Column<int>* col = (Column<int>*) colBase;
				vector<string> filesToSave = col->saveToDisk(logPath);
				content->push_back("INTEGER");
				for(string f : filesToSave) {
					content->push_back(f);
				}
			}
			else if (colBase->getType() == ColumnBase::charType ||
					 colBase->getType() == ColumnBase::varcharType) {
				Column<string>* col = (Column<string>*) colBase;
				vector<string> filesToSave = col->saveToDisk(logPath);
				content->push_back("STRING");
				for(string f : filesToSave) {
					content->push_back(f);
				}
			}
		}
		Util::saveToDisk(content, tableFileName);
		return tableFileName;
	}

	// restore from checkpoint
	void restore(string tableFile) {
		vector<string>* content = new vector<string>();
		Util::readFromDisk(content, tableFile);
		if (content->size() >= 1) {
			// table name
			this->name = content->at(0);
		}
		// restore columns
		size_t length = content->size();
		for (size_t i = 1; i < length; i = i+5) {
			// column type
			string colType = content->at(i+1);
			ColumnBase* colBase = new ColumnBase();
			if (colType == "INTEGER") {
				Column<int>* col = new Column<int>();
				if (i+4 < length)
					col->restore(content->at(i+1), content->at(i+2),
							content->at(i+3), content->at(i+4));
				colBase = col;
			}
			else {
				Column<string>* col = new Column<string>();
				if (i+3 < length)
					col->restore(content->at(i+1), content->at(i+2),
							content->at(i+3), content->at(i+4));
				colBase = col;
			}
			this->m_columns->push_back(colBase);
		}
		delete content;
	}

	// redo log restore
	void redoLogRestore(string colName,
						vector<string>* deltaSpaceLog, vector<string>* versionVecValueLog,
						vector<string>* insertLog,
						vector<string>* versionColumnLog, vector<string>* hashtableLog) {
		ColumnBase* colBase = getColumnByName(colName);
		if (colBase->getType() == ColumnBase::intType) {
			Column<int>* col = (Column<int>*) colBase;
			col->redoLogRestore(deltaSpaceLog, versionVecValueLog,
					insertLog, versionColumnLog, hashtableLog);
		}
		else {
			Column<string>* col = (Column<string>*) colBase;
			col->redoLogRestore(deltaSpaceLog, versionVecValueLog,
					insertLog, versionColumnLog, hashtableLog);
		}
	}

};

} /* namespace std */

#endif /* TABLE_H_ */
