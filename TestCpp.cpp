//============================================================================
// Name        : TestCpp.cpp
// Author      : Le Van Duc
// Version     :
// Copyright   : 
// Description : Test C++
//============================================================================

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <iostream>
#include <fstream>
#include <chrono>
#include <ctime>
#include <tuple>
#include <algorithm>
#include <stdexcept>
#include <map>
#include <sstream>
#include <boost/algorithm/string.hpp>

#include "Dictionary.h"
#include "Column.h"
#include "ColumnBase.h"
#include "Table.h"
#include "sql-parser/SQLParser.h"
#include "Util.h"
#include "TestCpp.h"
#include "Transaction.h"
#include "Logging.h"

using namespace std;

// print operator type
std::ostream& operator<<(std::ostream& out, const ColumnBase::OP_TYPE value){
    const char* s = 0;
#define PROCESS_VAL(p, str) case(p): s = str; break;
    switch(value){
        PROCESS_VAL(ColumnBase::OP_TYPE::equalOp, "=");
        PROCESS_VAL(ColumnBase::OP_TYPE::neOp, "<>");
        PROCESS_VAL(ColumnBase::OP_TYPE::gtOp, ">");
        PROCESS_VAL(ColumnBase::OP_TYPE::geOp, ">=");
        PROCESS_VAL(ColumnBase::OP_TYPE::ltOp, "<");
        PROCESS_VAL(ColumnBase::OP_TYPE::leOp, "<=");
        PROCESS_VAL(ColumnBase::OP_TYPE::containOp, "CONTAIN");
    }
#undef PROCESS_VAL
    return out << s;
}

// print column type
std::ostream& operator<<(std::ostream& out, const ColumnBase::COLUMN_TYPE value){
    const char* s = 0;
#define PROCESS_VAL(p, str) case(p): s = str; break;
    switch(value){
        PROCESS_VAL(ColumnBase::intType, "INTEGER");
        PROCESS_VAL(ColumnBase::charType, "TEXT");
        PROCESS_VAL(ColumnBase::varcharType, "TEXT");
    }
#undef PROCESS_VAL
    return out << s;
}

Table* createTable(string createQuery) {
	hsql::SQLParserResult* pCreateQuery = hsql::SQLParser::parseSQLString(createQuery);
	string tableName;
	vector<string> cColumnName;
	vector<ColumnBase::COLUMN_TYPE> cColumnType;
	if (pCreateQuery->isValid) {
		hsql::SQLStatement* stmt = pCreateQuery->getStatement(0);
		if (stmt->type() == hsql::StatementType::kStmtCreate) {
			hsql::CreateStatement* createStmt = (hsql::CreateStatement*) stmt;
			tableName = createStmt->tableName;
			vector<hsql::ColumnDefinition*>* cols = createStmt->columns;
			for (hsql::ColumnDefinition* colDef : *cols) {
				cColumnName.push_back(colDef->name);
				switch (colDef->type) {
					case hsql::ColumnDefinition::INT:
					case hsql::ColumnDefinition::DOUBLE:
						cColumnType.push_back(ColumnBase::intType);
						break;
					case hsql::ColumnDefinition::TEXT:
						cColumnType.push_back(ColumnBase::charType);
						break;
				}
			}
		}
	}
	else {
		cout << "Create table statement is Invalid !" << endl;
		return NULL;
	}
	// init table
	vector<ColumnBase*>* columns = new vector<ColumnBase*>();
	Table* table = new Table(columns);
	table->setName(tableName);
	// init column
	for (size_t i = 0; i < cColumnName.size(); i++) {
		string name = cColumnName[i];
		ColumnBase::COLUMN_TYPE type = cColumnType[i];
		// create new column
		ColumnBase* colBase = new ColumnBase();
		if (type == ColumnBase::intType) {
			Column<int>* col = new Column<int>();
			colBase = col;
		}
		else {
			Column<string>* col = new Column<string>();
			colBase = col;
		}
		colBase->setName(name);
		colBase->setType(type);
		//if (name == "o_orderkey") colBase->setPrimaryKey(true);
		//if (name == "o_comment") colBase->setCreateInvertedIndex(true);
		columns->push_back(colBase);
	}

	// calculate time execution
	clock_t begin_time = clock();
	// create Transaction
	Transaction transaction;
	size_t txIdx = transaction.createTx();
	transaction.startTx(txIdx);
	uint64_t csn = transaction.getTimestampAsCSN();

	// read file
	cout << "Enter table source file path: ";
	string filePath = "/home/duclv/homework/data1M.csv";
	//getline(cin, filePath);
	ifstream infile(filePath);
	string line;
	string delim = ",";
	int row = 0;
	while(getline(infile, line)) {
		size_t last = 0; size_t next = 0;
		char idx = 0;
		vector<string> items;	// split items
		string lastItem;
		string token;
		while ((next = line.find(delim, last)) != string::npos) {
			token = line.substr(last, next - last);
			last = next + delim.length();
			if (idx >= table->numOfColumns() - 1)
				lastItem += token + delim;
			else
				items.push_back(token);
			++idx;
		}
		// get the last token and add to last item
		token = line.substr(last);
		lastItem += token;
		items.push_back(lastItem);

		// process input data based on column type
		for (int i = 0; i < table->numOfColumns(); i++) {
			string item = items[i];
			ColumnBase* colBase = columns->at(i);
			if (colBase->getType() == ColumnBase::intType) {
				int intValue = stoi(item);
				bool sorted = true, bulkInsert = true;
				// update dictionary
				Column<int>* col = (Column<int>*) colBase;
				col->updateDictionary(intValue, sorted, bulkInsert, csn);
			}
			else {
				// char or varchar type
				boost::replace_all(item, "\"", "");
				bool sorted = false, bulkInsert = true;
				// update dictionary
				Column<string>* col = (Column<string>*) colBase;
				col->updateDictionary(item, sorted, bulkInsert, csn);
			}
		}
		++row;
	}
	infile.close();
	// print distinct numbers
	for (ColumnBase* colBase : (*columns)) {
		if (colBase->getType() == ColumnBase::intType) {
			Column<int>* col = (Column<int>*) colBase;
			cout << col->getName() << " #distinct values = " << col->getDictionary()->size()<<"/"<<row << endl;
		}
		else {
			Column<string>* col = (Column<string>*) colBase;
			cout << col->getName() << " #distinct values = " << col->getDictionary()->size()<<"/"<<row << endl;
		}
	}

	// process columns of table
	table->processColumn(csn);
	// save checkpoint
	Logging* logging = new Logging();
	logging->saveCheckpoint(table);

	// loaded time
	cout << "Table Load time: " << float( clock () - begin_time ) /  CLOCKS_PER_SEC << " seconds " << endl;

	return table;
};

// UPDATE
string updateCommand(ServerSocket* client, Table* table, Transaction* transaction, Logging* logging,
		vector<string> command, GarbageCollector* garbage, size_t txIdx) {
	if (command.size() < 2) {
		return "ERROR: No row id field !";
	}
	int test = 0;
	try {
		test = stoi(command[1]);
	} catch(exception& e) {
		return "ERROR: row id must be a number !";
	}
	if (test < 1) {
		return "ERROR: row id must start from 1 !";
	}
	size_t rid = test - 1;	// rid start from 0
	// update value init
	int o_orderkey = -1;
	string o_orderstatus = "";
	int o_totalprice = -1;
	string o_comment = "";
	// create Transaction (if new command)
	if (txIdx == (size_t) -1) {
		txIdx = transaction->createTx();
		transaction->setClient(txIdx, client);
		transaction->setCommand(txIdx, command);
	}
	transaction->startTx(txIdx);
	logging->redoLogUpdate(txIdx, Logging::TX_START);
	uint64_t csn = transaction->getStartTimestamp(txIdx);
	// get update value from command and execute update
	if (command.size() >= 3) {
		o_orderkey = stoi(command[3-1]);
		Column<int>* col = (Column<int>*) table->getColumnByName("o_orderkey");
		if (col != NULL) {
			if (col->numOfRows() <= rid) {
				return "ERROR: row id excess number of rows !";
			}
			// check column's CSN with tx start time stamp
			if (col->getCSN(rid) <= csn) {
				col->addVersionVecValue(o_orderkey, csn, rid, txIdx, logging);
			}
			else
				transaction->addToWaitingList(txIdx);
		}
	}
	if (command.size() >= 4) {
		o_orderstatus = command[4-1];
		Column<string>* col = (Column<string>*) table->getColumnByName("o_orderstatus");
		if (col->numOfRows() <= rid) {
			return "ERROR: row id excess number of rows !";
		}
		// check column's CSN with tx start time stamp
		if (col->getCSN(rid) <= csn) {
			col->addVersionVecValue(o_orderstatus, csn, rid, txIdx, logging);
		}
		else
			transaction->addToWaitingList(txIdx);
	}
	if (command.size() >= 5) {
		o_totalprice = stoi(command[5-1]);
		Column<int>* col = (Column<int>*) table->getColumnByName("o_totalprice");
		if (col->numOfRows() <= rid) {
			return "ERROR: row id excess number of rows !";
		}
		// check column's CSN with tx start time stamp
		if (col->getCSN(rid) <= csn) {
			col->addVersionVecValue(o_totalprice, csn, rid, txIdx, logging);
		}
		else
			transaction->addToWaitingList(txIdx);
	}
	if (command.size() >= 6) {
		o_comment = command[6-1];
		Column<string>* col = (Column<string>*) table->getColumnByName("o_comment");
		if (col->numOfRows() <= rid) {
			return "ERROR: row id excess number of rows !";
		}
		// check column's CSN with tx start time stamp
		if (col->getCSN(rid) <= csn) {
			col->addVersionVecValue(o_comment, csn, rid, txIdx, logging);
		}
		else
			transaction->addToWaitingList(txIdx);
	}
	bool abortable = (command[0] == "UPDATE_WITH_ABORT");
	if (abortable) {
		return "Transaction Id = " + to_string(txIdx);
	}
	// commit Transaction (if not in waiting list)
	if (transaction->getTransaction(txIdx).status != Transaction::TRANSACTION_STATUS::WAITING) {
		// save redo log to public log buffer
		logging->redoLogUpdate(txIdx, Logging::TX_COMMIT);
		// commit
		transaction->commitTx(txIdx, csn);
		logging->redoLogUpdate(txIdx, Logging::TX_END);
		logging->redoLogPublicMerge();
		// add to Garbage Collector
		vector<size_t> updateRids;
		updateRids.push_back(rid);
		garbage->updateRecentlyUpdateRids(updateRids);

		return "Updated 1 row with rid = " + to_string(rid + 1);
	}
	return "WAITING";
}

// INSERT
string insertCommand(Table* table, Transaction* transaction, Logging* logging, vector<string> command) {
	if (command.size() < 5) {
		cout << "ERROR: Not enough 4 fields to INSERT !" << endl;
		return "ERROR: Not enough 4 fields to INSERT !";
	}
	// insert value init
	int o_orderkey = -1;
	string o_orderstatus = "";
	int o_totalprice = -1;
	string o_comment = "";
	// create Transaction
	size_t txIdx = transaction->createTx();
	// get insert value from command and execute insert
	transaction->startTx(txIdx);
	logging->redoLogUpdate(txIdx, Logging::TX_START);
	uint64_t csn = transaction->getTimestampAsCSN();
	size_t numOfRow = 0;
	if (command.size() >= 2) {
		o_orderkey = stoi(command[2-1]);
		Column<int>* col = (Column<int>*) table->getColumnByName("o_orderkey");
		col->addVersionVecValue(o_orderkey, csn, -1, txIdx, logging);
		numOfRow = col->numOfRows();
	}
	if (command.size() >= 3) {
		o_orderstatus = command[3-1];
		Column<string>* col = (Column<string>*) table->getColumnByName("o_orderstatus");
		col->addVersionVecValue(o_orderstatus, csn, -1, txIdx, logging);
		numOfRow = col->numOfRows();
	}
	if (command.size() >= 4) {
		o_totalprice = stoi(command[4-1]);
		Column<int>* col = (Column<int>*) table->getColumnByName("o_totalprice");
		col->addVersionVecValue(o_totalprice, csn, -1, txIdx, logging);
		numOfRow = col->numOfRows();
	}
	if (command.size() >= 5) {
		o_comment = command[5-1];
		Column<string>* col = (Column<string>*) table->getColumnByName("o_comment");
		col->addVersionVecValue(o_comment, csn, -1, txIdx, logging);
		numOfRow = col->numOfRows();
	}
	bool abortable = (command[0] == "UPDATE_WITH_ABORT");
	if (abortable) {
		return "Transaction ID = " + to_string(txIdx);
	}
	// redo log
	logging->redoLogUpdate(txIdx, Logging::TX_COMMIT);
	// commit Transaction
	transaction->commitTx(txIdx, csn);
	logging->redoLogUpdate(txIdx, Logging::TX_END);
	logging->redoLogPublicMerge();
	cout << "done" << endl;

	return "INSERTED 1 row ! Number of rows is: " + to_string(numOfRow);
}

// SCAN
string scanCommand(Table* table, Transaction* transaction, vector<string> command) {
	if (command.size() < 4) {
		cout << "SCAN command must have at least 4 fields: SCAN|filter value|filter column|filter operator !" << endl;
		return "SCAN command must have at least 4 fields: SCAN|filter value|filter column|filter operator !";
	}
	string filterValue1 = command[1];
	string column1 = command[2];
	string operator1 = command[3];

	clock_t begin_time = clock();
	// create Transaction
	size_t txIdx = transaction->createTx();
	transaction->startTx(txIdx);
	uint64_t txTs = transaction->getStartTimestamp(txIdx);
	// select with filter value 1
	vector<bool>* q_resultRid = new vector<bool>();
	ColumnBase* colBase = table->getColumnByName(column1);
	if (colBase->getType() == ColumnBase::intType) {
		Column<int>* col = (Column<int>*) colBase;
		int searchValue = stoi(filterValue1);
		col->selection(searchValue, ColumnBase::sToOp(operator1), q_resultRid);
	}
	else if (colBase->getType() == ColumnBase::charType) {
		Column<string>* col = (Column<string>*) colBase;
		string searchValue = filterValue1;
		col->selection(searchValue, ColumnBase::sToOp(operator1), q_resultRid);
	}
	// filter 2
	if (command.size() >= 7) {
		string filterValue2 = command[4];
		string column2 = command[5];
		string operator2 = command[6];
		// select with filter value 2
		ColumnBase* colBase = table->getColumnByName(column2);
		bool initResultRid = false;
		if (colBase->getType() == ColumnBase::intType) {
			Column<int>* col = (Column<int>*) colBase;
			int searchValue = stoi(filterValue2);
			col->selection(searchValue, ColumnBase::sToOp(operator2), q_resultRid, initResultRid);
		}
		else if (colBase->getType() == ColumnBase::charType) {
			Column<string>* col = (Column<string>*) colBase;
			string searchValue = filterValue2;
			col->selection(searchValue, ColumnBase::sToOp(operator2), q_resultRid, initResultRid);
		}
	}

	// update list row id for Transaction
	vector<size_t> vecRowid;
	for (size_t i = 0; i < q_resultRid->size(); i++) {
		if (q_resultRid->at(i))
			vecRowid.push_back(i);
	}
	transaction->updateRid2Transaction(txIdx, vecRowid);
	// scan all columns with version space to get result
	//cout << "********* Print scan result ************" << endl;
	size_t totalresult = 0;
	for (size_t i = 0; i < q_resultRid->size(); i++) {
		if (q_resultRid->at(i))
			++totalresult;
	}
	size_t limit = 10;
	size_t limitCount = 0;
	vector<string> q_select_fields;
	for (ColumnBase* colBase : *table->columns()) {
		q_select_fields.push_back(colBase->getName());
	}
	vector<string> outputs (limit + 2);
	for (size_t idx = 0; idx < q_select_fields.size(); idx++) {
		string select_field_name = q_select_fields[idx];
		outputs[0] += select_field_name + ", ";
		ColumnBase* colBase = (ColumnBase*) table->getColumnByName(select_field_name);
		if (colBase == NULL) continue;
		if (colBase->getType() == ColumnBase::intType) {
			Column<int>* t = (Column<int>*) colBase;
			vector<int> tmpOut = t->projectionWithVersion(q_resultRid, txTs, limit, limitCount);
			for (size_t i = 0; i < tmpOut.size(); i++) {
				outputs[i+1] += to_string(tmpOut[i]) + ",   ";
				// padding whitespace
				for (int j = 11 - (outputs[i+1].length()); j > 0; j--) {
					outputs[i+1] += " ";
				}
			}
		}
		else {
			Column<string>* t = (Column<string>*) colBase;
			vector<string> tmpOut = t->projectionWithVersion(q_resultRid, txTs, limit, limitCount);
			for (size_t i = 0; i < tmpOut.size(); i++) {
				outputs[i+1] += "\"" + tmpOut[i] + "\"" + ",   ";
			}
		}
	}
	// commit transaction
	transaction->commitTx(txIdx, transaction->getTimestampAsCSN());
	// show result
	std::stringstream showing;
	if (limitCount >= limit) {
		showing << "Showing "<<limit << "/" << totalresult <<" results !";
	}
	else if (limitCount == 0) {
		showing << "No result found !";
	}
	else {
		showing << "Showing " << limitCount << "/" << totalresult <<" results !";
	}
	outputs.push_back(showing.str());
	std::stringstream returnResult;
	for (string output : outputs) {
		if (!output.empty()) {
			//cout << output << endl;
			returnResult << output << "\n";
		}
	}
	returnResult << "Query time: " << float(clock() - begin_time)/CLOCKS_PER_SEC << " seconds " << endl;
	return returnResult.str();
}


int main_test(void) {
	puts("***** Simple Column-Store Database start ******");

	string createQuery = "create table orders (o_orderkey integer, o_orderstatus text, o_totalprice integer, o_comment text)";
	//cout << "Enter create table statement: ";
	//getline(cin, createQuery);
	hsql::SQLParserResult* pCreateQuery = hsql::SQLParser::parseSQLString(createQuery);
	string tableName;
	vector<string> cColumnName;
	vector<ColumnBase::COLUMN_TYPE> cColumnType;
	if (pCreateQuery->isValid) {
		hsql::SQLStatement* stmt = pCreateQuery->getStatement(0);
		if (stmt->type() == hsql::StatementType::kStmtCreate) {
			hsql::CreateStatement* createStmt = (hsql::CreateStatement*) stmt;
			tableName = createStmt->tableName;
			vector<hsql::ColumnDefinition*>* cols = createStmt->columns;
			for (hsql::ColumnDefinition* colDef : *cols) {
				cColumnName.push_back(colDef->name);
				switch (colDef->type) {
					case hsql::ColumnDefinition::INT:
					case hsql::ColumnDefinition::DOUBLE:
						cColumnType.push_back(ColumnBase::intType);
						break;
					case hsql::ColumnDefinition::TEXT:
						cColumnType.push_back(ColumnBase::charType);
						break;
				}
			}
		}
	}
	else {
		cout << "Create table statement is Invalid !" << endl;
		return -1;
	}
	cout << "tablename: " << tableName << endl;
	// init table
	vector<ColumnBase*>* columns = new vector<ColumnBase*>();
	Table* table = new Table(columns);
	table->setName(tableName);
	// init column
	for (size_t i = 0; i < cColumnName.size(); i++) {
		string name = cColumnName[i];
		ColumnBase::COLUMN_TYPE type = cColumnType[i];
		//cout << "colname: " << name << endl;
		//cout << "col type: " << type << endl;
		// create new column
		ColumnBase* colBase = new ColumnBase();
		if (type == ColumnBase::intType) {
			Column<int>* col = new Column<int>();
			colBase = col;
		}
		else {
			Column<string>* col = new Column<string>();
			colBase = col;
		}
		colBase->setName(name);
		colBase->setType(type);
		//if (name == "o_orderkey") colBase->setPrimaryKey(true);
		//if (name == "o_comment") colBase->setCreateInvertedIndex(true);
		columns->push_back(colBase);
	}

	// calculate time execution
	clock_t begin_time = clock();

	// read file
	//cout << "Enter file path: ";
	string filePath;
	//getline(cin, filePath);
	//ifstream infile(filePath);
	ifstream infile("/home/duclv/homework/data1M.csv");
	string line;
	string delim = ",";
	int row = 0;
	while(getline(infile, line)) {
		size_t last = 0; size_t next = 0;
		char idx = 0;
		vector<string> items;	// split items
		string lastItem;
		string token;
		while ((next = line.find(delim, last)) != string::npos) {
		    token = line.substr(last, next - last);
		    last = next + delim.length();
		    if (idx >= table->numOfColumns() - 1)
		    	lastItem += token + delim;
		    else
		    	items.push_back(token);
		    ++idx;
		}
		// get the last token and add to last item
		token = line.substr(last);
		lastItem += token;
		items.push_back(lastItem);

		// process input data based on column type
		for (int i = 0; i < table->numOfColumns(); i++) {
			string item = items[i];
			ColumnBase* colBase = columns->at(i);
			if (colBase->getType() == ColumnBase::intType) {
				int intValue = stoi(item);
				bool sorted = true;
				// update dictionary
				Column<int>* col = (Column<int>*) colBase;
				col->updateDictionary(intValue, sorted);
			}
			else {
				// char or varchar type
				boost::replace_all(item, "\"", "");
				bool sorted = false;
				// update dictionary
				Column<string>* col = (Column<string>*) colBase;
				col->updateDictionary(item, sorted);
			}
		}
		++row;
	}
	infile.close();
	// print distinct numbers
	for (ColumnBase* colBase : (*columns)) {
		if (colBase->getType() == ColumnBase::intType) {
			Column<int>* col = (Column<int>*) colBase;
			cout << col->getName() << " #distinct values = " << col->getDictionary()->size()<<"/"<<row << endl;
		}
		else {
			Column<string>* col = (Column<string>*) colBase;
			cout << col->getName() << " #distinct values = " << col->getDictionary()->size()<<"/"<<row << endl;
		}
	}

	// process columns of table
	table->processColumn();

	// print result
	//col1->printVecValue(10);
	//col2->printVecValue(10);
	//((Column<string>*)columns[3])->getDictionary()->print(10);

	// loaded time
	cout << "Table Load time: " << float( clock () - begin_time ) /  CLOCKS_PER_SEC << " seconds " << endl;

	/*
	 * Load table 2: lineitem
	 */
	begin_time = clock();
	// init table 2
	vector<ColumnBase*>* columns2 = new vector<ColumnBase*>();
	Table* table2 = new Table(columns2);
	table2->setName("lineitem");
	cout << "table name: " << table2->getName() << endl;

	// Column 0
	Column<int>* col0 = new Column<int>();
	col0->setName("l_orderkey");
	col0->setType(ColumnBase::intType);
	col0->setSize(4);

	// Column 1
	Column<int>* col1 = new Column<int>();
	col1->setName("l_quantity");
	col1->setType(ColumnBase::intType);
	col1->setSize(4);

	// Column 2
	Column<string>* col2 = new Column<string>();
	col2->setName("l_returnflag");
	col2->setType(ColumnBase::charType);
	col2->setSize(1);

	// read data into column
	filePath = "/home/duclv/homework/lineitem1M.tbl";
	ifstream infile2(filePath);
	if (!infile2) {
		cout << "Cannot open file path: " << filePath << endl;
		return -1;
	}
	// process file
	row = 0;
	delim = "|";
	while(getline(infile2, line)) {
		size_t last = 0; size_t next = 0;
		char i = 0;
		string token;
		// read each token and add into column store
		while ((next = line.find(delim, last)) != string::npos) {
			token = line.substr(last, next - last);
			last = next + delim.length();
			i++;
			// orderkey is 1st column
			if (i == 1) {
				int orderkey = stoi(token);
				col0->updateDictionary(orderkey, true);
			}
			// quantity is 2nd column
			else if (i == 2) {
				int quantity = stoi(token);
				col1->updateDictionary(quantity, true);
			}
		}
		// get the last token
		token = line.substr(last);
		boost::replace_all(token, "\"", "");
		col2->updateDictionary(token, false);
		++row;
	}
	infile.close();

	// print distinct values
	cout << col0->getName() << " #distinct values = " << col0->getDictionary()->size()<<"/"<<row << endl;
	cout << col1->getName() << " #distinct values = " << col1->getDictionary()->size()<<"/"<<row << endl;
	cout << col2->getName() << " #distinct values = " << col2->getDictionary()->size()<<"/"<<row << endl;

	// add columns to table
	columns2->push_back(col0);
	columns2->push_back(col1);
	columns2->push_back(col2);
	table2->processColumn();

	cout << "Table 2 Load time: " << float( clock () - begin_time ) /  CLOCKS_PER_SEC << " seconds " << endl;

	// query
	while (true) {
		string query = "Select * from orders where o_totalprice < 56789 and o_totalprice > 5678";
		cout << "Enter a query or select join query example (join1/join2/join3): ";
		getline(cin, query);
		if ("quit" == query)
			break;

		// Join example
		if (query.find("join") != string::npos) {
			begin_time = clock();
			// join l_orderkey with o_orderkey
			Column<int>* l_orderkey = (Column<int>*) table2->getColumnByName("l_orderkey");
			Column<int>* o_orderkey = (Column<int>*) table->getColumnByName("o_orderkey");

			// initialize matching row ids
			vector<bool>* l_rowIds = new vector<bool>();
			vector<bool>* o_rowIds = new vector<bool>();

			// join example 2: orders.o_totalprice < 56789 AND l_quantity > 40
			if (query.find("2") != string::npos) {
				Column<int>* o_totalprice = (Column<int>*) table->getColumnByName("o_totalprice");
				Column<int>* l_quantity = (Column<int>*) table2->getColumnByName("l_quantity");
				// execute where query
				int value = 56789;
				o_totalprice->selection(value, ColumnBase::ltOp, o_rowIds);
				value = 40;
				l_quantity->selection(value, ColumnBase::gtOp, l_rowIds);
			}
			// join example 3: orders.o_comment contains ‘gift’
			else if (query.find("3") != string::npos) {
				Column<string>* o_comment = (Column<string>*) table->getColumnByName("o_comment");
				// execute where query
				string value = "gift";
				o_comment->selection(value, ColumnBase::containOp, o_rowIds);
			}
			// join example 1: no where selection
			else {
				// all rows of 2 joining tables are selected
				for (size_t i = 0; i < l_rowIds->size(); i++) {
					l_rowIds->at(i) = true;
				}
				for (size_t i = 0;i < o_rowIds->size(); i++) {
					o_rowIds->at(i) = true;
				}
			}

			// process hash and probe
			if (Util::rowSelectedSize(l_rowIds) >= Util::rowSelectedSize(o_rowIds)) {
				// create mapping between vecValue of 2 join columns
				map<size_t, size_t> mappingValueId;
				for (size_t i = 0; i < l_orderkey->getDictionary()->size(); i++) {
					size_t valueId1 = i;
					int* dictValue1 = l_orderkey->getDictionary()->lookup(valueId1);
					if (dictValue1 != NULL) {
						// search dictionary value on 2nd column
						vector<size_t> result;
						o_orderkey->getDictionary()->search(*dictValue1, ColumnBase::equalOp, result);
						// if not existed valueId2 then return -1
						size_t valueId2 = result[0];
						mappingValueId[valueId1] = valueId2;
					}
				}

				// build hashmap for smaller column
				map<size_t, vector<size_t>> hashmap;
				o_orderkey->buildHashmap(hashmap, o_rowIds);

				// probe (join) to find matching row ids
				for (size_t rowId = 0; rowId < l_orderkey->vecValueSize(); rowId++) {
					size_t valueId1 = l_orderkey->vecValueAt(rowId);
					// get valueId2 from mapping
					size_t valueId2 = mappingValueId[valueId1];
					// found on hashmap
					vector<size_t> rowIds = hashmap[valueId2];
					if (rowIds.size() > 0) {
						// keep row id
						l_rowIds->at(rowId) = true;
						for (size_t o_rowId : rowIds) {
							o_rowIds->at(o_rowId) = true;
						}
					}
				}
			}
			else {
				// create mapping between vecValue of 2 join columns
				map<size_t, size_t> mappingValueId;
				for (size_t i = 0; i < o_orderkey->getDictionary()->size(); i++) {
					size_t valueId1 = i;
					int* dictValue1 = o_orderkey->getDictionary()->lookup(valueId1);
					if (dictValue1 != NULL) {
						// search dictionary value on 2nd column
						vector<size_t> result;
						l_orderkey->getDictionary()->search(*dictValue1, ColumnBase::equalOp, result);
						// if not existed valueId2 then return -1
						size_t valueId2 = result[0];
						mappingValueId[valueId1] = valueId2;
					}
				}

				// build hashmap for smaller column
				map<size_t, vector<size_t>> hashmap;
				l_orderkey->buildHashmap(hashmap, l_rowIds);

				// probe (join) to find matching row ids
				for (size_t rowId = 0; rowId < o_orderkey->vecValueSize(); rowId++) {
					size_t valueId1 = o_orderkey->vecValueAt(rowId);
					// get valueId2 from mapping
					size_t valueId2 = mappingValueId[valueId1];
					// found on hashmap
					vector<size_t> rowIds = hashmap[valueId2];
					if (rowIds.size() > 0) {
						// keep row id
						o_rowIds->at(rowId) = true;
						for (size_t l_rowId : rowIds)
							l_rowIds->at(l_rowId) = true;
					}
				}
			}

			// print the result based on matching row ids
			cout << "********* Print join result ************" << endl;
			size_t l_totalresult = 0;
			for (size_t i = 0; i < l_rowIds->size(); i++) {
				if (l_rowIds->at(i))
					++l_totalresult;
			}
			size_t o_totalresult = 0;
			for (size_t i = 0; i < o_rowIds->size(); i++) {
				if (o_rowIds->at(i))
					++o_totalresult;
			}
			size_t limit = 10;
			size_t limitCount = 0;
			vector<string> q_select_fields;
			for (ColumnBase* colBase : (*table->columns())) {
				q_select_fields.push_back(colBase->getName());
			}
			vector<string> outputs (limit + 1);
			for (size_t idx = 0; idx < q_select_fields.size(); idx++) {
				string select_field_name = q_select_fields[idx];
				outputs[0] += select_field_name + ", ";
				ColumnBase* colBase = (ColumnBase*) table->getColumnByName(select_field_name);
				if (colBase == NULL) continue;
				if (colBase->getType() == ColumnBase::intType) {
					Column<int>* t = (Column<int>*) colBase;
					vector<int> tmpOut = t->projection(o_rowIds, limit, limitCount);
					for (size_t i = 0; i < tmpOut.size(); i++) {
						outputs[i+1] += to_string(tmpOut[i]) + ",   ";
						// padding whitespace
						for (int j = 11 - (outputs[i+1].length()); j > 0; j--) {
							outputs[i+1] += " ";
						}
					}
				}
				else {
					Column<string>* t = (Column<string>*) colBase;
					vector<string> tmpOut = t->projection(o_rowIds, limit, limitCount);
					for (size_t i = 0; i < tmpOut.size(); i++) {
						outputs[i+1] += "\"" + tmpOut[i] + "\"" + ",   ";
					}
				}
			}
			for (string output : outputs) {
				if (!output.empty())
					cout << output << endl;
			}
			if (limitCount >= limit)
				cout << "Showing "<<limit<<"/"<<o_totalresult<<" results !" << endl;
			else if (limitCount == 0)
				cout << "No result found !" << endl;
			else
				cout << "Showing "<<limitCount<<"/"<<o_totalresult<<" results !" << endl;
			delete o_rowIds;
			//
			/*cout << "lineitem result:" << endl;
			vector<int> l_output = l_orderkey->projection(&l_rowIds, limit, limitCount);
			for (auto l : l_output) {
				cout << l << endl;
			}
			cout << "Print " << limit << "/" << l_totalresult << " results !" << endl;*/
			delete l_rowIds;
			std::cout << "Query time: " << float(clock() - begin_time)/CLOCKS_PER_SEC << " seconds " << endl;
			continue;
		}

		begin_time = clock();
		// parse a given query
		hsql::SQLParserResult* pResult = hsql::SQLParser::parseSQLString(query);
		// check whether the parsing was successfull
		bool queryValid = pResult->isValid;
		while (queryValid) {
			printf("Parsed successfully!\n");
			// process the statements...
			string q_table;
			vector<string> q_select_fields;
			vector<string> q_where_fields;
			vector<ColumnBase::OP_TYPE> q_where_ops;
			vector<string> q_where_values;

			hsql::SQLStatement* stmt = pResult->getStatement(0);
			if (stmt->type() == hsql::StatementType::kStmtSelect) {
				hsql::SelectStatement* select = (hsql::SelectStatement*) stmt;
				q_table = select->fromTable->getName();
				cout << "Table name: " << q_table << endl;
				if (q_table != table->getName()) {queryValid = false; break;}

				for (hsql::Expr* expr : *select->selectList) {
					if (expr->type == hsql::ExprType::kExprStar) {
						for (ColumnBase* colBase : (*table->columns())) {
							q_select_fields.push_back(colBase->getName());
						}
						/*q_select_fields.push_back("o_orderkey");
						q_select_fields.push_back("o_orderstatus");
						q_select_fields.push_back("o_totalprice");
						q_select_fields.push_back("o_comment");*/
					}
					else if (expr->type == hsql::ExprType::kExprColumnRef)
						q_select_fields.push_back(expr->name);
					else if (expr->type == hsql::ExprType::kExprFunctionRef) {
						q_select_fields.push_back(string(expr->name) + string("#") + string(expr->expr->name));
					}
				}
				for (size_t i = 0; i < q_select_fields.size(); i++) {
					cout << "select fields[" << i << "] = " << q_select_fields[i] << endl;
				}

				if (select->whereClause != NULL) {
					hsql::Expr* expr = select->whereClause;
					if (expr->type == hsql::ExprType::kExprOperator) {
						if (expr->op_type == hsql::Expr::OperatorType::SIMPLE_OP) {
							q_where_fields.push_back(expr->expr->name);

							if (expr->op_char == '>')
								q_where_ops.push_back(ColumnBase::OP_TYPE::gtOp);
							else if (expr->op_char == '<')
								q_where_ops.push_back(ColumnBase::OP_TYPE::ltOp);
							else if (expr->op_char == '=')
								q_where_ops.push_back(ColumnBase::OP_TYPE::equalOp);

							hsql::ExprType literalType = expr->expr2->type;
							if (literalType == hsql::ExprType::kExprLiteralInt)
								q_where_values.push_back(to_string(expr->expr2->ival));
							else if (literalType == hsql::ExprType::kExprColumnRef)
								q_where_values.push_back(expr->expr2->name);
						}
						else if (expr->op_type == hsql::Expr::OperatorType::LIKE) {
							q_where_fields.push_back(expr->expr->name);

							q_where_ops.push_back(ColumnBase::OP_TYPE::containOp);

							hsql::ExprType literalType = expr->expr2->type;
							if (literalType == hsql::ExprType::kExprLiteralInt)
								q_where_values.push_back(to_string(expr->expr2->ival));
							else if (literalType == hsql::ExprType::kExprColumnRef)
								q_where_values.push_back(expr->expr2->name);
						}
						else if (expr->op_type == hsql::Expr::OperatorType::AND) {
							hsql::Expr* expr1 = expr->expr;
							hsql::Expr* expr2 = expr->expr2;
							if (expr1->op_type == hsql::Expr::OperatorType::SIMPLE_OP) {
								q_where_fields.push_back(expr1->expr->name);

								if (expr1->op_char == '>')
									q_where_ops.push_back(ColumnBase::OP_TYPE::gtOp);
								else if (expr1->op_char == '<')
									q_where_ops.push_back(ColumnBase::OP_TYPE::ltOp);
								else if (expr1->op_char == '=')
									q_where_ops.push_back(ColumnBase::OP_TYPE::equalOp);

								if (expr1->expr2->type == hsql::ExprType::kExprLiteralInt)
									q_where_values.push_back(to_string(expr1->expr2->ival));
								else if (expr1->expr2->type == hsql::ExprType::kExprColumnRef)
									q_where_values.push_back(expr1->expr2->name);
							}
							if (expr2->op_type == hsql::Expr::OperatorType::SIMPLE_OP) {
								q_where_fields.push_back(expr2->expr->name);

								if (expr2->op_char == '>')
									q_where_ops.push_back(ColumnBase::OP_TYPE::gtOp);
								else if (expr2->op_char == '<')
									q_where_ops.push_back(ColumnBase::OP_TYPE::ltOp);
								else if (expr2->op_char == '=')
									q_where_ops.push_back(ColumnBase::OP_TYPE::equalOp);

								if (expr2->expr2->type == hsql::ExprType::kExprLiteralInt)
									q_where_values.push_back(to_string(expr2->expr2->ival));
								else if (expr2->expr2->type == hsql::ExprType::kExprColumnRef)
									q_where_values.push_back(expr2->expr2->name);
							}
						}
						else {
							// not support
							queryValid = false;
							break;
						}
					}
					for (size_t i = 0; i < q_where_fields.size(); i++) {
						cout << "where fields[" << i << "] = " << q_where_fields[i] << endl;
					}
					for (size_t i = 0; i < q_where_ops.size(); i++) {
						cout << "where operators[" << i << "] = " << q_where_ops[i] << endl;
					}
					for (size_t i = 0; i < q_where_values.size(); i++) {
						cout << "where values[" << i << "] = " << q_where_values[i] << endl;
					}
				}
				// init query result row id
				vector<bool>* q_resultRid = new vector<bool>(); // contain query result row id
				// execute query
				vector<vector<size_t>> q_results;
				for (size_t fidx = 0; fidx < q_where_fields.size(); fidx++) {
					string q_where_field = q_where_fields[fidx];
					ColumnBase::OP_TYPE q_where_op = q_where_ops[fidx];
					string q_where_value = q_where_values[fidx];
					bool initResultRid = (fidx == 0);

					// get column by name then cast to appropriate column object based on type
					ColumnBase* colBase = table->getColumnByName(q_where_field);
					if (colBase == NULL) continue;
					switch (colBase->getType()) {
						case ColumnBase::COLUMN_TYPE::intType:
						{
							Column<int>* t = (Column<int>*) colBase;
							int searchValue = 0;
							try {
								searchValue = stoi(q_where_value);
							} catch (exception& e) {
								cerr << "Exception: " << e.what() << endl;
								break;
							}
							t->selection(searchValue, q_where_op, q_resultRid, initResultRid);
							break;
						}
						case ColumnBase::charType:
						case ColumnBase::varcharType:
						{
							Column<string>* t = (Column<string>*) colBase;
							string searchValue = q_where_value;
							t->selection(searchValue, q_where_op, q_resultRid, initResultRid);
							break;
						}
					}
				}
				size_t totalResult = 0;
				for (size_t rid = 0; rid < q_resultRid->size(); rid++) {
					if (q_resultRid->at(rid))
						++totalResult;
				}
				// print result
				cout << "*** Query result ***"<< endl;
				size_t limit = 10;
				size_t limitCount = 0;
				vector<string> outputs (limit + 1);
				for (size_t idx = 0; idx < q_select_fields.size(); idx++) {
					string select_field_name = q_select_fields[idx];
					outputs[0] += select_field_name + ", ";
					ColumnBase* colBase = (ColumnBase*) table->getColumnByName(select_field_name);
					if (colBase == NULL) continue;
					if (colBase->getType() == ColumnBase::intType) {
						Column<int>* t = (Column<int>*) colBase;
						vector<int> tmpOut = t->projection(q_resultRid, limit, limitCount);
						for (size_t i = 0; i < tmpOut.size(); i++) {
							outputs[i+1] += to_string(tmpOut[i]) + ",   ";
							// padding whitespace
							for (int j = 11 - (outputs[i+1].length()); j > 0; j--) {
								outputs[i+1] += " ";
							}
						}
					}
					else {
						Column<string>* t = (Column<string>*) colBase;
						vector<string> tmpOut = t->projection(q_resultRid, limit, limitCount);
						for (size_t i = 0; i < tmpOut.size(); i++) {
							outputs[i+1] += "\"" + tmpOut[i] + "\"" + ",   ";
						}
					}
				}
				for (string output : outputs) {
					if (!output.empty())
						cout << output << endl;
				}
				if (limitCount >= limit)
					cout << "Showing "<<limit<<"/"<<totalResult<<" results !" << endl;
				else if (limitCount == 0)
					cout << "No result found !" << endl;
				else
					cout << "Showing "<<limitCount<<"/"<<totalResult<<" results !" << endl;
				delete q_resultRid;
				// query time
				std::cout << "Table Selection time: " << float(clock() - begin_time)/CLOCKS_PER_SEC << " seconds " << endl;
				// Processe done !
				break;
			}
			else
				cout << "Please enter a SELECT query !!!" << endl;
		}
		if (!queryValid) {
			printf("The SQL query is invalid or not supported !!!\n");
		}
	}

	return EXIT_SUCCESS;
}

