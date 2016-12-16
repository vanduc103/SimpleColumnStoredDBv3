/*
 * Column.h
 *
 *  Created on: Sep 23, 2016
 *      Author: duclv
 */
#include <vector>

#ifndef COLUMN_H_
#define COLUMN_H_

#include <math.h>
#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <stdexcept>
#include <algorithm>
#include <cstdint>
#include "ColumnBase.h"
#include "Dictionary.h"
#include "PackedArray.h"
#include "Transaction.h"
#include "Logging.h"
#include <stdio.h>

namespace std {

template<typename T>
class Column: public ColumnBase {
private:
	// value vector for column
	vector<size_t>* vecValue;
	// bit packing
	PackedArray* packed;
	// dictionary vector for column
	Dictionary<T>* dictionary;
	// bulk insert ?
	bool bulkInsert = false;

	// DATA SPACE
	struct data_column {
		bool versionFlag;
		size_t encodedValueIdx;	// index to encodedValue in vecValue vector
		uint64_t csn;
	};
	vector<data_column>* dataColumn;

	// VERSION SPACE
	vector<size_t>* versionVecValue;
	struct version_column {
		size_t encodedValueIdx;	// index to encodedValue in versionVecValue vector
		uint64_t csn;
		version_column* next;
	};
	vector<version_column>* versionColumn;
	Dictionary<T>* deltaSpace;
	map<size_t, size_t>* hashtable;

	// Transaction
	Transaction tx;
public:
	Column() {
		dictionary = new Dictionary<T>();
		vecValue = new vector<size_t>();
		packed = new PackedArray();
		dataColumn = new vector<data_column>();
		versionVecValue = new vector<size_t>();
		versionColumn = new vector<version_column>();
		deltaSpace = new Dictionary<T>();
		hashtable = new map<size_t, size_t>();
	}
	virtual ~Column() {
		delete vecValue;
		delete dictionary;
		PackedArray_destroy(packed);
		delete dataColumn;
		delete versionVecValue;
		delete versionColumn;
		delete deltaSpace;
		delete hashtable;
	}

	vector<size_t>* getVecValue() {
		if (vecValue == NULL) {
			vecValue = new vector<size_t>();
		}
		vecValue->clear();
		for (int i = 0; i < packed->count; i++) {
			vecValue->push_back(PackedArray_get(packed, i));
		}
		return vecValue;
	}

	size_t vecValueSize() {
		return packed->count;
	}

	size_t numOfRows() {
		return dataColumn->size();
	}

	size_t vecValueAt(size_t index) {
		if (index < 0 || index >= packed->count) {
			return -1; // indicate no result
		}
		return PackedArray_get(packed, index);
	}

	void updateVecValueAt(size_t index, size_t value) {
		if (index < 0 || index >= packed->count) {
			return; // no update
		}
		// update by bit backing
		PackedArray_set(packed, index, value);
	}

	void printVecValue(int row) {
		vecValue = getVecValue();
		for (size_t i = 0; i < (*vecValue).size() && i < row; i++) {
			cout << "vecValue[" << i << "] = " << (*vecValue)[i] << "\n";
		}
		// free vecValue
		vecValue->resize(0);
	}

	void bitPackingVecValue() {
		// #bit to represent encode dictionary value
		size_t numOfBit = (size_t) ceil(log2((double) dictionary->size()));
		// init bit packing array
		packed = PackedArray_create(numOfBit, vecValue->size());

		for (size_t i = 0; i < vecValue->size(); i++) {
			size_t value = vecValue->at(i);
			PackedArray_set(packed, i, value);
		}
		// free vecValue
		vecValue->resize(0);
	}

	vector<size_t>* unpackingVecValue() {
		if (vecValue == NULL) {
			vecValue = new vector<size_t>();
		}
		vecValue->clear();
		for (int i = 0; i < packed->count; i++) {
			vecValue->push_back(PackedArray_get(packed, i));
		}
		return vecValue;
	}

	void updateVecValue(T& value) {
		// used for column has few duplicate values
		vecValue->push_back(value);
	}

	Dictionary<T>* getDictionary() {
		if (dictionary == NULL) {
			dictionary = new Dictionary<T>();
		}
		return dictionary;
	}

	// Update new value for dictionary
	void updateDictionary(T& value, bool sorted = true, bool bulkInsert = true,
			uint64_t csn = 0) {
		// no bulk insert if no sort
		if (!sorted)
			bulkInsert = false;
		this->bulkInsert = bulkInsert;

		dictionary->setSorted(sorted);
		dictionary->addNewElement(value, vecValue, sorted, bulkInsert);
		if (!bulkInsert) {
			// build dataColumn vector
			data_column data;
			data.encodedValueIdx = vecValue->size() - 1;
			data.csn = csn;
			data.versionFlag = false;
			dataColumn->push_back(data);
		}
	}

	bool isBulkInsert() {
		return bulkInsert;
	}
	void bulkBuildVecVector(uint64_t csn = 0) {
		// bulk insert -> update vecValue after building entire dictionary
		vecValue->resize(0);
		vector<T>* bulkVecValue = dictionary->getBulkVecValue();
		if (bulkVecValue != NULL) {
			for (size_t i = 0; i < bulkVecValue->size(); i++) {
				// find position of valueId in dictionary
				vector<size_t> result;
				dictionary->search(bulkVecValue->at(i), ColumnBase::equalOp,
						result);
				size_t pos = result[0];
				if (pos != -1)
					vecValue->push_back(pos);
			}
			// build dataColumn vector
			dataColumn->resize(0);
			for (size_t i = 0; i < vecValue->size(); i++) {
				data_column data;
				data.encodedValueIdx = i;
				data.csn = csn;
				data.versionFlag = false;
				dataColumn->push_back(data);
			}
		}
		bulkVecValue->resize(0);
	}

	void createInvertedIndex() {
		if (dictionary != NULL)
			dictionary->buildInvertedIndex();
	}

	bool selection(T& searchValue, ColumnBase::OP_TYPE q_where_op,
			vector<bool>* q_resultRid, bool initResultRid = true) {
		// init q_resultRid to all true
		if (initResultRid)
			for (size_t i = 0; i < numOfRows(); i++) {
				q_resultRid->push_back(true);
			}
		vector<size_t> result;
		this->getDictionary()->search(searchValue, q_where_op, result);

		// find rowId with appropriate dictionary position
		for (size_t rowId = 0; rowId < this->numOfRows(); rowId++) {
			size_t encodedValue = this->vecValueAt(rowId);
			if ((q_where_op != ColumnBase::containOp
					&& encodedValue >= result.front()
					&& encodedValue <= result.back())
					|| (q_where_op == ColumnBase::containOp
							&& binary_search(result.begin(), result.end(),
									encodedValue))) {
				// do nothing, keep q_resultRid true
			} else {
				// update to false -> not in result
				q_resultRid->at(rowId) = false;
			}
		}
		return true;
	}

	vector<T> projection(vector<bool>* q_resultRid, size_t limit,
			size_t& limitCount) {
		vector<T> outputs; // output result
		limitCount = 0; // reset limit count
		for (size_t rid = 0; rid < q_resultRid->size(); rid++) {
			if (q_resultRid->at(rid)) {
				size_t encodeValue = this->vecValueAt(rid);
				T* a = this->getDictionary()->lookup(encodeValue);
				outputs.push_back(*a);
				if (++limitCount >= limit)
					break;
			}
		}

		return outputs;
	}

	// Build hashmap of valueId based on selected row ids
	void buildHashmap(map<size_t, vector<size_t>>& hashmap,
			vector<bool>* vecRowId) {
		hashmap.clear();
		for (size_t rowId = 0; rowId < vecRowId->size(); rowId++) {
			// get valueId from bit packing if row id is selected
			// then build hashmap
			if (vecRowId->at(rowId)) {
				size_t valueId = vecValueAt(rowId);
				hashmap[valueId].push_back(rowId);
			}
		}
	}

	// Return vector of matching row ids
	vector<size_t> probe(map<size_t, vector<size_t>>* hashmap,
			size_t probedValue) {
		if (hashmap != NULL) {
			try {
				return hashmap->at(probedValue);
			} catch (exception& e) {
				// empty vector
				return vector<size_t>();
			}
		}
		return vector<size_t>();
	}

	// DATA SPACE
	void insertDataVecValue(T&value, uint64_t csn) {
		// uncompress vecValue vector from bit packing
		vecValue = unpackingVecValue();
		// add new value to dictionary
		bool sorted = this->getType() == ColumnBase::intType;
		dictionary->addNewElement(value, vecValue, sorted, false);
		// get index of new insert value to vecValue vector
		size_t newInsertVecValueIdx = vecValue->size() - 1;
		// bit packing vecValue again
		bitPackingVecValue();
		// create new data space value
		data_column newData;
		newData.encodedValueIdx = newInsertVecValueIdx;
		newData.csn = csn;
		newData.versionFlag = false;
		dataColumn->push_back(newData);
	}

	uint64_t getCSN(size_t rid) {
		try {
			return dataColumn->at(rid).csn;
		} catch (out_of_range& e) {
			return 0;
		}
	}

	void setCSN(size_t rid, bool setMaximum = true) {
		try {
			data_column data = dataColumn->at(rid);
			if (setMaximum)
				data.csn = UINT64_MAX;
			dataColumn->at(rid) = data;
		} catch (out_of_range& e) {
			// nothing
		}
	}

	// VERSION SPACE
	void addVersionVecValue(T& value, uint64_t csn, int rid, size_t txIdx, Logging* logging) {
		// set maximum csn so that another transaction cannot update
		if (rid != -1)
			this->setCSN(rid, true);
		// logging column start
		logging->redoLogUpdate(txIdx, Logging::COLUMN_START, this->getName());
		// add to delta space and version vector (start from last dictionary position)
		bool sorted = dictionary->getSorted();
		deltaSpace->addNewElement(value, versionVecValue, sorted, false);
		// get index of encodedValue in versionVecValue
		size_t encodedValueIdx = versionVecValue->size() - 1;
		// create new version
		version_column newVersion;
		newVersion.encodedValueIdx = encodedValueIdx;
		newVersion.next = NULL;
		newVersion.csn = csn;
		// rid = -1: INSERT case
		vector<string>* insertLog = new vector<string>();
		if (rid == -1) {
			// add new in data column
			data_column newData;
			dataColumn->push_back(newData);
			rid = dataColumn->size() - 1;
			// redo log for Insert case
			insertLog->push_back(to_string(-1));
			logging->redoLogAdd(txIdx, Logging::INSERT, *insertLog);
		}
		// check previous version on hash table
		int preVersionIdx = -1;
		try {
			preVersionIdx = hashtable->at(rid);
		} catch (out_of_range& e) {
			// not existed on hash table, keep -1
			preVersionIdx = -1;
		}
		if (preVersionIdx >= 0) {
			version_column preVersion = versionColumn->at(preVersionIdx);
			// point the next pointer of new created version to previous version
			newVersion.next = &preVersion;
			// replace the previous version on Version space vector by new version
			versionColumn->at(preVersionIdx) = newVersion;
		} else {
			// add new version to Version space vector
			versionColumn->push_back(newVersion);
			// create a new entry for rid on Hash table
			(*hashtable)[rid] = versionColumn->size() - 1;
		}
		// update version_flag on DATA space
		data_column dataValue = dataColumn->at(rid);
		dataValue.versionFlag = true;
		dataColumn->at(rid) = dataValue;

		// create redo log
		// log delta space
		vector<string>* deltaSpaceLog = new vector<string>();
		deltaSpace->redoLogCreate(deltaSpaceLog);
		logging->redoLogAdd(txIdx, Logging::DELTA_SPACE, *deltaSpaceLog);
		delete deltaSpaceLog;
		// log version vecValue
		vector<string>* versionVecValueLog = new vector<string>();
		for (size_t i = 0; i < versionVecValue->size(); i++) {
			size_t value = versionVecValue->at(i);
			versionVecValueLog->push_back(to_string(value));
		}
		logging->redoLogAdd(txIdx, Logging::VERSION_VECVALUE, *versionVecValueLog);
		// log versionColumn
		vector<string>* versionColumnLog = new vector<string>();
		versionColumnLog->push_back(to_string(encodedValueIdx));
		versionColumnLog->push_back(to_string(csn));
		logging->redoLogAdd(txIdx, Logging::VERSION_COLUMN, *versionColumnLog);
		// log hashtable
		vector<string>* hashtableLog = new vector<string>();
		hashtableLog->push_back(to_string(rid));
		logging->redoLogAdd(txIdx, Logging::HASHTABLE, *hashtableLog);
		// logging column end
		logging->redoLogUpdate(txIdx, Logging::COLUMN_END, this->getName());
	}

	void redoLogRestore(vector<string>* deltaSpaceLog, vector<string>* versionVecValueLog,
						vector<string>* insertLog,
						vector<string>* versionColumnLog, vector<string>* hashtableLog) {
		// redo for delta space
		this->deltaSpace->redoLogRestore(deltaSpaceLog);
		// redo for versionVecValue
		this->versionVecValue->clear();
		for(size_t i = 0; i < versionVecValueLog->size(); i++) {
			string value = versionVecValueLog->at(i);
			versionVecValue->push_back(stol(value));
		}
		delete versionVecValueLog;
		// redo for INSERT case
		if (insertLog != NULL && insertLog->size() >= 1) {
			// add new in data column
			data_column newData;
			dataColumn->push_back(newData);
		}
		delete insertLog;
		// get rid from hashtable log
		size_t rid = -1;
		if (hashtableLog->size() >= 1) {
			rid = stol(hashtableLog->at(0));
		}
		else {
			cout << "hashtable redo log error !";
			return;
		}
		delete hashtableLog;
		// redo for versionColumn
		size_t encodedValueIdx = -1;
		uint64_t csn = 0;
		if (versionColumnLog->size() >= 2){
			encodedValueIdx = stol(versionColumnLog->at(0));
			csn = stol(versionColumnLog->at(1));
		}
		else {
			cout << "versionColumn redo log error !";
			return;
		}
		delete versionColumnLog;
		// create new version
		version_column newVersion;
		newVersion.encodedValueIdx = encodedValueIdx;
		newVersion.next = NULL;
		newVersion.csn = csn;
		// check previous version on hash table
		int preVersionIdx = -1;
		try {
			preVersionIdx = hashtable->at(rid);
		} catch (out_of_range& e) {
			// not existed on hash table, keep -1
			preVersionIdx = -1;
		}
		if (preVersionIdx >= 0) {
			version_column preVersion = versionColumn->at(preVersionIdx);
			// point the next pointer of new created version to previous version
			newVersion.next = &preVersion;
			// replace the previous version on Version space vector by new version
			versionColumn->at(preVersionIdx) = newVersion;
		} else {
			// add new version to Version space vector
			versionColumn->push_back(newVersion);
			// create a new entry for rid on Hash table
			(*hashtable)[rid] = versionColumn->size() - 1;
		}
		// update version_flag on DATA space
		data_column dataValue = dataColumn->at(rid);
		dataValue.versionFlag = true;
		dataColumn->at(rid) = dataValue;
	}

	vector<T> projectionWithVersion(vector<bool>* q_resultRid, uint64_t txTs,
			size_t limit, size_t& limitCount) {
		vector<T> outputs; // output result
		limitCount = 0; // reset limit count
		for (size_t rid = 0; rid < q_resultRid->size(); rid++) {
			if (q_resultRid->at(rid)) {
				// get data at rid
				data_column data = dataColumn->at(rid);
				// if has no version -> get value from data space
				if (!data.versionFlag) {
					// check txTs >= CSN
					if (txTs >= data.csn) {
						size_t vecValueIdx = data.encodedValueIdx;
						size_t dictIdx = this->vecValueAt(vecValueIdx);
						T* a = this->getDictionary()->lookup(dictIdx);
						if (a != NULL) {
							outputs.push_back(*a);
						}
					}
				}
				// has version-> get value from version space
				else {
					// get version index from hashtable
					int versionIdx = -1;
					try {
						versionIdx = hashtable->at(rid);
					} catch (exception& e) {
						// not existed rid in hashtable, do nothing
					}
					// get version from Version space
					if (versionIdx >= 0 && versionIdx < versionColumn->size()) {
						version_column versionData = versionColumn->at(
								versionIdx);
						// traverse all versions from newest to oldest to find version with CSN <= txTs
						while (versionData.csn > txTs
								&& versionData.next != NULL) {
							versionData = *versionData.next;
						}
						if (txTs >= versionData.csn) {
							size_t versionVecValueIdx = versionData.encodedValueIdx;
							size_t dictIdx = this->versionVecValue->at(
									versionVecValueIdx);
							// lookup in Dictionary or Delta space
							T* a = deltaSpace->lookup(dictIdx);
							if (a != NULL) {
								outputs.push_back(*a);
							}
						}
					}
				}
				// get maximum limitCount result
				if (++limitCount >= limit)
					break;
			}
		}

		return outputs;
	}

	void updateVersionSpace2DataSpace(size_t rid) {
		try {
			// get latest version of rid from hash table
			size_t versionIdx = hashtable->at(rid);
			version_column lastestVersion = versionColumn->at(versionIdx);
			size_t encodedValue = versionVecValue->at(lastestVersion.encodedValueIdx);
			// get dictionary value from delta space
			T* a = deltaSpace->lookup(encodedValue);
			// update this value into column's dictionary
			vecValue = unpackingVecValue();
			size_t newEncodedValue = dictionary->addNewElement(*a, vecValue, dictionary->getSorted(), false);
			vecValue->pop_back();	// remove last item
			bitPackingVecValue();
			// get data at rid
			data_column dataAtRid = dataColumn->at(rid);
			// update vecValue and data at rid
			updateVecValueAt(rid, newEncodedValue);
			dataAtRid.csn = lastestVersion.csn;
			dataAtRid.encodedValueIdx = rid;	// index to vecValue
			dataAtRid.versionFlag = true;
			dataColumn->at(rid) = dataAtRid;
		} catch (exception& e) {
			//do nothing
		}
	}

	void removeOldVersion(size_t rid, uint64_t txStartTs) {
		try {
			// get latest version of rid from hashtable
			size_t versionIdx = hashtable->at(rid);
			version_column version = versionColumn->at(versionIdx);
			// check if history version has csn < txStartTs then remove
			version_column* curVersion = &version;
			while (version.csn >= txStartTs && version.next != NULL) {
				curVersion = &version;
				version = *version.next;
			}
			// remove old version
			if (version.csn < txStartTs) {
				vector<version_column*> vecOld;
				while (version.next != NULL) {
					vecOld.push_back(version.next);
				}
				for (version_column* old : vecOld) {
					delete old;
				}
				curVersion->next = NULL;
			}
		} catch (exception& e) {
			//do nothing
		}
	}

	// save to disk for checkpoint
	vector<string> saveToDisk(string logPath) {
		// save column basic information
		string fileColToSave = logPath + "/column_" + to_string(Util::currentMilisecond());
		vector<string>* contentColToSave = new vector<string>();
		contentColToSave->push_back(this->getName());
		contentColToSave->push_back(this->typeToString(this->getType()));
		Util::saveToDisk(contentColToSave, fileColToSave);
		// save dictionary
		string fileDictToSave = this->dictionary->saveToDisk(logPath);
		// save vecValue
		vecValue = unpackingVecValue();
		string vecValueFileToSave = logPath + "/vecValue_" + to_string(Util::currentMilisecond());
		vector<string>* vecValueToSave = new vector<string>();
		for (size_t v : (*vecValue)) {
			vecValueToSave->push_back(to_string(v));
		}
		vecValue->resize(0);
		Util::saveToDisk(vecValueToSave, vecValueFileToSave);
		// save dataColumn
		string dataColumnFileName = logPath + "/dataColumn_" + to_string(Util::currentMilisecond());
		vector<string>* dataColumnToSave = new vector<string>();
		for (data_column data : (*this->dataColumn)) {
			dataColumnToSave->push_back(to_string(data.csn));
			dataColumnToSave->push_back(to_string(data.encodedValueIdx));
			dataColumnToSave->push_back(to_string(data.versionFlag));
		}
		Util::saveToDisk(dataColumnToSave, dataColumnFileName);
		// return saved files
		vector<string> fileToSave;
		fileToSave.push_back(fileColToSave);
		fileToSave.push_back(fileDictToSave);
		fileToSave.push_back(vecValueFileToSave);
		return fileToSave;
	}

	// restore from checkpoint
	void restore(string colFile, string dictFile, string vecValueFile, string dataColumnFile) {
		// restore column
		vector<string>* contentCol = new vector<string>();
		Util::readFromDisk(contentCol, colFile);
		if (contentCol->size() == 2) {
			this->setName(contentCol->at(0));
			this->setType(this->stringToType(contentCol->at(1)));
		}
		delete contentCol;
		// restore dictionary
		this->dictionary->restore(dictFile);
		// restore vecValue
		vector<string>* contentVecValue = new vector<string>();
		Util::readFromDisk(contentVecValue, vecValueFile);
		for (size_t i = 0; i < contentVecValue->size(); i++) {
			vecValue->push_back(stol(contentVecValue->at(i)));
		}
		// bit packing
		bitPackingVecValue();
		delete contentVecValue;
		// restore dataColumn
		vector<string>* contentDataColumn = new vector<string>();
		Util::readFromDisk(contentDataColumn, dataColumnFile);
		dataColumn->clear();
		for (size_t i = 0; i < contentDataColumn->size(); i = i+3) {
			data_column data;
			if (i + 1 <= contentDataColumn->size())
				data.csn = stol(contentDataColumn->at(i));
			if (i + 2 <= contentDataColumn->size())
				data.encodedValueIdx = stol(contentDataColumn->at(i+1));
			if (i + 3 <= contentDataColumn->size())
				data.versionFlag = contentDataColumn->at(i+2) != "0";
			dataColumn->push_back(data);
		}
		delete contentDataColumn;
	}

	// save to undo space log
	void saveUndoLog(Transaction* transaction, size_t txIdx, size_t rid) {
		// log delta space
		vector<string>* deltaSpaceLog = new vector<string>();
		deltaSpace->redoLogCreate(deltaSpaceLog);
		// log version vecValue
		vector<string>* versionVecValueLog = new vector<string>();
		for (size_t i = 0; i < versionVecValue->size(); i++) {
			size_t value = versionVecValue->at(i);
			versionVecValueLog->push_back(to_string(value));
		}
		// log versionColumn
		vector<string>* versionColumnLog = new vector<string>();
		if (rid != -1) {
			// update case
			version_column versionColumnValue = versionColumn->at(rid);
			versionColumnLog->push_back(to_string(rid));
			versionColumnLog->push_back(to_string(versionColumnValue.encodedValueIdx));
			versionColumnLog->push_back(to_string(versionColumnValue.csn));
			// next pointer no change because new version will point to this
		}
		else {
			// insert case => save number of current row
			versionColumnLog->push_back(to_string(versionColumn->size()));
		}
		// log hashtable
		vector<string>* hashtableLog = new vector<string>();
		for(map<size_t, size_t>::iterator it = hashtable.begin(); it != hashtable.end(); ++it) {
			hashtableLog->push_back(to_string(it->first));
			hashtableLog->push_back(to_string(it->second));
		}
		// log dataColumn
		vector<string>* dataColumnLog = new vector<string>();
		if (rid != -1) {
			// just only log in update case
			data_column dataColumnValue = dataColumn->at(rid);
			dataColumnLog->push_back(to_string(rid));
			dataColumnLog->push_back(to_string(dataColumnValue.encodedValueIdx));
			dataColumnLog->push_back(to_string(dataColumnValue.csn));
			dataColumnLog->push_back(to_string(dataColumnValue.versionFlag));
		}
		else {
			// insert case => save number of current row
			dataColumnLog->push_back(to_string(dataColumn->size()));
		}

		// add to undo space
		Transaction::undoSpace undoValue;
		undoValue.txIdx = txIdx;
		undoValue.colName = this->getName();
		undoValue.dataColumn = dataColumnLog;
		undoValue.delta = deltaSpaceLog;
		undoValue.hashtable = hashtableLog;
		undoValue.versionColumn = versionColumnLog;
		undoValue.versionVecValue = versionVecValueLog;
		transaction->saveUndoSpace(txIdx, undoValue);
	}

	void abort(Transaction::undoSpace undoValue) {
		// restore delta
		this->deltaSpace->redoLogRestore(undoValue.delta);
		// restore versionVecValue
		this->versionVecValue->clear();
		for(size_t i = 0; i < undoValue.versionVecValue->size(); i++) {
			string value = undoValue.versionVecValue->at(i);
			versionVecValue->push_back(stol(value));
		}
		// restore versionColumn
		if (undoValue.versionColumn->size() >= 3) {
			// update case
			size_t rid = stol(undoValue.versionColumn->at(0));
			size_t encodedValueIdx = stol(undoValue.versionColumn->at(1));
			uint64_t csn = stol(undoValue.versionColumn->at(2));
			version_column versionValue = versionColumn->at(rid);
			versionValue.encodedValueIdx = encodedValueIdx;
			versionValue.csn = csn;
			versionColumn->at(rid) = versionValue;
		}
		else if (undoValue.versionColumn->size() == 1) {
			// insert case => delete all new values
			size_t oldNumOfRows = stol(undoValue.versionColumn->at(0));
			versionColumn->resize(oldNumOfRows);
		}
	}
};

} /* namespace std */

#endif /* COLUMN_H_ */
