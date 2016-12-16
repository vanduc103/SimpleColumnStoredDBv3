/*
 * Dictionary.cpp
 *
 *  Created on: Sep 23, 2016
 *      Author: duclv
 */
#ifndef _Dictionary_
#define _Dictionary_

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <iostream>
#include <sstream>
#include <iterator>
#include <map>
#include <unordered_map>
#include <algorithm>
#include <locale>
#include "Dictionary.h"

using namespace std;

template<class T>
bool compFunc(T value1, T value2) {
	return value1 < value2;
};

template<class T>
bool equalFunc(T value1, T value2) {
	return value1 == value2;
};

bool equalFunc(string value1, int value2) {
	return false;
};

void strTolower(string& value) {
	transform(value.begin(), value.end(), value.begin(), ::tolower);
}

void strTolower(int& value) {
	// do nothing
}

template<class T>
T* Dictionary<T>::lookup(size_t index) {
	if (items->empty() || index < 0 || index >= items->size()) {
		return NULL;
	} else {
		return &items->at(index);
	}
}

template<class T>
void Dictionary<T>::search(T& value, ColumnBase::OP_TYPE opType, vector<size_t>& result) {
	if (sorted) searchWithSorted(value, opType, result);
	else searchWithNoSorted(value, opType, result);
}

template<class T>
void Dictionary<T>::searchWithSorted(T& value, ColumnBase::OP_TYPE opType, vector<size_t>& result) {
	if (items->empty()) {
		// return -1 to show no result
		result.push_back(-1);
	} else {
		// find the lower bound for value in vector
		typename vector<T>::iterator lower;
		lower = std::lower_bound(items->begin(), items->end(), value,
				compFunc<T>);

		// based on operator to find exact position in dictionary
		switch (opType) {
		case ColumnBase::equalOp: {
			if (lower != items->end() && equalFunc(*lower, value)) {
				result.push_back(lower - items->begin());
			} else {
				// return -1 to show no result
				result.push_back(-1);
			}
			break;
		}
		case ColumnBase::neOp: {
			int exclusivePosition = -1;
			if (lower != items->end() && equalFunc(*lower, value)) {
				exclusivePosition = lower - items->begin();
			}
			// return all dictionary positions except exclusiveValue
			for (size_t i = 0; i < items->size(); i++) {
				if (i != exclusivePosition) {
					result.push_back(i);
				}
			}
			break;
		}
		case ColumnBase::ltOp: {
			// return positions from 0 to lower
			for (size_t i = 0;
					(lower == items->end()) ?
							i < items->size() : i < (lower - items->begin());
					i++) {
				result.push_back(i);
			}
			break;
		}
		case ColumnBase::leOp: {
			unsigned int position = -1;
			if (lower == items->end()) {
				position = items->size();
			} else if (equalFunc(*lower, value)) {
				position = (lower - items->begin()) + 1;
			} else {
				position = lower - items->begin();
			}
			// return from 0 to position
			for (size_t i = 0; i < position; i++) {
				result.push_back(i);
			}
			break;
		}
		case ColumnBase::gtOp: {
			unsigned int position = items->size();
			if (lower == items->end()) {
				// all items are less than value
				position = items->size();
			} else if (equalFunc(*lower, value)) {
				position = (lower - items->begin()) + 1;
			} else {
				position = lower - items->begin();
			}
			// return from postion to items.size()
			for (size_t i = position; i < items->size(); i++) {
				result.push_back(i);
			}
			break;
		}
		case ColumnBase::geOp: {
			// return from lower to items.size()
			unsigned int i =
					(lower == items->end()) ?
							items->size() : (lower - items->begin());
			for (; i < items->size(); i++) {
				result.push_back(i);
			}
			break;
		}
		case ColumnBase::containOp: {
			// search by inverted index
			struct invertedIndex idxContain;
			strTolower(value);	// to lower to compare with index
			idxContain.word = value;
			typename vector<invertedIndex>::iterator lowerIdx;
			lowerIdx = std::lower_bound(vecIndexLevel0->begin(), vecIndexLevel0->end(),
					idxContain);
			// found
			if (lowerIdx != vecIndexLevel0->end() && *lowerIdx == idxContain) {
				invertedIndex idx = vecIndexLevel0->at(lowerIdx - vecIndexLevel0->begin());
				result.insert(result.end(), idx.location.begin(), idx.location.end());
				// sort result
				std::sort(result.begin(), result.end());
			}
			/*for (size_t i = 0; i < vecInvertedIndex->size(); i++) {
				invertedIndex idx = vecInvertedIndex->at(i);
				if (equalFunc(idx.word, value)) {
					result.insert(result.end(), idx.location.begin(), idx.location.end());
					// sort result
					std::sort(result.begin(), result.end());
				}
			}*/
			break;
		}
		}
	}
}

template<class T>
void Dictionary<T>::searchWithNoSorted(T& value, ColumnBase::OP_TYPE opType, vector<size_t>& result) {
	if (items->empty()) {
		// return -1 to show no result
		result.push_back(-1);
	} else {
		// Search with no sorted dictionary => scan through all items
		for (size_t i = 0; i < items->size(); i++) {
			T dictionaryValue = items->at(i);
			// based on operator to find exact position in dictionary
			switch (opType) {
			case ColumnBase::equalOp: {
				// equal
				if (equalFunc(dictionaryValue, value)) {
					result.push_back(i);
				} else {
					// return -1 to show no result
					result.push_back(-1);
				}
				// return result because dictionary is no duplicate values
				return;
			}
			case ColumnBase::neOp: {
				// not equal
				if (!equalFunc(value, dictionaryValue)) {
					result.push_back(i);
				}
				break;
			}
			case ColumnBase::ltOp: {
				// less than
				if (compFunc(dictionaryValue, value)){
					result.push_back(i);
				}
				break;
			}
			case ColumnBase::leOp: {
				// less than or equal = not greater than
				if (!compFunc(value, dictionaryValue)) {
					result.push_back(i);
				}
				break;
			}
			case ColumnBase::gtOp: {
				// greater than
				if (compFunc(value, dictionaryValue)){
					result.push_back(i);
				}
				break;
			}
			case ColumnBase::geOp: {
				// greater than or equal = not less than
				if (!compFunc(dictionaryValue, value)) {
					result.push_back(i);
				}
				break;
			}
			case ColumnBase::containOp: {
				// search by inverted index
				struct invertedIndex idxContain;
				strTolower(value);	// to lower to compare with index
				idxContain.word = value;
				typename vector<invertedIndex>::iterator lowerIdx;
				lowerIdx = std::lower_bound(vecIndexLevel0->begin(), vecIndexLevel0->end(),
						idxContain);
				// found
				if (lowerIdx != vecIndexLevel0->end() && *lowerIdx == idxContain) {
					invertedIndex idx = vecIndexLevel0->at(lowerIdx - vecIndexLevel0->begin());
					result.insert(result.end(), idx.location.begin(), idx.location.end());
					// sort result
					std::sort(result.begin(), result.end());
				}
				/*for (size_t i = 0; i < vecInvertedIndex->size(); i++) {
					invertedIndex idx = vecInvertedIndex->at(i);
					if (equalFunc(idx.word, value)) {
						result.insert(result.end(), idx.location.begin(), idx.location.end());
						// sort result
						std::sort(result.begin(), result.end());
					}
				}*/
				break;
			}
			}
		}
	}
}

template<class T>
size_t Dictionary<T>::addNewElement(T& value, vector<size_t>* vecValue, bool sorted, bool bulkInsert) {
	if (items->empty()) {
		items->push_back(value);
		if (bulkInsert)
			bulkVecValue->push_back(value);
		vecValue->push_back(0);
		if (!sorted) (*sMap)[value] = 1;
		return 0;
	} else if (!sorted) {
		// check if value existed on dictionary
		if ((*sMap)[value] == 0) {
			items->push_back(value);
			vecValue->push_back(items->size() - 1);
			(*sMap)[value] = vecValue->back() + 1;
		}
		else {
			vecValue->push_back((*sMap)[value] - 1);
		}
		return vecValue->back();
	} else {
		// find the lower bound for value in vector
		typename vector<T>::iterator lower;
		lower = std::lower_bound(items->begin(), items->end(), value,
				compFunc<T>);
		if (bulkInsert)
			bulkVecValue->push_back(value);
		// value existed
		if (lower != items->end() && equalFunc(value, *lower)) {
			// return the position of lower
			long elementPos = lower - items->begin();
			vecValue->push_back(elementPos);
			return elementPos;
		} else {
			// The position of new element in dictionary
			size_t newElementPos = 0L;
			if (lower == items->end()) {
				// insert to the end of dictionary
				newElementPos = items->size();
				items->push_back(value);
				vecValue->push_back(newElementPos);
			} else {
				newElementPos = lower - items->begin();
				// insert into dictionary
				items->insert(lower, value);
				// update (+1) to all elements in vecValue have value >= newElementPos
				if (!bulkInsert) {
					for (int i = 0; i < vecValue->size(); i++) {
						if (vecValue->at(i) >= newElementPos) {
							++vecValue->at(i);
						}
					}
				}
				vecValue->push_back(newElementPos);
			}

			// return the position of new element
			return newElementPos;
		}
	}
}

template<>
void Dictionary<string>::buildInvertedIndex() {
	// make an unordered_map of words from all items
	vector<string>* strItems = (vector<string>*) items;
	//unordered_map<string, vector<size_t>> mapWords;
	unordered_map<string, vector<size_t>> mapWordsLevel0;
	for (size_t i = 0; i < strItems->size(); i++) {
		// split item into word by whitespace
		vector<string> words;
		istringstream iss(strItems->at(i));
		copy(istream_iterator<string>(iss), istream_iterator<string>(), back_inserter(words));
		// add to map
		for (size_t j = 0; j < words.size(); j++) {
			// map key = word + position in text
			string key = words[j] + "#" + to_string(j);
			//vector<size_t> location = mapWords[key];
			vector<size_t> locationLevel0 = mapWordsLevel0[words[j]];
			//location.push_back(i); // text position
			locationLevel0.push_back(i);
			//mapWords[key] = location;
			mapWordsLevel0[words[j]] = locationLevel0;
		}
	}
	// create vector of inverted index from map
	/*for (const auto& m : mapWords) {
		string key = m.first;
		vector<size_t> location = m.second;
		size_t findPos = key.find('#');
		string word = key.substr(0, findPos);
		char position = stoi(key.substr(findPos + 1));
		// inverted index with position
		invertedIndex idx;
		strTolower(word); // keep lower word
		idx.word = word;
		idx.position = position;
		idx.location = location;
		vecInvertedIndex->push_back(idx);
	}*/
	// create vector of inverted index level 0 from map
	for (const auto& m : mapWordsLevel0) {
		string word = m.first;
		vector<size_t> location = m.second;
		// inverted index level 0
		invertedIndex idxLevel0;
		strTolower(word); // keep lower word
		idxLevel0.word = word;
		idxLevel0.location = location;
		vecIndexLevel0->push_back(idxLevel0);
	}
	// sort vector of inverted index
	//std::sort(vecInvertedIndex->begin(), vecInvertedIndex->end());
	std::sort(vecIndexLevel0->begin(), vecIndexLevel0->end());
	//for (size_t i = 0; i < vecInvertedIndex->size(); i++)
	//cout << "Phan tu " << i << " = " << (vecInvertedIndex->at(i)).word << "," << to_string((vecInvertedIndex->at(i)).position) << endl;
}


template<class T>
size_t Dictionary<T>::size() {
	return items->size();
}

template<class T>
void Dictionary<T>::print(int row) {
	for (int i = 0; i < items->size() && i < row; i++) {
		cout << "Dictionary[" << i << "] = " << items->at(i) << "\n";
	}
}

// overload to_string for string case
string to_string(string val) {
	return val;
}

template<class T>
string Dictionary<T>::saveToDisk(string logPath) {
	string fileToSave = logPath + "/dictionary_" + to_string(Util::currentMilisecond());
	vector<string>* content = new vector<string>();
	content->push_back(to_string(sorted));
	for (size_t i = 0; i < items->size(); i++) {
		T value = items->at(i);
		content->push_back(to_string(value));
	}
	Util::saveToDisk(content, fileToSave);
	return fileToSave;
}

template<>
void Dictionary<int>::restore(string fileName) {
	vector<string>* content = new vector<string>();
	Util::readFromDisk(content, fileName);

	if (content->size() >= 1 && items != NULL) {
		items->clear(); // clear before restore
		// restore sorted
		this->sorted = content->at(0) != "0";

		// restore dictionary values
		for (size_t i = 1; i < content->size(); i++) {
			string value = content->at(i);
			items->push_back(stoi(value));
		}
	}
	delete content;
}

template<>
void Dictionary<string>::restore(string fileName) {
	vector<string>* content = new vector<string>();
	Util::readFromDisk(content, fileName);

	if (content->size() >= 1 && items != NULL) {
		items->clear(); // clear before restore
		// restore sorted
		this->sorted = content->at(0) != "0";

		// restore dictionary values
		for (size_t i = 1; i < content->size(); i++) {
			string value = content->at(i);
			items->push_back(value);
		}
	}
	delete content;
}

template<class T>
void Dictionary<T>::redoLogCreate(vector<string>* content) {
	// add all items to content
	if (content != NULL) {
		content->push_back(to_string(sorted));
		for (size_t i = 0; i < items->size(); i++) {
			T value = items->at(i);
			content->push_back(to_string(value));
		}
	}
}

template<>
void Dictionary<int>::redoLogRestore(vector<string>* content) {
	if (content != NULL) {
		if (content->size() >= 1 && items != NULL) {
			items->clear();	// clear before restore
			// restore sorted
			this->sorted = content->at(0) != "0";

			// restore dictionary values
			for (size_t i = 1; i < content->size(); i++) {
				string value = content->at(i);
				items->push_back(stoi(value));
			}
		}
		delete content;
	}
}

template<>
void Dictionary<string>::redoLogRestore(vector<string>* content) {
	if (content != NULL) {
		if (content->size() >= 1 && items != NULL) {
			items->clear();	// clear before restore
			// restore sorted
			this->sorted = content->at(0) != "0";

			// restore dictionary values
			for (size_t i = 1; i < content->size(); i++) {
				string value = content->at(i);
				items->push_back(value);
			}
		}
		delete content;
	}
}

template class Dictionary<string> ;
template class Dictionary<int> ;

#endif
