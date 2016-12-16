/*
 * Util.h
 *
 *  Created on: Oct 20, 2016
 *      Author: duclv
 *      Description: Utilities functions
 */

#include <stdio.h>
#include <stdlib.h>
#include <vector>
#include <chrono>
#include <fstream>
#include <iostream>
#include <string>
#include "boost/filesystem.hpp"

#ifndef UTIL_H_
#define UTIL_H_

namespace std {

class Util {
public:
	// compute the number of row ids selected
	static size_t rowSelectedSize(vector<bool>* vRowId) {
		size_t count = 0;
		for (bool ele : *vRowId) {
			if (ele) ++count;
		}
		return count;
	}

	static size_t currentMilisecond() {
		return chrono::duration_cast < chrono::milliseconds
					> (chrono::steady_clock::now().time_since_epoch()).count();
	}

	static void saveToDisk(vector<string>* content, string fileName) {
		if (content != NULL) {
			std::ofstream out(fileName);
			for (string s : (*content)) {
				out << s << endl;
			}
			out.flush();
			out.close();
			delete content;
		}
	}

	static void readFromDisk(vector<string>* content, string fileName) {
		if (content != NULL) {
			ifstream infile(fileName);
			string line;
			while(getline(infile, line)) {
				content->push_back(line);
			}
			infile.close();
		}
	}

	static void appendToFile(string content, string fileName) {
		if (content != "" && fileName != "") {
			std::ofstream ofs;
			ofs.open(fileName, ofstream::app);
		}
	}

	static void createFolder(string folderPath) {
		boost::filesystem::path dir(folderPath);
		boost::filesystem::remove_all(dir);
		boost::filesystem::create_directory(dir);
	}

	static string getLatestFile(string folderPath, string pattern);
	static vector<string> getNewestFiles(string folderPath, string pattern, long aTime);
	static void parseContentToVector(vector<string>* data, string content, string delim);

};

} /* namespace std */

#endif /* UTIL_H_ */
