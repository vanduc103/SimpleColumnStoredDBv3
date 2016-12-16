/*
 * Util.cpp
 *
 *  Created on: Oct 20, 2016
 *      Author: duclv
 */

#include "Util.h"

namespace std {

	// get latest file from file pattern
	string Util::getLatestFile(string folderPath, string pattern) {
		string latestFileName = "";
		long maxTime = 0;
		boost::filesystem::path dir(folderPath);
		cout << "dadada" << endl;
		if (true) {
			cout << "here - 2" << endl;
			boost::filesystem::directory_iterator end_iter;
			for ( boost::filesystem::directory_iterator dir_itr( dir );
				  dir_itr != end_iter;
				  ++dir_itr ) {
				if ( boost::filesystem::is_regular_file( dir_itr->status() ) ) {
					string filename = dir_itr->path().filename().string();
					cout << filename << endl;
					size_t i = filename.find(pattern);
					if (i != string::npos) {
						//found
						long time = stol(filename.substr(i+1));
						if (time > maxTime)
							latestFileName = filename;
					}
				}
			}
		}
		cout << "here - 3" << endl;
		return latestFileName;
	}

	// get all file from a specific time
	vector<string> Util::getNewestFiles(string folderPath, string pattern, long aTime) {
		vector<string> newestFiles;
		boost::filesystem::path dir(folderPath);
		if (boost::filesystem::exists(dir) && boost::filesystem::is_directory(dir)) {
			boost::filesystem::directory_iterator end_iter;
			for ( boost::filesystem::directory_iterator dir_itr( dir );
				  dir_itr != end_iter;
				  ++dir_itr ) {
				if ( boost::filesystem::is_regular_file( dir_itr->status() ) ) {
					string filename = dir_itr->path().filename().string();
					cout << filename << endl;
					size_t i = filename.find(pattern);
					if (i != string::npos) {
						//found
						long time = stol(filename.substr(i+1));
						if (time >= aTime)
							newestFiles.push_back(filename);
					}
				}
			}
		}
		return newestFiles;
	}

	void Util::parseContentToVector(vector<string>* data, string content, string delim) {
		string token;
		size_t last = 0; size_t next = 0;
		while ((next = content.find(delim, last)) != string::npos) {
			token = content.substr(last, next - last);
			last = next + delim.length();
			if(token != "") data->push_back(token);
		}
		// get the last token
		token = content.substr(last);
		if(token != "") data->push_back(token);
	}

}  // namespace std
