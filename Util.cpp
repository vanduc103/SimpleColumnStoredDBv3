/*
 * Util.cpp
 *
 *  Created on: Oct 20, 2016
 *      Author: duclv
 */

#include "Util.h"

void Util::parseContentToVector(vector<string>* data, string content,
		string delim) {
	string token;
	size_t last = 0;
	size_t next = 0;
	while ((next = content.find(delim, last)) != string::npos) {
		token = content.substr(last, next - last);
		last = next + delim.length();
		if (token != "")
			data->push_back(token);
	}
	// get the last token
	if (last < content.length()) {
		token = content.substr(last);
		if (token != "")
			data->push_back(token);
	}
}

string Util::substr(string s, string pattern) {
	if (s != "" && s.find(pattern) != string::npos) {
		return s.substr(s.find(pattern) + pattern.length());
	}
	return s;
}
