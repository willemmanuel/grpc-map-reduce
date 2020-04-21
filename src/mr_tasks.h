#pragma once

#include <string>
#include <iostream>
#include <fstream>

struct BaseMapperInternal {
		BaseMapperInternal();
		void emit(const std::string& key, const std::string& val);
		void set_file(const std::string file);
		std::string _file;
};


inline BaseMapperInternal::BaseMapperInternal() {
}

inline void BaseMapperInternal::set_file(const std::string file) {
	_file = file;
}

inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
	std::ofstream result;
	result.open(_file, std::ofstream::out | std::ios_base::app);
	result << key + "," + val + '\n';
	result.close();
}

struct BaseReducerInternal {

		BaseReducerInternal();
		void emit(const std::string& key, const std::string& val);
		void set_file(const std::string file);
		std::string _file;
};

inline BaseReducerInternal::BaseReducerInternal() {}

inline void BaseReducerInternal::set_file(const std::string file) {
	_file = file;
}

inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
	std::ofstream result;
	result.open(_file, std::ofstream::out | std::ios_base::app);
	result << key + " " + val + '\n';
	result.close();
}