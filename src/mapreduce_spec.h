#pragma once

#include <vector>
#include <string>
#include <iostream>
#include <fstream>

struct MapReduceSpec {
	int num_workers;
	int map_kilobytes;
	std::string user_id;
	int num_output_files;
	std::string output_dir;
	std::vector<std::string> input_files;
	std::vector<std::string> workers;
	std::vector<bool> workers_available;
};

/**
String split function from http://www.cplusplus.com/articles/2wA0RXSz/
*/
inline std::vector<std::string> csv_parse(const std::string& s) {
	std::string buff{""};
	std::vector<std::string> v;
	const char c = ',';
	for(auto n:s) {
		if(n != c) buff+=n; else
		if(n == c && buff != "") { v.push_back(buff); buff = ""; }
	}
	if(buff != "") v.push_back(buff);
	return v;
}

inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {
	std::ifstream config_file(config_filename);
	std::string line;
	while (std::getline(config_file, line)) {
		std::string key = line.substr(0, line.find("="));
		line.erase(0, line.find("=") + 1);
		std::string value = line;

		if (!key.compare("n_workers")) {
			mr_spec.num_workers = std::stoi(value);
		} else if (!key.compare("worker_ipaddr_ports")) {
			mr_spec.workers = csv_parse(value);
		} else if (!key.compare("input_files")) {
			mr_spec.input_files = csv_parse(value);
		} else if (!key.compare("output_dir")) {
			mr_spec.output_dir = value;
		} else if (!key.compare("n_output_files")) {
			mr_spec.num_output_files = std::stoi(value);
		} else if (!key.compare("map_kilobytes")) {
			mr_spec.map_kilobytes = std::stoi(value);
		} else if (!key.compare("user_id")) {
			mr_spec.user_id = value;
		} else {
			std::cout << "Invalid key " << key << "\n";
		}
	}
	config_file.close();
	// All workers start as available
	for (int i = 0; i < mr_spec.num_workers; i++) {
		mr_spec.workers_available.push_back(true);
	}
	return true;
}

inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
	if (mr_spec.num_workers != mr_spec.workers.size()) {
		return false;
	}
	if (mr_spec.num_workers == 0) {
		return false;
	}
	if (mr_spec.map_kilobytes == 0) {
		return false;
	}
	if (mr_spec.num_output_files == 0) {
		return false;
	}
	if (mr_spec.user_id.empty()) {
		return false;
	}
	if (mr_spec.output_dir.empty()) {
		return false;
	}
	if (mr_spec.input_files.size() == 0) {
		return false;
	}
	if (mr_spec.workers.size() == 0) {
		return false;
	}

	return true;
}
