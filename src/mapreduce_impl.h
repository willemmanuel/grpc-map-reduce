#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"

class MapReduceImpl {
	
	public:
		bool run(const std::string& config_filename);

	private:
		bool read_and_validate_spec(const std::string& config_filename);
		bool create_shards();
		bool run_master();

		MapReduceSpec mr_spec_;
		std::vector<FileShard> file_shards_;

};
