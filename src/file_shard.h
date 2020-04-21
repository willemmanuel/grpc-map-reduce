#pragma once

#include <vector>
#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include <algorithm> 
#include "mapreduce_spec.h"

struct FileShard {
     std::string file;
     // Start byte inclusive
     long start_byte;
     // End byte exclusive
     long end_byte;
     bool complete;
};

/*
With help from stackoverflow.com/questions/5840148
*/
inline long get_file_size(std::string filename) {
    struct stat stat_buf;
    int rc = stat(filename.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

inline long find_next_line_break(const std::string file_path, long start, long size) {
     if (start > size) return size;
     std::ifstream file(file_path);
     file.seekg(start);
     file.ignore(size, '\n');
     long result = file.tellg();
     file.close();
     if (result == -1) return size;
     return result;
}

inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
     for (int file_idx = 0; file_idx < mr_spec.input_files.size(); file_idx++) {
          long total_size = get_file_size(mr_spec.input_files.at(file_idx));
          long map_bytes = (long)mr_spec.map_kilobytes * 1000;
          long begin = 0;
          long end = std::min(total_size, map_bytes);
          while (true) {
               if (begin >= total_size) break;
               FileShard next_shard;
               next_shard.file = mr_spec.input_files.at(file_idx);
               next_shard.start_byte = begin;
               long aligned_end = find_next_line_break(mr_spec.input_files.at(file_idx), end, total_size);
               next_shard.end_byte = aligned_end;
               next_shard.complete = false;
               fileShards.push_back(next_shard);
               begin = aligned_end + 1;
               end = std::min(total_size, map_bytes + begin);
          }
     }
     return true;
}