#pragma once

#include <grpcpp/grpcpp.h>
#include <mr_task_factory.h>
#include <unordered_map>
#include "masterworker.grpc.pb.h"
#include "mr_tasks.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using masterworker::WorkRequest;
using masterworker::WorkResponse;
using masterworker::WorkerService;

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/*
Strip all special characters to get a unique worker identifier
*/
std::string worker_id(std::string ip_addr) {
	ip_addr.erase(remove(ip_addr.begin(), ip_addr.end(), ':'), ip_addr.end());
	ip_addr.erase(remove(ip_addr.begin(), ip_addr.end(), '/'), ip_addr.end());
	ip_addr.erase(remove(ip_addr.begin(), ip_addr.end(), '.'), ip_addr.end());
	return ip_addr;
}

class Worker : public WorkerService::Service {

	public:
		Worker(std::string ip_addr_port);
		bool run();
		Status DoWork(ServerContext* context, const WorkRequest* request, WorkResponse* reply) override {
			if (request->request_type().compare("map") == 0) {
				std::list<std::string> outputs = map(request);
				reply->set_success(true);
				for (auto const& output : outputs) {
					reply->add_output_files(output);
				}
			} else {
				std::string output = reduce(request);
				reply->set_success(true);
				reply->add_output_files(output);
			}
    		return Status::OK;
  		}

		static std::string WorkerId;

	private:
		std::string _ip_addr;
		std::string _worker_id;

		std::string get_file_name(std::string output_dir, int shard, std::string task) {
			return output_dir + "/" + task + "-" + _worker_id + "-" + std::to_string(shard) + ".txt";
		}

		std::list<std::string> map(const WorkRequest* request) {
			std::list<std::string> output;
			for (auto const& shard : request->files()) {
				auto mapper = get_mapper_from_task_factory(request->user_id());
				std::string file_name = get_file_name(request->output_directory(), shard.index(), "map");
				mapper->impl_->set_file(file_name);
				std::ifstream file;
				file.open(shard.path());
				file.seekg(shard.start());
				std::string line;
				while (file.tellg() <= shard.end() && file.tellg() != -1) {
					std::getline(file, line);
					mapper->map(line);
				}
				file.close();
				output.push_back(file_name);
			}
			return output;
		}

		static bool sort_by_vector_size(
			const std::pair<std::string, std::vector<std::string>> &a,
			const std::pair<std::string, std::vector<std::string>> &b
		) {
			if (b.second.size() != a.second.size())
    			return (b.second.size() < a.second.size());
			return a.first.compare(b.first);
		}

		std::string reduce(const WorkRequest* request) {
			auto reducer = get_reducer_from_task_factory(request->user_id());
			std::string file_name = get_file_name(request->output_directory(), request->files().at(0).index(), "reduce");
			reducer->impl_->set_file(file_name);
			std::unordered_map<std::string, std::vector<std::string>> reduce_dict;
			for (auto const& shard : request->files()) {
				std::ifstream file;
				file.open(shard.path());
				std::string line;
				while (std::getline(file, line)) {
					std::string k, v;
					int delim_idx = line.find(",");
					k = line.substr(0, delim_idx);
					v = line.substr(delim_idx + 1, line.size());
					if (reduce_dict.find(k) == reduce_dict.end()) {
						std::vector<std::string> value_list;
						value_list.push_back(v);
						reduce_dict[k] = value_list;
					} else {
						reduce_dict[k].push_back(v);
					}
				}
				file.close();
			}

			std::vector<std::pair<std::string, std::vector<std::string>>> pair_vector;
			for (auto const& pair : reduce_dict) {
				pair_vector.push_back(pair);
    		}
			std::sort(pair_vector.begin(), pair_vector.end(), sort_by_vector_size);
			for (auto const& pair : pair_vector) {
				reducer->reduce(pair.first, pair.second);
    		}
			return file_name;
		}
};

Worker::Worker(std::string ip_addr_port) {
	_ip_addr = ip_addr_port;
	_worker_id = worker_id(_ip_addr);
}

bool Worker::run() {
	std::string server_address(_ip_addr);
	ServerBuilder builder;
	builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
	builder.RegisterService(this);
	std::unique_ptr<Server> server(builder.BuildAndStart());
	server->Wait();
	return true;
}
