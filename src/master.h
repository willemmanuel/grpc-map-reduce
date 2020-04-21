#pragma once

#include <iostream>
#include <memory>
#include <vector>
#include <string>
#include <grpcpp/grpcpp.h>
#include <future>
#include <unistd.h>


#include "mapreduce_spec.h"
#include "file_shard.h"
#include "masterworker.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using masterworker::WorkRequest;
using masterworker::Shard;
using masterworker::WorkResponse;
using masterworker::WorkerService;
using grpc::ClientAsyncResponseReader;
using grpc::ServerAsyncResponseWriter;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::Channel;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::ServerCompletionQueue;

class WorkerClient {
 public:
  WorkerClient(std::shared_ptr<Channel> channel)
	  : stub_(WorkerService::NewStub(channel)) {}

  void scheduleWork(
	  WorkRequest request,
	  const std::function<void(WorkResponse)>& completion
  ) {
	// Two minute timeout for each job
	unsigned int timeout = 120;

	std::chrono::system_clock::time_point deadline =
		std::chrono::system_clock::now() + std::chrono::seconds(timeout);

    Status status;
    CompletionQueue cq;
    WorkResponse reply;
    ClientContext context;
	context.set_deadline(deadline);

	try {
		std::unique_ptr<ClientAsyncResponseReader<WorkResponse>> rpc(
			stub_->PrepareAsyncDoWork(&context, request, &cq));
		rpc->StartCall();

		rpc->Finish(&reply, &status, (void*)1);
		void* got_tag;
		bool ok = false;

		GPR_ASSERT(cq.Next(&got_tag, &ok));
		GPR_ASSERT(got_tag == (void*)1);
		GPR_ASSERT(ok);
		completion(reply);
	} catch (...) {
		// If any exception is encountered, set request success to
		// false to force a retry
		reply.set_success(false);
		completion(reply);
	}
  }

 private:
  std::unique_ptr<WorkerService::Stub> stub_;
};


class Master {

	public:
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		bool run();

		int freeNode();
		void scheduleMapShard(int i);
		void scheduleReduceShard(int i);

	private:
		MapReduceSpec _mr_spec;
		std::vector<FileShard> _file_shards;
    	std::vector<WorkerClient*> _workers;
		std::vector<std::string> _mapped_files;
		std::vector<std::future<int>> _map_futures;
		std::vector<std::string> _reduced_files;
		std::vector<std::future<int>> _reduce_futures;
};

Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {
	_mr_spec = mr_spec;
	_file_shards = file_shards;
	for (auto const& worker : _mr_spec.workers) {
    	WorkerClient *client = new WorkerClient(grpc::CreateChannel(worker, grpc::InsecureChannelCredentials()));
		_workers.push_back(client);
	}
}

int Master::freeNode() {
	while(true) {
		for (int i = 0; i < _mr_spec.num_workers; i++) {
			if (_mr_spec.workers_available.at(i) == true) { 
				return i;
			}
			if (i == _mr_spec.num_workers - 1) {
				i = 0;
			}
		}
	}
}

void Master::scheduleMapShard(int i) {
	auto shard = _file_shards.at(i);
	WorkRequest request;
	request.set_request_type("map");
	request.set_user_id(_mr_spec.user_id);
	request.set_output_directory(_mr_spec.output_dir);
	Shard* new_shard = request.add_files();
	new_shard->set_path(shard.file);
	new_shard->set_start(shard.start_byte);
	new_shard->set_end(shard.end_byte);
	new_shard->set_index(i);
	int node_to_use = freeNode();
	_mr_spec.workers_available[node_to_use] = false;

	const std::function<void(WorkResponse)> cb = [&, node_to_use](WorkResponse response) {
		_mr_spec.workers_available[node_to_use] = true;
		if (!response.success()) {
			return scheduleMapShard(i);
		}
		for (auto output : response.output_files()) {
			_mapped_files.push_back(output);
		}
	};
	std::future<int> f = std::async(std::launch::async,[&, cb, request]{ 
		_workers.at(node_to_use)->scheduleWork(request, cb);
		return 0;
	});
	_map_futures.push_back(std::move(f));
}

void Master::scheduleReduceShard(int i) {
	int shards_per_output = _mapped_files.size() / _mr_spec.num_output_files;
	int start_shard = i * shards_per_output;
	int end_shard = start_shard + shards_per_output;
	if (i == _mr_spec.num_output_files - 1) {
		end_shard = _mapped_files.size();
	}
	WorkRequest request;
	request.set_request_type("reduce");
	request.set_user_id(_mr_spec.user_id);
	request.set_output_directory(_mr_spec.output_dir);

	for (int j = start_shard; j < end_shard; j++) {
		Shard* new_shard = request.add_files();
		new_shard->set_path(_mapped_files.at(j));
		new_shard->set_start(0);
		new_shard->set_end(0);
		new_shard->set_index(i);
	}

	int node_to_use = freeNode();
	_mr_spec.workers_available[node_to_use] = false;

	const std::function<void(WorkResponse)> cb = [&, node_to_use](WorkResponse response) {
		_mr_spec.workers_available[node_to_use] = true;
		if (!response.success()) {
			return scheduleReduceShard(i);
		}
		for (auto output : response.output_files()) {
			_reduced_files.push_back(output);
		}
	};

	std::future<int> f = std::async(std::launch::async,[&, cb, request]{ 
		_workers.at(node_to_use)->scheduleWork(request, cb);
		return 0;
	});
	_reduce_futures.push_back(std::move(f));
}

bool Master::run() {
	for (int i = 0; i < _file_shards.size(); i++) {
		scheduleMapShard(i);
	}

	// Wait on all map callbacks
	while (_mapped_files.size() < _file_shards.size()) {}

	for (int i = 0; i < _mr_spec.num_output_files; i++) {
		scheduleReduceShard(i);
	}

	// Wait on all reduce callbacks
	while(_reduced_files.size() < _mr_spec.num_output_files) {}

	return true;
}