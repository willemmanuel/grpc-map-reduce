## gRPC MapReduce Implementation
This is an implementation of a MapReduce framework using gRPC and written in C++.

`make` generates two binaries: a worker and master. The master worker accepts a `config.ini` of the following format to start a MR job.

```
n_workers=6
worker_ipaddr_ports=localhost:50051,localhost:50052,localhost:50053,localhost:50054,localhost:50055,localhost:50056
input_files=input/testdata_1.txt,input/testdata_2.txt
output_dir=output
n_output_files=8
map_kilobytes=500
user_id=cs6210
```

The worker accepts an address (e.g. `localhost:50051`) to begin listening on for work. The worker exposes a gRPC server which allows the master node to schedule map or reduce jobs.

This was written for Prof. Kishmore's Adv OS class (CS6210); setup boilerplate was provided from the assignment.