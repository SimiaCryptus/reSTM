# What is reSTM?

reSTM is a distributed software transactional memory platform implemented using a rest-friendly HTTP protocol and written in Scala. It is currently a prototype packaged as a Play application. 

For more information, see the [manual](https://docs.google.com/document/d/1NrdFnJWqWfGVvwKLG-WNrsNLir7hp6XkRl5LJFq5sFE/edit?usp=sharing)

It uses a layered architecture and distributed design; Data is managed via a software transactional memory api which stores pointer values using multi version concurrency control (MVCC), and transaction isolation is enforced by a system of read/write locks on individual pointers. The state of memory pointer values and data transactions are stored as a system of stateful actors. Additional software layers on top of the storage platform provide data structures, algorithms, and facilities to manage and distribute task execution.

Although this solution will generally be less performant than a more specialized alternatives such as Hadoop due to its highly granular nature and enforced isolation, it may provide a competitive platform due to flexibility and ease of use in implementing nearly any algorithm or data structure while also delivering a scalable service.

Some features of this platform:

1. Provides scalable, transactional cluster memory access with perfect isolation
1. Non-blocking design uses no master server and no global locks, with essentially no scalability bottlenecks other than contention for logical pointers
1. Data can be replicated and offloaded as appropriate using a supplemental external database such as dynamo or an on-disk bdb.
1. It can manage the execution of arbitrary lists and graphs of tasks distributed among the cluster, allowing the management and reduction of virtually unlimited subtasks.
1. The protocol is a REST API, using standard HTTP and JSON serialization. It is potentially usable by clients in other languages, and the raw memory can be browsed using a web browser.

Although this project is intended to be research-grade code to illustrate an idea, I have attempted to implement most of the essentials for a "production" system within this platform, including:

The ability to setup a high-scale fault-tolerant cluster, with data both partitioned and replicated

1. Visibility into nodes including activity logs and performance metrics
1. Ability to gracefully shut down nodes for servicing, avoiding data loss.
1. Halfway decent documentation (sic)

#Caveat Emptor

Hopefully, the reader is excited to try out this tool. It does provide an interesting and promising solution to common problems in service development. However, there are some issues I have not addressed that would be needed in a real, sustainable, operable solution. Speaking as a professional engineer of commercial and enterprise software services, don’t use this on Prod! I’ve tried to keep the bar high enough that I could someday present it as an option for solving problems in my professional realm, so that it is at least “within sight” of production, so with these caveats aside, there is no reason I can think of that would prevent this computing model from becoming a viable solution for a wide range of problems.

One big point of scalability that has not been addressed (but can be implemented) is memory reclamation, and thus it is practically inevitable that the cold store (heap, BDB/disk, Dynamo/cloud) will grow indefinitely with continued use. Basically, not everything is ever deleted. The lack of this feature is one major reason I would not claim this service is “production” grade.

Another reason for caution is simply that reSTM is, at this point, the work of a single engineer with far too much free time on his hands. You get what you pay for, etc. For example, the collection library has been to this point very much ad-hoc and very little attention has been paid to freeing memory (see above). I have developed only the classes and capabilities that made sense for the other things I wanted to build, namely an interesting proof-of-concept demo and limited experimentation.

Other than that, this code is released to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  

You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.

# Getting Started

We will now review a brief tour of what it takes to start,test, and shutdown the application, starting from a standard plain Linux environment with Java 8 and Git installed. (I start from a fresh amazon ec2 image)

First clone the application from github:

	git clone https://github.com/acharneski/reSTM.git
	cd reSTM
	

This is packaged as a Play application, and as such there are many features of the build system that are of interest. For example you can launch a web-based UI:

	chmod +x ./bin/activator
 	./bin/activator ui

Which allows you to build, run, and test the application and even view and edit source code. For more information about the many features of this toolkit, check out the documentation. I mainly use this UI for rebuilding the application’s auto-generated files while developing; for example, when I modify the routes.txt file. 

The basic command to compile and run the server in non-development mode is:

	./bin/activator dist
	cd target/universal
	unzip play-scala-1.0-SNAPSHOT.zip
	./play-scala-1.0-SNAPSHOT/bin/play-scala -Dplay.crypto.secret=abcdefghijk &

This will launch the service on port 9000 with all the default settings. You can check the health of the system by requesting the metrics page:

	curl http://localhost:9000/sys/metrics
	{
	  "code": {
	    "SystemController": {
	      "SystemController.metrics": {
		"success": 0,
		"failed": 0,
		"invocations": 1,
		"avgTime": "NaN",
		"totalTime": 0.0
	      }
	    }
	  },
	  "scalars": {},
	  "txns": {}
	}

Since we haven’t run anything yet, it will be fairly empty, but it normally contains detailed information about the timing and counts of each transaction call and many critical methods.

Just starting the server does not fully initialize the service. Namely, the storage service needs to be configured, and then the execution service needs to be started. This is done by requesting the system init endpoint:

	curl http://localhost:9000/sys/init
	Init actor system

This bootstraps the daemon and execution management services.

Now let's do something with the service. A simple yet comprehensive test is the demo sort task, which creates a random collection of random strings and then sorts them using the task execution service. Since this task incorporates many parts of the platform, it serves as a good test. First, simply make a request to the demo endpoint:

	curl http://localhost:9000/demo/sort?n=10000 
	<html><body><a href="/task/result/69312247132:0:09ec">Task 69312247132:0:09ec started</a></body></html>

This will return the ID of a newly created task, in this case “69312247132:0:09ec”. You can monitor this task as it completes execution by requesting its status endpoint:

	curl http://localhost:9000/task/info/69312247132:0:09ec

This should show that the task has expanded into a tree of sub-tasks, with each leaf being an executing or queued task. As this completes, it will grow into a larger tree as the tasks expand, and then it will collapse as the results are reduced. Once complete, the result can be fetched from the task’s result endpoint:

	curl http://localhost:9000/task/result/69312247132:0:09ec

(This method will block until the result is available, if called before the result is ready.)

Eventually, you will want to do an orderly shutdown of the service. This can be done by requesting the system shutdown endpoint:

	curl http://localhost:9000/sys/shutdown

This will block while all tasks are completed, all values are written to cold storage, all transactions are terminated, and all (non-blocking) requests are complete. The server will not accept new requests, start new tasks, or allow new pointer locks while shutting down. After shutdown, the process can be terminated and the host restarted as needed.
