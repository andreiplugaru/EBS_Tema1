**Configuration:**
- **Publications**: 100,000
- **Subscriptions**: 100,000
- **Field Weights**
  - **Temperature**: 0.2
  - **Rain**: 0.3
  - **City**: 1
- **Equality Weights**
  - **City**: 0.9
  - **Temperature**: 0.5

**Setup:**

	AMD Ryzen 7 7840HS with Radeon 780M Graphics 

	Base speed:	3.80 GHz
	Sockets:	1
	Cores:	8
	Logical processors:	16

**Results**:

| Threads/ Processes | Publication Time (s) | Subscription Time (s) | Total Time (s) |
|---------|----------------------|------------------------|----------------|
| 10      | 0.8835               | 2.6115                 | 3.4950         |
| 5       | 0.7409               | 2.9912                 | 3.7321         |
| 1       | 0.9562               | 1.1480                 | 2.1042         |

To optimize performance, we implemented a hybrid parallelism strategy: publication generation was handled using multiple processes, while subscription generation was managed using multiple threads.

The reason for using a multi process approach was to avoid Python limitation which allows only one thread to hold the control of the Python interpreter([docs](https://wiki.python.org/moin/GlobalInterpreterLock)). This resulted in clear performance improvements when the number of processes are increased. However, for the subcriptions generation we needed more advanced synchronization, so we used a more traditional multi threading approach.

