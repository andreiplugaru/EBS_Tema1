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

| Threads | Publication Time (s) | Subscription Time (s) | Total Time (s) |
|---------|----------------------|------------------------|----------------|
| 10      | 0.8835               | 2.6115                 | 3.4950         |
| 5       | 0.7409               | 2.9912                 | 3.7321         |
| 1       | 0.9562               | 1.1480                 | 2.1042         |

