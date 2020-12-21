# Lissandra (C implementation of Cassandra)
Yes, Lissandra the LoL champion. The whole architecture documentation is here https://docs.google.com/document/d/1uMReZYgwPkWuVMxfv_FfyouFpSYVl1kqewOZuyAHw-A/edit (in Spanish)

## Basic idea of the project

![](/architecture.png "Architecture")

We had to develop a distributed key-value database and implement an API that could CRUD across memory and persisted files.

Like Cassandra, there is a memtable that held the "cache", this is the first point of entry of a new key-value record. You also have an N number of Memory workers that are called the "Memory Pool", they know the existance of each other by _gossiping_. This pool holds the values once the main process _dumps_ them. Then, one by one, the memories start sending the new values and keys to the LFS (the Lissandra File System). Once it's there, a compacting process runs in a thread and compacts the files so 

## How it works
### In general (for commands associated with tables)
The Kernel receives a bunch of requests either by CLI or a socket request. It "plans them" using a Round Robin scheduling algorithm, with a _Quantum_ number set in the config and then sends each one into the corresponding Memory process in the Memory Pool.

For each table created, you can set the consistency type, the types are:
- **Strong Consistency:** Only one Memory of the pool is in charge of handling requests related to that table.
- **Strong-hash Consistency:** It can use more than 1 Memory to process the table's request but depending on the key the operation will go to a particular Memory that handles all operations on that key. This is achieved by a hash function applied on the key.
- **Eventual Consistency:** Pick a Memory at random or using a Round Robin algorithm to pick one. This will not ensure consistent reads. If you INSERT a key-value pair and immediately SELECT it, it probably won't return the latest value.

### For SET requests
Once a new set request is received, Lissandra:
1. Sends the request to the corresponding memory.
2. The Memory process sets the value in memory.
3. After a while, the Memory process _journals_ it's tables and values to the LFS. LFS saves them in memory.
4. After a while, the LFS _dumps_ it's memtable to dump temporary files.
5. After a while, the LFS _compacts_ the new dumped temp files with the old files and discards all old values. To save space in disk.


### For GET requests
Once a new get request is received, Lissandra:
1. Checks first in the memtable inside the Kernel process
2. If it's not there, it consults the value in the correspondent Memory from the Memory Pool.
3. If it's not there, the request is sent to the file system to check in the newly dumped files.
4. If it's not there, then it checks for the compacted files.
5. If it's not there, it doesn't exist.

If it was found in one of these steps, we can ensure it's the latest value (unless we're using Eventual Consistency - refer above).

### Other commands
- CREATE
- INSERT
- SELECT
- DROP
- RUN
- DESCRIBE
- JOURNAL
- ADD
- METRICS
- CLEAR
