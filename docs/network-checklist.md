# Network-first checklist

- [ ] Each VM has ZeroTier installed, joined to the same authorized network, and is reachable by ZeroTier IP.
- [ ] Record OS, Java version, Python version, RAM, Spark/Hadoop versions (avoid committing credentials).
- [ ] Test reachability, then test specific service ports; ping alone is not sufficient.
- [ ] Node 2 HDFS NameNode starts; both Node 2 and Node 3 DataNodes register.
- [ ] HDFS replication factor 2 is verified on actual files using HDFS status tools.
- [ ] Node 3 Spark Master starts; workers on Nodes 2 and 3 register.
- [ ] Node 1 submits a tiny remote Spark ingestion job with its driver on Node 1.
- [ ] Node 3 launches a small indexing/export test with its driver on Node 3.
- [ ] Node 4 receives and loads a completed export without contacting HDFS at query time.
- [ ] Restart sequence and logs documented; failure demo does not unexpectedly destroy data.

Do not assume Kafka broker failures predict HDFS/Spark behavior: validate each service independently.
