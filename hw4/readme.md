Compiling the programs)

From the folder with the code run this command.
javac -cp sqlite-jdbc-3.27.2.1.jar *.java

* please keep all files in src within the same folder as eachother (no file organization right now)

Running the coordinator)

nohup java Coordinator <n0000 or n0001> &

example:
Run this from n0000
nohup java Coordinator n0000 &

Run this from n0001
nohup java Coordinator n0001 &

* n0001 is a backup coordinator for n0000 so either n0000 or n0001 or both can be run. It is preferable to have n0000 running since the client and replicas will try it first.

Running the replica servers)

nohup java -cp .:sqlite-jdbc-3.27.2.1.jar ReplicaServer <server name> &

These commands must be run from n0004, n0005, and n0008 respectively.
nohup java -cp .:sqlite-jdbc-3.27.2.1.jar ReplicaServer n0004 &
nohup java -cp .:sqlite-jdbc-3.27.2.1.jar ReplicaServer n0005 &
nohup java -cp .:sqlite-jdbc-3.27.2.1.jar ReplicaServer n0008 &
* all three replica servers must be up for the coordinator to respond to requests properly.

Running the client and test program)

From n0009 or above run one of these commands.
java Client

or

java Test

Terminating the java program)
kill $(pgrep java)
