import java.io.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Stack;

public class Coordinator implements CoordinatorInterface {
    private static long transactionId = 0;
    private static final String[] replicaServers = {"n0004", "n0005", "n0008"};
    private static String serverName;

    // TODO: Prevent transaction id overflow and make the log delete old entries after getting to a certain size.
    // TODO: Possibly add checkpoints to the log to help shorten the log scan for redoing/undoing actions while scanning from bottom to top.

    // do initialization and recovery if necessary
    // all operations will be locked until recovery and initialization is finished
    public static void main(String[] args) {
        try {
            serverName = args[0];
            // do the recovery
            recover();
			if(serverName.equals("n0000")) {
				BufferedWriter writer = new BufferedWriter(new FileWriter(serverName + "_log.txt", true));
				writer.append("Initializing " + serverName + "\n");
				writer.close();
			}
            CoordinatorInterface coordinatorForClient = new Coordinator();
            CoordinatorInterface stubForClient =
                    (CoordinatorInterface) UnicastRemoteObject.exportObject(coordinatorForClient, 1099);
            Registry registry = LocateRegistry.createRegistry(1099);
            registry.rebind(serverName + "-Coordinator", stubForClient);
            System.out.println("Server bound");
            System.out.println(registry.toString());
        } catch (Exception e) {
            System.err.println("Server exception:");
            e.printStackTrace();
        }
    }

    @SuppressWarnings("Duplicates")
    // recovery function
    private static void recover() {
        Stack<String> strings = new Stack<>();
        HashSet<Long> completedTransactions = new HashSet<>(500);

        try {
            File file = new File("n0000_log.txt");
            file.createNewFile();
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            while ((line = br.readLine()) != null) {
                // process the line.
                System.out.println("String: " + line);
                strings.push(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        while (!strings.empty()) {
            try {
                String[] tokens = strings.pop().split(" ");
                if (tokens[0].contains("Init")) {
                    continue;
                }
                long id = Long.parseLong(tokens[1]);
                if(id >= transactionId) {
                    transactionId = id + 1;
                }
                String status = tokens[8];
                if (status.contains("DONE")) {
                    completedTransactions.add(id);
                } else if (!completedTransactions.contains(id)) {
                    if (status.equals("GLOBAL_COMMIT")) {
                        doCommit(new Transaction(id, tokens[3], tokens[5], Boolean.parseBoolean(tokens[7])));
                    } else if (status.equals("GLOBAL_ABORT")) {
                        doAbort(new Transaction(id, tokens[3], tokens[5], Boolean.parseBoolean(tokens[7])), replicaServers.length);
                    } else if (status.equals("START")) {
                        Transaction t = new Transaction(id, tokens[3], tokens[5], Boolean.parseBoolean(tokens[7]));
                        try {
                            BufferedWriter writer = new BufferedWriter(new FileWriter("n0000_log.txt", true));
                            writer.append(t.toString() + " GLOBAL_ABORT" + "\n");
                            writer.close();
                        } catch (Exception e2) {
                            e2.printStackTrace();
                        }
                        doAbort(t, replicaServers.length);
                    }
					completedTransactions.add(id);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // choose a random replica server to read from
    public String get(String key) {
        try {
            int random = (int) (Math.random() * 3);
            Registry registry = LocateRegistry.getRegistry(replicaServers[random]);
            ReplicaInterface replica = (ReplicaInterface) registry.lookup(ReplicaInterface.name);
            return replica.doGet(key);
        } catch (Exception e) {
            return e.getClass().getName() + ": " + e.getMessage();
        }
    }

    // If all the replica servers vote to commit then log a commit message and
    // send a commit command to all the replica servers and then return true.
    public boolean put(String key, String value) {
        Transaction t = new Transaction(transactionId, key, value, true);
        return doCommand(t);
    }

    // If all the replica servers vote to commit then log a commit message and
    // send a commit command to all the replica servers and then return true.
    public boolean delete(String key) {
        Transaction t = new Transaction(transactionId, key, null, false);
        return doCommand(t);
    }

    // start of transaction
    private boolean logStart(Transaction t) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("n0000_log.txt", true));
            writer.append(t.toString() + " START" + "\n");
            writer.close();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    // helper method to do the actual delete/put commands on the replica servers
    private boolean doCommand(Transaction t) {
        try {
            if (!logStart(t)) {
                return false;
            }
            transactionId++;
            if (!getVotes(t)) {
                return false;
            }
            BufferedWriter writer = new BufferedWriter(new FileWriter("n0000_log.txt", true));
            writer.append(t.toString() + " GLOBAL_COMMIT" + "\n");
            writer.close();
            doCommit(t);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    // send commit command to the replicas
    private static void doCommit(Transaction t) {
        for (int i = 0; i < replicaServers.length; i++) {
            try {
                Registry registry = LocateRegistry.getRegistry(replicaServers[i]);
                ReplicaInterface replica = (ReplicaInterface) registry.lookup(ReplicaInterface.name);
                replica.doCommit(t);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter( "n0000_log.txt", true));
            writer.append(t.toString() + " GLOBAL_COMMIT_DONE" + "\n");
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // scan the log for the transaction from the end to the start and if the vote was to commit the transaction then return true
    public TransactionStatus getDecision(Transaction t) {
        Stack<String> strings = new Stack<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader("n0000_log.txt"));
            String line;
            while ((line = br.readLine()) != null) {
                // process the line.
                strings.push(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        while (!strings.empty()) {
            String[] tokens = strings.pop().split(" ");
            if (tokens[0].contains("Init")) {
                continue;
            } else if (Long.parseLong(tokens[1]) == t.getId()) {
                String status = tokens[8];
                if (status.equals("GLOBAL_COMMIT")) {
                    return TransactionStatus.COMMITTED;
                } else if (status.equals("GLOBAL_ABORT")) {
                    return TransactionStatus.ABORTED;
                } else if (status.equals("STARTED")) {
                    return TransactionStatus.ACTIVE;
                }
            }
        }
        // in theory this should never be reached. Unless something wrong happens with logging on the coordinator or the transaction id is overflowed or modified.
        return TransactionStatus.NONEXISTENT;
    }

    // Start the two phase commit by getting votes from the replica servers.
    // If all the replica servers vote to commit then we return true.
    private boolean getVotes(Transaction t) {
        for (int i = 0; i < replicaServers.length; i++) {
            try {
                Registry registry = LocateRegistry.getRegistry(replicaServers[i]);
                ReplicaInterface replica = (ReplicaInterface) registry.lookup(ReplicaInterface.name);
                if (!replica.canCommit(t)) {
                    throw new Exception(replicaServers[i] + " cannot commit.");
                }
            } catch (Exception e1) {
                try {
                    BufferedWriter writer = new BufferedWriter(new FileWriter("n0000_log.txt", true));
                    writer.append(t.toString() + " GLOBAL_ABORT" + "\n");
                    writer.close();
                } catch (Exception e2) {
                    e2.printStackTrace();
                }
                doAbort(t, i);
                e1.printStackTrace();
                return false;
            }
        }
        return true;
    }

    // send abort command to the replicas
    private static void doAbort(Transaction t, int i) {
        for (int j = 0; j < i; j++) {
            try {
                Registry registry = LocateRegistry.getRegistry(replicaServers[j]);
                ReplicaInterface replica = (ReplicaInterface) registry.lookup(ReplicaInterface.name);
                replica.doAbort(t);
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("n0000_log.txt", true));
            writer.append(t.toString() + " GLOBAL_ABORT_DONE" + "\n");
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
