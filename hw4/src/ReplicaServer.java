import java.io.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class ReplicaServer implements ReplicaInterface {

    private static String serverName;
    private static HashMap<String, ReentrantLock> locks;
    private static HashMap<Long, Transaction> transactions;
    private static final String[] peers = {"n0000", "n0001", "n0004", "n0005", "n0008"};

    // do initialization and recovery if necessary
    public static void main(String[] args) {
        try {
            Connection c;
            Statement stmt;
            serverName = args[0];
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:" + serverName + ".db");
            stmt = c.createStatement();
            stmt.executeUpdate("create table if not exists kvstore " +
                    "(k text primary key not null, " +
                    "v text not null)");
            stmt.close();
            c.close();
            locks = new HashMap<>();
            transactions = new HashMap<>();
            // do any necessary recovery
            recover();
            BufferedWriter writer = new BufferedWriter(new FileWriter(serverName + "_log.txt", true));
            writer.append("Initializing " + serverName + "\n");
            writer.close();
            ReplicaInterface server = new ReplicaServer();
            ReplicaInterface stub =
                    (ReplicaInterface) UnicastRemoteObject.exportObject(server, 1099);
            Registry registry = LocateRegistry.createRegistry(1099);
            registry.rebind(name, stub);
            System.out.println("Server bound");
            System.out.println(registry.toString());
            // start task to check idle transactions
            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new getDecisions(), 180000, 180000);
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
            File file = new File(serverName + "_log.txt");
            file.createNewFile();
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            while ((line = br.readLine()) != null) {
                // process the line.
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
                String status = tokens[8];
                if (status.contains("DONE") || status.contains("ABORT")) { // GLOBAL_COMMIT_DONE or VOTE_ABORT or GLOBAL_ABORT
                    completedTransactions.add(id);
                } else if (!completedTransactions.contains(id)) {
                    Transaction t = new Transaction(id, tokens[3], tokens[5], Boolean.parseBoolean(tokens[7]));
                    if (status.equals("GLOBAL_COMMIT")) { // GLOBAL_COMMIT
                        recoveryOperation(t);
                    } else if (status.contains("VOTE")) { // VOTE_COMMIT
                        for (int i = 0; i < peers.length; i++) {
                            try {
                                TransactionStatus transactionStatus;
                                Registry registry = LocateRegistry.getRegistry(peers[i], 1099);
                                CoordinatorInterface coordinator;
                                ReplicaInterface replica;
                                if (i < 2) {
                                    coordinator = (CoordinatorInterface) registry.lookup(peers[i] + "-Coordinator");
                                    transactionStatus = coordinator.getDecision(t);
                                } else if (peers[i].equals(serverName)) {
                                    continue;
                                } else {
                                    replica = (ReplicaInterface) registry.lookup(name);
                                    transactionStatus = replica.getDecision(t);
                                }
                                if (transactionStatus == TransactionStatus.COMMITTED) {
                                    BufferedWriter writer = new BufferedWriter(new FileWriter(serverName + "_log.txt", true));
                                    writer.append(t.toString() + " GLOBAL_COMMIT" + "\n");
                                    writer.close();
                                    recoveryOperation(t);
									writer = new BufferedWriter(new FileWriter(serverName + "_log.txt", true));
                                    writer.append(t.toString() + " GLOBAL_COMMIT_DONE" + "\n");
                                    writer.close();
                                } else if (transactionStatus == TransactionStatus.ABORTED) {
                                    BufferedWriter writer = new BufferedWriter(new FileWriter(serverName + "_log.txt", true));
                                    writer.append(t.toString() + " GLOBAL_ABORT" + "\n");
                                    writer.close();
                                } else if (transactionStatus == TransactionStatus.ACTIVE) {
                                    doPutOrDelete(t);
                                }
                                break;
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
					completedTransactions.add(id);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void recoveryOperation(Transaction t) {
        Connection c = null;
        try {
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:" + serverName + ".db");
            c.setAutoCommit(false);
            if (t.isPut()) {
                insert(t, c);
            } else {
                delete(t, c);
            }
            c.commit();
            c.close();
            BufferedWriter writer = new BufferedWriter(new FileWriter(serverName + "_log.txt", true));
            writer.append(t.toString() + " GLOBAL_COMMIT_DONE" + "\n");
            writer.close();
        } catch (Exception e) {
            try {
                if (c != null)
                    c.close();
            } catch (Exception e2) {
                e2.printStackTrace();
            }
            e.printStackTrace();
        }
    }

    // put helper function
    private static void insert(Transaction t, Connection c) throws SQLException {
        PreparedStatement stmt;
        stmt = c.prepareStatement("insert into kvstore (k, v) values (?, ?)");
        stmt.setString(1, t.getKey());
        stmt.setString(2, t.getValue());
        stmt.executeUpdate();
        stmt.close();
    }

    // remove helper function
    private static void delete(Transaction t, Connection c) throws SQLException {
        PreparedStatement stmt;
        stmt = c.prepareStatement("delete from kvstore where k = ?");
        stmt.setString(1, t.getKey());
        stmt.executeUpdate();
        stmt.close();
    }

    private static class getDecisions extends TimerTask {
        public void run() {
            for (int i = 0; i < peers.length; i++) {
                try {
                    Iterator<Transaction> iterator = transactions.values().iterator();
                    if (!iterator.hasNext()) {
                        return;
                    }
                    TransactionStatus status;
                    Registry registry = LocateRegistry.getRegistry(peers[i], 1099);
                    CoordinatorInterface coordinator = null;
                    ReplicaInterface replica = null;
                    if(i < 2) {
                        coordinator = (CoordinatorInterface) registry.lookup(peers[i] + "-Coordinator");
                    } else if (peers[i].equals(serverName)) {
                        continue;
                    } else {
                        replica = (ReplicaInterface) registry.lookup(name);
                    }
                    while (iterator.hasNext()) {
                        Transaction t = iterator.next();
                        long id = t.getId();
                        if(i < 2) {
                            status = coordinator.getDecision(t);
                        } else {
                            status = replica.getDecision(t);
                        }
                        Connection c = t.getConnection();
                        if (c != null) {
                            if (status == TransactionStatus.COMMITTED) {
                                c.commit();
                                c.close();
                            } else if (status == TransactionStatus.ABORTED) {
                                c.rollback();
                                c.close();
                            }
                        }
                        if (status != TransactionStatus.ACTIVE) {
                            transactions.remove(id);
                        }
                    }
                    return;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }

    // retrieve the kv pair from sqlite
    public String doGet(String key) {
        Connection c = null;
        PreparedStatement stmt = null;
        ResultSet results = null;
        StringBuilder stringBuilder = new StringBuilder();

        try {
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:" + serverName + ".db");
            stmt = c.prepareStatement("select v from kvstore where k = ?");
            stmt.setString(1, key);
            results = stmt.executeQuery();
            if (results.next()) {
                stringBuilder.append("Value: ");
                stringBuilder.append(results.getString(1));
                results.close();
                stmt.close();
                c.close();
                return stringBuilder.toString();
            } else {
                results.close();
                stmt.close();
                c.close();
                return "Value for key: " + key + " not found.";
            }
        } catch (Exception e) {
            if (results != null) {
                try {
                    results.close();
                } catch (Exception e2) {
                    e2.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception e2) {
                    e2.printStackTrace();
                }
            }
            if (c != null) {
                try {
                    c.close();
                } catch (Exception e2) {
                    e2.printStackTrace();
                }
            }
            e.printStackTrace();
            return e.getClass().getName() + ": " + e.getMessage();
        }
    }

    // insert or delete from the kv store but do not commit (this is done later when doCommit is called for this transaction)
    // if any issue occurs we log abort and tell the coordinator we aborted
    // *lock for the key must be available, otherwise we abort
    private static boolean doPutOrDelete(Transaction t) {
        Connection c = null;
        try {
            String key = t.getKey();
            if (!locks.containsKey(key)) {
                locks.put(key, new ReentrantLock());
            }
            ReentrantLock lock = locks.get(key);
            if (!lock.tryLock()) {
                throw new Exception("Could not acquire lock for key: " + key);
            }
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:" + serverName + ".db");
            c.setAutoCommit(false);
            if(t.isPut()) {
                insert(t, c);
            } else {
                delete(t, c);
            }
            t.setConnection(c);
            transactions.put(t.getId(), t);
            return true;
        } catch (Exception e) {
            ErrorHandler(c, e, t);
            return false;
        }
    }

    // coordinator will try a insert or delete to the kv store and if no problems occur
    // then we tell the coordinator that we vote to commit
    public boolean canCommit(Transaction t) {
        boolean result;
        result = doPutOrDelete(t);
        if (result) {
            try {
                BufferedWriter writer = new BufferedWriter(new FileWriter(serverName + "_log.txt", true));
                writer.append(t.toString() + " VOTE_COMMIT" + "\n");
                writer.close();
                return true;
            } catch (Exception e) {
                e.printStackTrace();
                doAbort(t);
                return false;
            }
        }
        return false;
    }

    // do the actual committing of the sql statement here
    public void doCommit(Transaction t) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(serverName + "_log.txt", true));
            writer.append(t.toString() + " GLOBAL_COMMIT" + "\n");
            writer.close();
            long id = t.getId();
            Connection c = transactions.get(id).getConnection();
            if (c != null) {
                c.commit();
                c.close();
            }
            transactions.remove(id);
            String key = t.getKey();
            ReentrantLock lock = locks.get(key);
            if (lock != null) {
                lock.unlock();
            }
            if (!t.isPut()) {
                locks.remove(key);
            }
            writer = new BufferedWriter(new FileWriter(serverName + "_log.txt", true));
            writer.append(t.toString() + " GLOBAL_COMMIT_DONE" + "\n");
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // abort the sql statement
    public void doAbort(Transaction t) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(serverName + "_log.txt", true));
            writer.append(t.toString() + " GLOBAL_ABORT" + "\n");
            writer.close();
            long id = t.getId();
            Connection c = transactions.get(id).getConnection();
            if (c != null) {
                c.rollback();
                c.close();
            }
            transactions.remove(id);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ReentrantLock lock = locks.get(t.getKey());
            if (lock != null) {
                lock.unlock();
            }
        }
    }

    // scan the log for the transaction from the end to the start and if the result was to commit the transaction then return true
    public TransactionStatus getDecision(Transaction t) {
        Stack<String> strings = new Stack<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(serverName + "_log.txt"));
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
                } else if (status.contains("ABORT")) {
                    return TransactionStatus.ABORTED;
                }
            }
        }
        // in theory this should never be reached. Unless something wrong happens with logging on the coordinator or the transaction id is overflowed or modified.
        return TransactionStatus.NONEXISTENT;
    }

    // error handler for put and delete. This will log an abort and rollback the transaction.
    private static void ErrorHandler(Connection c, Exception e, Transaction t) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(serverName + "_log.txt", true));
            writer.append(t.toString() + " VOTE_ABORT" + "\n");
            writer.close();
        } catch (Exception e2) {
            e2.printStackTrace();
        }
        System.err.println(e.getClass().getName() + ": " + e.getMessage());
        if (c != null) {
            try {
                System.err.println("Transaction being rolled back.");
                c.rollback();
                c.close();
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }
}
