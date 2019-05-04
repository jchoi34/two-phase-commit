import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ReplicaInterface extends Remote {
    static final String name = "kvstore";
    public String doGet(String key) throws RemoteException;
    public boolean canCommit(Transaction t) throws RemoteException;
    public void doCommit(Transaction t) throws RemoteException;
    public void doAbort(Transaction t) throws RemoteException;
    public TransactionStatus getDecision(Transaction t) throws RemoteException;
}
