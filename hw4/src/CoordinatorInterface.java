import java.rmi.Remote;
import java.rmi.RemoteException;

public interface CoordinatorInterface extends Remote {
    public String get(String key) throws RemoteException;
    public boolean put(String key, String value) throws RemoteException;
    public boolean delete(String key) throws RemoteException;
    public TransactionStatus getDecision(Transaction t) throws RemoteException;
}
