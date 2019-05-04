import java.rmi.Remote;
import java.rmi.RemoteException;

public interface CoordinatorInterfaceForReplicas extends Remote {
    public TransactionStatus getDecision(Transaction t) throws RemoteException;
}
