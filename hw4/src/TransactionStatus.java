import java.io.Serializable;

public enum TransactionStatus implements Serializable {
    COMMITTED, ABORTED, ACTIVE, NONEXISTENT
}
