import java.io.Serializable;
import java.sql.Connection;

public class Transaction implements Serializable {
    private static final long serialVersionUID = 1L;
    private long id;
    private String key;
    private String value;
    private boolean put; // if put is true we are inserting a new kv pair. Otherwise, it is a delete operation.
    private Connection connection;

    public Transaction(long id, String key, String value, boolean put) {
        this.id = id;
        this.key = key;
        this.value = value;
        this.put = put;
    }

    public long getId() {
        return id;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public boolean isPut() {
        return put;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    @Override
    public String toString() {
        return "Id: " + id + " Key: " + key + " Value: " + value + " Put: " + put;
    }
}
