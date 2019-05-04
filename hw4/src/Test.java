import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.concurrent.TimeUnit;

public class Test {
    private static final String[] coordinators = {"localhost", "localhost"};
    private static final int[] ports = {1099, 1100};

    public static void main(String[] args) {
        try {
            System.out.println("sleeping");
            TimeUnit.SECONDS.sleep(3);
            Registry registry;
            CoordinatorInterface coordinator;
            registry = LocateRegistry.getRegistry(coordinators[0], ports[0]);
            coordinator = (CoordinatorInterface) registry.lookup(coordinators[0] + "-Coordinator");
            System.out.println("Get result: " + coordinator.get("a"));
            System.out.println("Delete result: " + coordinator.delete("a"));
            System.out.println("Get result: " + coordinator.get("a"));
            System.out.println("Put result: " + coordinator.put("a", "b"));
            System.out.println("Get result: " + coordinator.get("a"));
            System.out.println("Get result: " + coordinator.get("a"));
            System.out.println("Get result: " + coordinator.get("a"));
            System.out.println("Delete result: " + coordinator.delete("a"));
            System.out.println("Delete result: " + coordinator.delete("b"));
            System.out.println("Delete result: " + coordinator.delete("c"));
            System.out.println("Delete result: " + coordinator.delete("d"));
            System.out.println("Get result: " + coordinator.get("a"));
            System.out.println("Get result: " + coordinator.get("b"));
            System.out.println("Get result: " + coordinator.get("c"));
            System.out.println("Get result: " + coordinator.get("d"));
            System.out.println("Put result: " + coordinator.put("a", "b"));
            System.out.println("Put result: " + coordinator.put("b", "c"));
            System.out.println("Put result: " + coordinator.put("c", "d"));
            System.out.println("Put result: " + coordinator.put("d", "e"));
            System.out.println("Get result: " + coordinator.get("a"));
            System.out.println("Get result: " + coordinator.get("b"));
            System.out.println("Get result: " + coordinator.get("c"));
            System.out.println("Get result: " + coordinator.get("d"));
            System.out.println("Delete result: " + coordinator.delete("a"));
            System.out.println("Delete result: " + coordinator.delete("b"));
            System.out.println("Delete result: " + coordinator.delete("c"));
            System.out.println("Delete result: " + coordinator.delete("d"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
