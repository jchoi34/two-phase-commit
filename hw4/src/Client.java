import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Client {
    private static final String[] coordinators = {"n0000", "n0001"};
    public static void main(String[] args) {
        String[] inputs;
        String userInput;
        try {
            BufferedReader stdIn =
                    new BufferedReader(
                            new InputStreamReader(System.in));
            System.out.println("Enter a command or exit to quit.");
            while ((userInput = stdIn.readLine()) != null && !userInput.equalsIgnoreCase("exit")) {
                inputs = userInput.split(" ");
                if (inputs.length > 1) {
                    if (inputs[0].equalsIgnoreCase("get")) {
                        System.out.println(doCommand(inputs[1], null, 0));
                    } else if (inputs.length > 2 && inputs[0].equalsIgnoreCase("put")) {
                        System.out.println(doCommand(inputs[1], inputs[2], 1));
                    } else if (inputs[0].equalsIgnoreCase("delete")) {
                        System.out.println(doCommand(inputs[1], null, 2));
                    } else {
                        System.out.println("Invalid command");
                    }
                } else {
                    System.out.println("Invalid command");
                }
                System.out.println("Enter a command or exit to quit.");
            }
        } catch (Exception e) {
        }
    }

    private static String doCommand(String key, String value, int operation) {
        Registry registry;
        CoordinatorInterface coordinator;
        for (String c : coordinators) {
            try {
                registry = LocateRegistry.getRegistry(c, 1099);
                coordinator = (CoordinatorInterface) registry.lookup(c + "-Coordinator");
                if (operation == 0) {
                    return coordinator.get(key);
                } else if (operation == 1) {
                    if (coordinator.put(key, value))
                        return "Put command complete.";
                    else
                        return "Put command failed.";
                } else {
                    if (coordinator.delete(key))
                        return "Delete command complete.";
                    else
                        return "Delete command failed.";
                }
            } catch (Exception e) {
            }
        } // TODO: may need to change this
        return "Command may have failed (system failure).";
    }
}
