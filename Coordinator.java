import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Coordinator {
    static final Map<String, NodeInfo> nodeMap = new ConcurrentHashMap<>();    // ConcurrentHashMap: (key: node addr - hostname + port, val: node-info)

    // TODO: sychronized or not?
    public static void addNode(String address, NodeInfo info) {
        nodeMap.put(address, info);
    }

    // TODO: sychronized or not?
    public static boolean containsNode(String address) {
        return nodeMap.containsKey(address);
    }

    // TODO: sychronized or not?
    public static List<String> getAvailableNodes() {
        List<String> availableNodes = new ArrayList<>();
        synchronized (nodeMap) {
            for (Map.Entry<String, NodeInfo> entry : nodeMap.entrySet()) {
                NodeInfo node = entry.getValue();
//                if (!node.isBusy()) {
                    availableNodes.add(entry.getKey());
//                    node.setBusy(true);
//                }
            }
        }
        return availableNodes;
    }
}
