import org.apache.thrift.async.AsyncMethodCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

public class HashAsyncClient implements Callable<List<String>> {
    private final List<String> password;
    private final short logRounds;
    private final List<String> availableBEs;
    private final int i;

    private final CountDownLatch latch;

    public HashAsyncClient(List<String> password, short logRounds, List<String> availableBEs, int i) {
        this.password = password;
        this.logRounds = logRounds;
        this.availableBEs = availableBEs;
        this.i = i;
        this.latch = new CountDownLatch(1);
    }

    @Override
    public List<String> call() {
        try {
            String[] address = availableBEs.get(i).split(":");
            int n = password.size();
            int num = availableBEs.size();
            int splitSize = n / num;
            int start = splitSize * i;
            int end;
            if (i == num - 1) {
                end = n;
            } else {
                end = start + splitSize;    // exclusive
            }
            List<String> subList = password.subList(start, end);

            NodeInfo info = Coordinator.nodeMap.get(availableBEs.get(i));

            BcryptService.AsyncClient client = info.getAsyncClient();

            NodeInfo currInfo = Coordinator.nodeMap.get(availableBEs.get(i));
            currInfo.setBusy(true);
            currInfo.addLoad(splitSize, logRounds);

            // for test only
            System.out.println("hashing offload to BE " + i + ": " + address[0] + " " + address[1]);

            List<String> subResult = new ArrayList<>();
            client.hashPassword(subList, logRounds, new HashCallback(subResult));

            currInfo.setBusy(false);
            currInfo.subLoad(splitSize, logRounds);

            latch.await();
            return subResult;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    class HashCallback implements AsyncMethodCallback<List<String>> {
        private List<String> subResult;

        public HashCallback(List<String> subResult) {
            this.subResult = subResult;
        }

        public void onComplete(List<String> response) {
            this.subResult = response;
            latch.countDown();
        }

        public void onError(Exception e) {
            e.printStackTrace();
            latch.countDown();
        }
    }
}
