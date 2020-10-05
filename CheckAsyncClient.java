import org.apache.thrift.async.AsyncMethodCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

public class CheckAsyncClient implements Callable<List<Boolean>> {
    private final List<String> password;
    private final List<String> hash;
    private final List<String> availableBEs;
    private final int i;

    private final CountDownLatch latch;

    List<Boolean> subResult;

    public CheckAsyncClient(List<String> password, List<String> hash, List<String> availableBEs, int i) {
        this.password = password;
        this.hash = hash;
        this.availableBEs = availableBEs;
        this.i = i;
        this.latch = new CountDownLatch(1);
    }

    @Override
    public List<Boolean> call() {
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
            List<String> subPassword = password.subList(start, end);
            List<String> subHash = hash.subList(start, end);

            NodeInfo info = Coordinator.nodeMap.get(availableBEs.get(i));

            BcryptService.AsyncClient client = info.getAsyncClient();

            NodeInfo currInfo = Coordinator.nodeMap.get(availableBEs.get(i));
            currInfo.setBusy(true);
            currInfo.addLoad(splitSize, (short)1);

            // for test only
            System.out.println("hashing offload to BE " + i + ": " + address[0] + " " + address[1]);

            client.checkPassword(subPassword, subHash, new CheckCallback());

            currInfo.setBusy(false);
            currInfo.subLoad(splitSize, (short)1);

            latch.await();
            return subResult;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    class CheckCallback implements AsyncMethodCallback<List<Boolean>> {

        public void onComplete(List<Boolean> response) {
            subResult = response;
            latch.countDown();
        }

        public void onError(Exception e) {
            e.printStackTrace();
            latch.countDown();
        }
    }
}
