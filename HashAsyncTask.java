import org.apache.thrift.async.AsyncMethodCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

public class HashAsyncTask implements Callable<List<String>> {
    private final List<String> password;
    private final short logRounds;
    private final List<String> availableBEs;
    private final int i;
    private final CountDownLatch latch;

    List<String> subResult;

    public HashAsyncTask(List<String> password, short logRounds, List<String> availableBEs, int i) {
        this.password = password;
        this.logRounds = logRounds;
        this.availableBEs = availableBEs;
        this.i = i;
        this.latch = new CountDownLatch(1);

//        subResult = new ArrayList<>();
    }

    @Override
    public List<String> call() {
        try {
            int n = password.size();
            List<String> subList;
            if (n == 1) {
                subList = password;
            } else {
                int num = availableBEs.size();
                int splitSize = n / (num + 1);
                int start = splitSize * i;
                int end;
//            if (i == num - 1) {
//                end = n;
//            } else {
                end = start + splitSize;    // exclusive
//            }
                subList = password.subList(start, end);
            }

            NodeInfo currInfo = Coordinator.nodeMap.get(availableBEs.get(i));
            BcryptService.AsyncClient client = currInfo.getAsyncClient();

            while (currInfo.isBusy());
            currInfo.setBusy(true);
            synchronized (client) {
//                currInfo.setBusy(true);
//                currInfo.addLoad(splitSize, logRounds);

//                // for test only
//                System.out.println("hashing offload to BE " + i + ": " + address[0] + " " + address[1]);

                client.hashPassword(subList, logRounds, new HashCallback());

//                currInfo.setBusy(false);
//                currInfo.subLoad(splitSize, logRounds);

                latch.await();
                currInfo.setBusy(false);
            }
            return subResult;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    class HashCallback implements AsyncMethodCallback<List<String>> {

        public void onComplete(List<String> response) {
            subResult = response;
            latch.countDown();
        }

        public void onError(Exception e) {
            e.printStackTrace();
            latch.countDown();
        }
    }
}
