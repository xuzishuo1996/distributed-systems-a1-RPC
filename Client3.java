import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

public class Client3 {
    /*  1 client thread, 16 passwords per request,
        logRounds = 10.
     */
    static Logger log;

    private static final int LEN_OF_CHARS_PER_PASSWORD = 128;
    private static final int NUM_OF_PASSWORDS_PER_REQUEST = 4;
    private static final short LOG_ROUNDS = 10;
    private static final int NUM_OF_REQUESTS_PER_THREAD = 5;   //no interval between requests in a single thread
    private static final int NUM_OF_THREADS = 4;

    private static CountDownLatch latch;
    private static Semaphore semaphore = new Semaphore(1);

    public static void main(String [] args) {
        BasicConfigurator.configure();
        log = org.apache.log4j.Logger.getLogger(Client3.class.getName());

        ExecutorService exec = Executors.newFixedThreadPool(NUM_OF_THREADS);
        latch = new CountDownLatch(NUM_OF_THREADS);

        long startTime = System.currentTimeMillis();

        try {
//            TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
//            TTransport transport = new TFramedTransport(sock);
//            TProtocol protocol = new TBinaryProtocol(transport);
//            BcryptService.Client client = new BcryptService.Client(protocol);
//            transport.open();

            for (int i = 0; i < NUM_OF_THREADS; ++i) {
                exec.execute(new Task(args[0], Integer.parseInt(args[1])));
            }

            latch.await();

            long endTime = System.currentTimeMillis();
            log.info("Throughput for logRounds=" + LOG_ROUNDS + ": " + NUM_OF_PASSWORDS_PER_REQUEST * NUM_OF_REQUESTS_PER_THREAD * NUM_OF_THREADS * 1000f/(endTime-startTime));
            exec.shutdown();
//            transport.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    static class Task implements Runnable {
        private final String hostFE;
        private final int portFE;

        Task(String hostFE, int portFE) {
            this.hostFE = hostFE;
            this.portFE = portFE;
        }

        @Override
        public void run() {
            try {
                TSocket sock = new TSocket(hostFE, portFE);
                TTransport transport = new TFramedTransport(sock);
                TProtocol protocol = new TBinaryProtocol(transport);
                BcryptService.Client client = new BcryptService.Client(protocol);
                transport.open();

                for (int i = 0; i < NUM_OF_REQUESTS_PER_THREAD; ++i) {
                    List<String> passwords = ClientUtility.genPasswords(LEN_OF_CHARS_PER_PASSWORD, NUM_OF_PASSWORDS_PER_REQUEST);

                    long startTime = System.currentTimeMillis();

//                    semaphore.acquire();
                    List<String> hashes = client.hashPassword(passwords, LOG_ROUNDS);

                    long endTime = System.currentTimeMillis();
//                    log.info("Request " + i  + " Latency for logRounds=" + LOG_ROUNDS + ": " + (endTime-startTime)/NUM_OF_PASSWORDS_PER_REQUEST);

                    // Check correctness
                    List<Boolean> result = client.checkPassword(passwords, hashes);
//                    semaphore.release();

                    boolean succeed = true;
                    for (Boolean b: result) {
                        if (!b) {
                            succeed = false;
                            break;
                        }
                    }
                    if (!succeed) {
                        log.info("Request " + i + " Check: " + succeed);
                    }
                }
                latch.countDown();
            } catch (TException e) {
                e.printStackTrace();
            }
        }
    }
}



