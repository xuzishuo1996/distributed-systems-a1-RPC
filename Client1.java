import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.ArrayList;
import java.util.List;

public class Client1 {
    /*  1 client thread, 16 passwords per request,
        logRounds = 10.
     */
    static Logger log;

    private static final int LEN_OF_CHARS_PER_PASSWORD = 128;
    private static final int NUM_OF_PASSWORDS_PER_REQUEST = 16;
    private static final short LOG_ROUNDS = 10;
    private static final int NUM_OF_REQUESTS_PER_THREAD = 5;   //no interval between requests in a single thread
    private static final int NUM_OF_THREADS = 1;


    public static void main(String [] args) {
        BasicConfigurator.configure();
        log = org.apache.log4j.Logger.getLogger(Client1.class.getName());

        try {
            TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
            TTransport transport = new TFramedTransport(sock);
            TProtocol protocol = new TBinaryProtocol(transport);
            BcryptService.Client client = new BcryptService.Client(protocol);
            transport.open();

            long sumTime = 0;

            for (int i = 0; i < NUM_OF_REQUESTS_PER_THREAD; ++i) {
                List<String> passwords = ClientUtility.genPasswords(LEN_OF_CHARS_PER_PASSWORD, NUM_OF_PASSWORDS_PER_REQUEST);

                long startTime = System.currentTimeMillis();

                List<String> hashes = client.hashPassword(passwords, LOG_ROUNDS);

                long endTime = System.currentTimeMillis();
                sumTime += endTime - startTime;
//                log.info("Request " + i  + " Latency for logRounds=" + LOG_ROUNDS + ": " + (endTime-startTime)/NUM_OF_PASSWORDS_PER_REQUEST);

                // Check correctness
                List<Boolean> result = client.checkPassword(passwords, hashes);
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

            log.info("Throughput for logRounds=" + LOG_ROUNDS + ": " + NUM_OF_PASSWORDS_PER_REQUEST * NUM_OF_REQUESTS_PER_THREAD * 1000f/(sumTime));

            transport.close();
        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
