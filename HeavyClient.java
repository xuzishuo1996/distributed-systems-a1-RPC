import java.util.*;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;

public class HeavyClient {
    public static void main(String [] args) {
        try {
            TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
            TTransport transport = new TFramedTransport(sock);
            TProtocol protocol = new TBinaryProtocol(transport);
            BcryptService.Client client = new BcryptService.Client(protocol);
            transport.open();

            for (int i = 0; i < 3; ++i) {
                int n = 16;
                short logRounds = 10;
                List<String> password = genPasswordList(16);

                long startTime = System.currentTimeMillis();

                List<String> hash = client.hashPassword(password, logRounds);

                long endTime = System.currentTimeMillis();
                System.out.println("Throughput for logRounds=" + logRounds + ": " + n * 1000f/(endTime-startTime));
                System.out.println("Latency for logRounds=" + logRounds + ": " + (endTime-startTime)/n);

                boolean succeed = true;
                List<Boolean> result = client.checkPassword(password, hash);
                for (Boolean b: result) {
                    if (!b) {
                        succeed = false;
                        break;
                    }
                }
                System.out.println("Round " + i + " Check: " + succeed);
            }

            transport.close();
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    public static List<String> genPasswordList(int n) {
        Random rand = new Random();
        List<String> l = new ArrayList<>(1024);
        String someBigPassword = "faldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurvcvmvcmdoiZZ";
        for (int i = 0; i < n; i++) {
            l.add(someBigPassword + rand.nextInt(10));
        }
        return l;
    }
}
