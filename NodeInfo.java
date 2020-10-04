import org.mindrot.jbcrypt.BCrypt;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class NodeInfo {
    // TODO: add hostname and postname or not?
    private boolean busy;
    private double load;    // because of the return val of Math.pow() is double.
    private final BcryptService.Client client;
    private final TTransport transport;

    public NodeInfo(String hostBE, int portBE) {
        busy = false;
        this.load = 0;

        // create the client for transport reuse
        TSocket sock = new TSocket(hostBE, portBE);
        transport = new TFramedTransport(sock);
        TProtocol protocol = new TBinaryProtocol(transport);
        client = new BcryptService.Client(protocol);
    }

    public BcryptService.Client getClient() {
        return client;
    }

    public TTransport getTransport() {
        return transport;
    }

    public boolean isBusy() {
        return busy;
    }

    public void setBusy(boolean busy) {
        this.busy = busy;
    }

    public double getLoad() {
        return load;
    }

    public void addLoad(int numOfPasswords, short logRounds) {
        load += numOfPasswords * Math.pow(2, logRounds);
        busy = true;
    }

    public void subLoad(int numOfPasswords, short logRounds) {
        load -= numOfPasswords * Math.pow(2, logRounds);
        if (load == 0) {
            busy = false;
        }
    }
}
