import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.io.IOException;

public class NodeInfo {
    // TODO: add hostname and postname or not?
    private volatile boolean busy;
    private double load;    // because of the return val of Math.pow() is double.
    private final BcryptService.AsyncClient asyncClient;

    public NodeInfo(String hostBE, int portBE) throws IOException {
        busy = false;
        this.load = 0;

        // create the async client for transport reuse
        TNonblockingTransport transport = new TNonblockingSocket(hostBE, portBE);
        TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
        TAsyncClientManager clientManager = new TAsyncClientManager();
        asyncClient = new BcryptService.AsyncClient(protocolFactory, clientManager, transport);
    }

    public BcryptService.AsyncClient getAsyncClient() {
        return asyncClient;
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
