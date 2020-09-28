public class ConnectFEServiceHandler implements ConnectFEService.Iface {
    public boolean connectFE(String hostBE, int portBE) throws IllegalArgument, org.apache.thrift.TException {
        System.out.println("Get connection request from BE node: " + hostBE + ":" + portBE);
        return true;
    }
}
