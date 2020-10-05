import java.util.List;
import java.util.ArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;

public class ClientForTest2 {
	public static void main(String [] args) {
//		if (args.length < 3) {
//			System.err.println("Usage: java Client FE_host FE_port passwords");
//			System.exit(-1);
//		}

		try {
			TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
			TTransport transport = new TFramedTransport(sock);
			TProtocol protocol = new TBinaryProtocol(transport);
			BcryptService.Client client = new BcryptService.Client(protocol);
			transport.open();

			List<String> passwords = ClientUtility.genPasswords(128, 8);

			/* IllegalArgument Tests */
			// Test 2: passwords and hashes length does not match: passed
			List<String> hash = client.hashPassword(passwords, (short)10);
			System.out.println("Before removing a hash: " + client.checkPassword(passwords, hash));
			hash.remove(hash.size() - 1);
			client.checkPassword(passwords, hash);

			transport.close();
		} catch (TException x) {
			x.printStackTrace();
		}
	}
}



