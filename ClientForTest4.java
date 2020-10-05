import java.util.List;
import java.util.ArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;

public class ClientForTest4 {
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

			// Test 4: list with empty password check - should not throw exception
			passwords.add("");
			List<String> hashes = client.hashPassword(passwords, (short)10);
			List<Boolean> result = client.checkPassword(passwords, hashes);
			System.out.println("list with empty password check result: " + result);
//			boolean match = true;
//			for (Boolean b: result) {
//				match = b;
//			}
//			System.out.println("Should be true: " + match);

			transport.close();
		} catch (TException x) {
			x.printStackTrace();
		}
	}
}



