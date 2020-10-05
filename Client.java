import java.util.List;
import java.util.ArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;

public class Client {
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
//			List<String> hashes = client.hashPassword(passwords, (short)10);
//			List<Boolean> result = client.checkPassword(passwords, hashes);
//			boolean match = true;
//			for (Boolean b: result) {
//				match = b;
//			}
//			System.out.println("Should be true: " + match);


			/* IllegalArgument Tests */
//			// Test 1: Too many rounds: passed
//			List<String> hash = client.hashPassword(passwords, (short)50);
//			// Test 2: passwords and hashes length does not match: passed
//			List<String> hash = client.hashPassword(passwords, (short)10);
//			System.out.println("Before removing a hash: " + client.checkPassword(passwords, hash));
//			hash.remove(hash.size() - 1);
//			client.checkPassword(passwords, hash);

//			// Test 3: empty list test - should not throw exception
//			passwords = new ArrayList<>();
//			List<String> hashes = client.hashPassword(passwords, (short)10);
//			System.out.println("empty list hash is: " + hashes);
//			List<Boolean> res = client.checkPassword(passwords, hashes);
//			System.out.println("empty list check result is: " + res);

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

	private static void deprecatedMain(String[] args) {
		if (args.length != 3) {
			System.err.println("Usage: java Client FE_host FE_port password");
			System.exit(-1);
		}

		try {
			TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
			TTransport transport = new TFramedTransport(sock);
			TProtocol protocol = new TBinaryProtocol(transport);
			BcryptService.Client client = new BcryptService.Client(protocol);
			transport.open();

			List<String> password = new ArrayList<>();
			password.add(args[2]);
			List<String> hash = client.hashPassword(password, (short) 10);
			System.out.println("Password: " + password.get(0));
			System.out.println("Hash: " + hash.get(0));
			System.out.println("Positive check: " + client.checkPassword(password, hash));
			hash.set(0, "$2a$14$reBHJvwbb0UWqJHLyPTVF.6Ld5sFRirZx/bXMeMmeurJledKYdZmG");
			System.out.println("Negative check: " + client.checkPassword(password, hash));
			try {
				hash.set(0, "too short");
				List<Boolean> rets = client.checkPassword(password, hash);
				System.out.println("Exception check: no exception thrown");
			} catch (Exception e) {
				System.out.println("Exception check: exception thrown");
			}

			transport.close();
		} catch (TException x) {
			x.printStackTrace();
		}
	}
}



