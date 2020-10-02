import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.thrift.TException;
import org.mindrot.jbcrypt.BCrypt;

//import genJava.BcryptService;
//import genJava.IllegalArgument;

public class BcryptServiceHandler implements BcryptService.Iface {
	private final boolean isFE;
	private final static int BE_WORKER_THREADS_NUM = 2;
	private final static int BE_MULTI_THREAD_THRESHOLD = 5;

	public BcryptServiceHandler(boolean isFE) {
		this.isFE = isFE;
	}

	public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException
	{
		if (password.size() == 0) {
			return new ArrayList<>();
		}
		try {
			if (logRounds > 30) {	// 10 - 30
				throw new IllegalArgument("rounds exceeds maximum (30)");
			}

//			List<String> ret = new ArrayList<>();
//			for (String onePwd: password) {
//				String oneHash = BCrypt.hashpw(onePwd, BCrypt.gensalt(logRounds));
//				ret.add(oneHash);
//			}
//			return ret;

			String[] input = password.toArray(new String[0]);
			String[] res = new String[input.length];
			if (isFE) {

			} else {	// BE
				// set BE to busy and add load
//				String host = InetAddress.getLocalHost().getHostName();

				int size = password.size();
				if (size < BE_MULTI_THREAD_THRESHOLD) {
					hashPasswordHelper(input, logRounds, 0, size - 1, res);
				} else {
					int batchSize = size / BE_WORKER_THREADS_NUM;
					CountDownLatch latch = new CountDownLatch(BE_WORKER_THREADS_NUM);
					for (int i = 0; i < BE_WORKER_THREADS_NUM; ++i) {
						int start = batchSize * i;
						int end;
						if (i == BE_WORKER_THREADS_NUM - 1) {
							end = size - 1;
						} else {
							end = start + batchSize - 1;
						}
						new HashTask(input, logRounds, 0, end, res, latch);
					}
					latch.await();
				}

				// sub BE's load
			}
			return new ArrayList<>(Arrays.asList(res));

		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}
	}

	public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException
	{
		if (password.size() == 0 && hash.size() == 0) {
			return new ArrayList<>();
		}
		try {
			if (password.size() != hash.size()) {
				throw new IllegalArgument("the length of passwords and hashes does not match");
			}

//			List<Boolean> ret = new ArrayList<>();
//			for (int i = 0; i < password.size(); ++i) {
//				String onePwd = password.get(i);
//				String oneHash = hash.get(i);
//				ret.add(BCrypt.checkpw(onePwd, oneHash));
//			}
//			return ret;

			String[] passwordArray = password.toArray(new String[0]);
			String[] hashArray = hash.toArray(new String[0]);
			Boolean[] res = new Boolean[][passwordArray.length];
			if (isFE) {

			} else {
				// set BE to busy and add load
//				String host = InetAddress.getLocalHost().getHostName();

				int size = password.size();
				if (size < BE_MULTI_THREAD_THRESHOLD) {
					checkPasswordHelper(passwordArray, hashArray, 0, size - 1, res);
				} else {
					int batchSize = size / BE_WORKER_THREADS_NUM;
					CountDownLatch latch = new CountDownLatch(BE_WORKER_THREADS_NUM);
					for (int i = 0; i < BE_WORKER_THREADS_NUM; ++i) {
						int start = batchSize * i;
						int end;
						if (i == BE_WORKER_THREADS_NUM - 1) {
							end = size - 1;
						} else {
							end = start + batchSize - 1;
						}
						new CheckTask(passwordArray, hashArray, 0, end, res, latch);
					}
					latch.await();
				}

				// sub BE's load
			}

		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}
	}

	public void hashPasswordHelper(String[] input, short logRounds, int start, int end, String[] res)
	{
		for (int i = start; i < end; ++i) {
			res[i] = BCrypt.hashpw(input[i], BCrypt.gensalt(logRounds));
		}
	}

	public void checkPasswordHelper(String[] password, String[] hash, int start, int end, Boolean[] res)
	{
		for (int i = start; i < end; ++i) {
			res[i] = BCrypt.checkpw(password[i], hash[i]);
		}
	}

	class HashTask implements Runnable {
		private final String[] input;
		private final short logRounds;
		private final int start;
		private final int end;
		private final String[] res;
		private final CountDownLatch latch;

		public HashTask(String[] input, short logRounds, int start, int end, String[] res, CountDownLatch latch) {
			this.input = input;
			this.logRounds = logRounds;
			this.start = start;
			this.end = end;
			this.res = res;
			this.latch = latch;
		}

		@Override
		public void run() {
			hashPasswordHelper(input, logRounds, start, end, res);
			latch.countDown();
		}
	}

	class CheckTask implements Runnable {
		private final String[] password;
		private final String[] hash;
		private final int start;
		private final int end;
		private final Boolean[] res;
		private final CountDownLatch latch;

		public CheckTask(String[] password, String[] hash, int start, int end, Boolean[] res, CountDownLatch latch) {
			this.password = password;
			this.hash = hash;
			this.start = start;
			this.end = end;
			this.res = res;
			this.latch = latch;
		}

		@Override
		public void run() {
			checkPasswordHelper(password, hash, start, end, res);
			latch.countDown();
		}
	}

	public void connectFE(String hostBE, int portBE) throws IllegalArgument, org.apache.thrift.TException {
		// for test only
		System.out.println("Get connection request from BE node: " + hostBE + ":" + portBE);

		try {
			String address = hostBE + ":" + portBE;
			if (!Coordinator.containsNode(address)) {
				Coordinator.addNode(address, new NodeInfo());
			}
		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}
	}
}
