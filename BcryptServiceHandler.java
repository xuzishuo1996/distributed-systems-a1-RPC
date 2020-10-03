import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.mindrot.jbcrypt.BCrypt;

//import genJava.BcryptService;
//import genJava.IllegalArgument;

public class BcryptServiceHandler implements BcryptService.Iface {
	private final boolean isFE;
	private final static int BE_WORKER_THREADS_NUM = 2;
	private final static int BE_MULTI_THREAD_THRESHOLD = 5;		// should be greater than BE_WORKER_THREADS_NUM
	private final Logger log;

	public BcryptServiceHandler(boolean isFE) {
		this.isFE = isFE;
		BasicConfigurator.configure();
		log = Logger.getLogger(BcryptServiceHandler.class.getName());
	}

	public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException
	{
		int n = password.size();
		if (n == 0) {
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
			String[] res = new String[n];
			if (isFE) {
				List<String> availableBEs = Coordinator.getAvailableNodes();

				int num = availableBEs.size();
				if (num == 0) {
					// for test only
					log.info("hashing on FE!");

					hashPasswordHelper(input, logRounds, 0, n - 1, res);
					return new ArrayList<>(Arrays.asList(res));
				} else {
					List<String> result = new ArrayList<>();
					String[][] addresses = new String[num][2];
					int splitSize = n / num;
					for (int i = 0; i < num; ++i) {
						addresses[i] = availableBEs.get(i).split(":");
						int start = splitSize * i;
						int end;
						if (i == num - 1) {
							end = num;
						} else {
							end = start + splitSize;    // exclusive
						}
						List<String> subList = password.subList(start, end);

						TSocket sock = new TSocket(addresses[i][0], Integer.parseInt(addresses[i][1]));
						TTransport transport = new TFramedTransport(sock);
						TProtocol protocol = new TBinaryProtocol(transport);
						BcryptService.Client client = new BcryptService.Client(protocol);
						transport.open();

						NodeInfo currInfo = Coordinator.nodeMap.get(availableBEs.get(i));
						currInfo.setBusy(true);
						currInfo.addLoad(splitSize, logRounds);

						List<String> subResult = client.hashPassword(subList, logRounds);
						result.addAll(subResult);

						currInfo.setBusy(false);
						currInfo.subLoad(splitSize, logRounds);
						transport.close();
					}
					return result;
				}

			} else {	// BE
				// set BE to busy and add load
//				String host = InetAddress.getLocalHost().getHostName();

				if (n < BE_MULTI_THREAD_THRESHOLD) {
					// for test only
					log.info("single-threader hashing on BE!");

					hashPasswordHelper(input, logRounds, 0, n - 1, res);
				} else {
					// for test only
					log.info("multi-threaded hashing on BE!");

					int batchSize = n / BE_WORKER_THREADS_NUM;
					CountDownLatch latch = new CountDownLatch(BE_WORKER_THREADS_NUM);
					for (int i = 0; i < BE_WORKER_THREADS_NUM; ++i) {
						int start = batchSize * i;
						int end;
						if (i == BE_WORKER_THREADS_NUM - 1) {
							end = n - 1;
						} else {
							end = start + batchSize - 1;
						}
						log.info("multi-threaded hashing on BE - part " + i);
						new HashTask(input, logRounds, start, end, res, latch);
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
		int n = password.size();
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
			Boolean[] res = new Boolean[n];
			if (isFE) {
				List<String> availableBEs = Coordinator.getAvailableNodes();

				int num = availableBEs.size();
				if (num == 0) {
					// for test only
					log.info("checking on FE!");

					checkPasswordHelper(passwordArray, hashArray, 0, n - 1, res);
					return new ArrayList<>(Arrays.asList(res));
				} else {
					List<Boolean> result = new ArrayList<>();
					String[][] addresses = new String[num][2];
					int splitSize = n / num;
					for (int i = 0; i < num; ++i) {
						addresses[i] = availableBEs.get(i).split(":");
						int start = splitSize * i;
						int end;
						if (i == num - 1) {
							end = num;
						} else {
							end = start + splitSize;    // exclusive
						}
						List<String> subPassword = password.subList(start, end);
						List<String> subHash = hash.subList(start, end);

						TSocket sock = new TSocket(addresses[i][0], Integer.parseInt(addresses[i][1]));
						TTransport transport = new TFramedTransport(sock);
						TProtocol protocol = new TBinaryProtocol(transport);
						BcryptService.Client client = new BcryptService.Client(protocol);
						transport.open();

						NodeInfo currInfo = Coordinator.nodeMap.get(availableBEs.get(i));
						currInfo.setBusy(true);
						currInfo.addLoad(splitSize, (short)1);

						List<Boolean> subResult = client.checkPassword(subPassword, subHash);
						result.addAll(subResult);

						currInfo.setBusy(false);
						currInfo.subLoad(splitSize, (short)1);
						transport.close();
					}
					return result;
				}

			} else {
				// set BE to busy and add load
//				String host = InetAddress.getLocalHost().getHostName();

				if (n < BE_MULTI_THREAD_THRESHOLD) {
					// for test only
					log.info("single-threaded checking on BE!");

					checkPasswordHelper(passwordArray, hashArray, 0, n - 1, res);
				} else {
					// for test only
					log.info("multi-threaded checking on BE!");

					int batchSize = n / BE_WORKER_THREADS_NUM;
					CountDownLatch latch = new CountDownLatch(BE_WORKER_THREADS_NUM);
					for (int i = 0; i < BE_WORKER_THREADS_NUM; ++i) {
						int start = batchSize * i;
						int end;
						if (i == BE_WORKER_THREADS_NUM - 1) {
							end = n - 1;
						} else {
							end = start + batchSize - 1;
						}
						log.info("multi-threaded checking on BE - part " + i);
						new CheckTask(passwordArray, hashArray, start, end, res, latch);
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

	public void hashPasswordHelper(String[] input, short logRounds, int start, int end, String[] res)
	{
		for (int i = start; i <= end; ++i) {	// end: inclusive
			res[i] = BCrypt.hashpw(input[i], BCrypt.gensalt(logRounds));
		}
	}

	public void checkPasswordHelper(String[] password, String[] hash, int start, int end, Boolean[] res)
	{
		for (int i = start; i <= end; ++i) {	// end: inclusive
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
		log.info("Get connection request from BE node: " + hostBE + ":" + portBE);

		try {
			String address = hostBE + ":" + portBE;
			if (!Coordinator.containsNode(address)) {
				log.info("does not contain this node, register it at coordinator");
				Coordinator.addNode(address, new NodeInfo());

				// for test only
				int idx = 0;
				log.info("====== Current NodeMap =====");
				for (String s: Coordinator.nodeMap.keySet()) {
					++idx;
					log.info(idx + ": " + s);
				}
			}
		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}
	}
}
