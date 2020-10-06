import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.mindrot.jbcrypt.BCrypt;

public class BcryptServiceHandler implements BcryptService.Iface {
	private final boolean isFE;
	private final static int MAX_WORKER_THREADS_NUM = 2;
	private final static int MULTI_THREAD_INPUT_THRESHOLD = 2;		// should be greater than BE_WORKER_THREADS_NUM
	private final Logger log;

//	private final static Semaphore[] semaphores = new Semaphore[]{new Semaphore(1), new Semaphore(1)};	// hardcode it to 2 because at most 2 BE nodes for grading

	public BcryptServiceHandler(boolean isFE) {
		this.isFE = isFE;
		BasicConfigurator.configure();
		log = Logger.getLogger(BcryptServiceHandler.class.getName());
//		semaphores = new Semaphore[2];
//		Arrays.fill(semaphores, new Semaphore(1));
	}

	public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException
	{
		int n = password.size();
		if (n == 0) {
			return new ArrayList<>();
		}
		try {
			if (logRounds < 4 || logRounds > 30) {	// 4 - 30
				throw new IllegalArgument("the logRounds argument of hashPassword is out of range");
			}

			String[] input = password.toArray(new String[0]);
			String[] res = new String[n];
			if (isFE) {
				int num = Coordinator.nodeMap.size();
				List<String> availableBEs = new ArrayList<>(Coordinator.nodeMap.keySet());
				if (num == 0) {
					// for test only
					// log.info("hashing on FE!");

					if (n < MULTI_THREAD_INPUT_THRESHOLD) {
						hashPasswordHelper(input, logRounds, 0, n - 1, res);
					} else {
						// for test only
						// log.info("multi-threaded hashing on BE!");

						int batchSize = n / MAX_WORKER_THREADS_NUM;
						CountDownLatch latch = new CountDownLatch(MAX_WORKER_THREADS_NUM);
						for (int i = 0; i < MAX_WORKER_THREADS_NUM; ++i) {
							int start = batchSize * i;
							int end;
							if (i == MAX_WORKER_THREADS_NUM - 1) {
								end = n - 1;
							} else {
								end = start + batchSize - 1;
							}
							new Thread(new HashTask(input, logRounds, start, end, res, latch)).start();
						}
						latch.await();
					}
					return new ArrayList<>(Arrays.asList(res));

				} else {
					// offload to BE
					ExecutorService exec = Executors.newFixedThreadPool(2);

					Future<List<String>> subResult1;
					Future<List<String>> subResult2 = null;
					subResult1 = exec.submit(new HashAsyncTask(password, logRounds, availableBEs, 0));
					if (num >= 2) {
						subResult2 = exec.submit(new HashAsyncTask(password, logRounds, availableBEs, 1));
					}

					// leave 1/3 in FE
					hashPasswordHelper(input, logRounds, n / (num + 1) * num, n - 1, res);
					List<String> result = new ArrayList<>(Arrays.asList(res));

					result.addAll(subResult1.get());
					if (num >= 2) {
						result.addAll(subResult2.get());
					}

					exec.shutdown();
					// log.info("result: " + result);
					return result;
				}

			} else {	// BE
				if (n < MULTI_THREAD_INPUT_THRESHOLD) {
					// for test only
					// log.info("single-threaded hashing on BE!");

					hashPasswordHelper(input, logRounds, 0, n - 1, res);
				} else {
					// for test only
					// log.info("multi-threaded hashing on BE!");

					int batchSize = n / MAX_WORKER_THREADS_NUM;
					CountDownLatch latch = new CountDownLatch(MAX_WORKER_THREADS_NUM);
					for (int i = 0; i < MAX_WORKER_THREADS_NUM; ++i) {
						int start = batchSize * i;
						int end;
						if (i == MAX_WORKER_THREADS_NUM - 1) {
							end = n - 1;
						} else {
							end = start + batchSize - 1;
						}
						// log.info("multi-threaded hashing on BE - part " + i);
						new Thread(new HashTask(input, logRounds, start, end, res, latch)).start();
					}
					latch.await();
				}
				return new ArrayList<>(Arrays.asList(res));

			}
		} catch (Exception e) {
			e.printStackTrace();
			return new ArrayList<>();
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

			String[] passwordArray = password.toArray(new String[0]);
			String[] hashArray = hash.toArray(new String[0]);
			Boolean[] res = new Boolean[n];
			if (isFE) {
				int num = Coordinator.nodeMap.size();
				List<String> availableBEs = new ArrayList<>(Coordinator.nodeMap.keySet());
				if (num == 0) {
					// for test only
					// log.info("checking on FE!");

					if (n < MULTI_THREAD_INPUT_THRESHOLD) {
						checkPasswordHelper(passwordArray, hashArray, 0, n - 1, res);
					} else {
						int batchSize = n / MAX_WORKER_THREADS_NUM;
						CountDownLatch latch = new CountDownLatch(MAX_WORKER_THREADS_NUM);
						for (int i = 0; i < MAX_WORKER_THREADS_NUM; ++i) {
							int start = batchSize * i;
							int end;
							if (i == MAX_WORKER_THREADS_NUM - 1) {
								end = n - 1;
							} else {
								end = start + batchSize - 1;
							}
							new Thread(new CheckTask(passwordArray, hashArray, start, end, res, latch)).start();
						}
						latch.await();
					}
					return new ArrayList<>(Arrays.asList(res));

				} else {
					// offload to BE
					ExecutorService exec = Executors.newFixedThreadPool(2);

					Future<List<Boolean>> subResult1;
					Future<List<Boolean>> subResult2 = null;
					subResult1 = exec.submit(new CheckAsyncTask(password, hash, availableBEs, 0));
					if (num >= 2) {
						subResult2 = exec.submit(new CheckAsyncTask(password, hash, availableBEs, 1));
					}

					// leave 1/3 in FE
					checkPasswordHelper(passwordArray, hashArray, n / (num + 1) * num, n - 1, res);
					List<Boolean> result = new ArrayList<>(Arrays.asList(res));

					result.addAll(subResult1.get());
					if (num >= 2) {
						result.addAll(subResult2.get());
					}

					exec.shutdown();
					return result;
				}

			} else {
				if (n < MULTI_THREAD_INPUT_THRESHOLD) {
					// for test only
					// log.info("single-threaded checking on BE!");

					checkPasswordHelper(passwordArray, hashArray, 0, n - 1, res);
				} else {
					// for test only
					// log.info("multi-threaded checking on BE!");

					int batchSize = n / MAX_WORKER_THREADS_NUM;
					CountDownLatch latch = new CountDownLatch(MAX_WORKER_THREADS_NUM);
					for (int i = 0; i < MAX_WORKER_THREADS_NUM; ++i) {
						int start = batchSize * i;
						int end;
						if (i == MAX_WORKER_THREADS_NUM - 1) {
							end = n - 1;
						} else {
							end = start + batchSize - 1;
						}
//						// log.info("multi-threaded checking on BE - part " + i);
						new Thread(new CheckTask(passwordArray, hashArray, start, end, res, latch)).start();
					}
					latch.await();
				}
				return new ArrayList<>(Arrays.asList(res));
			}

		} catch (Exception e) {
			e.printStackTrace();
			return new ArrayList<>();
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
			try {
				res[i] = BCrypt.checkpw(password[i], hash[i]);
			} catch (Exception e) {
				res[i] = false;
			}
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
//		// for test only
//		// log.info("Get connection request from BE node: " + hostBE + ":" + portBE);

		try {
			String address = hostBE + ":" + portBE;
			if (!Coordinator.containsNode(address)) {
//				// log.info("does not contain this node, register it at coordinator");
				Coordinator.addNode(address, new NodeInfo(hostBE, portBE));

//				// for test only
//				int idx = 0;
//				// log.info("====== Current NodeMap =====");
//				for (String s: Coordinator.nodeMap.keySet()) {
//					++idx;
//					// log.info(idx + ": " + s);
//				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
