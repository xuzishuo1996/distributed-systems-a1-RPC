import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.mindrot.jbcrypt.BCrypt;

public class BcryptServiceHandler implements BcryptService.Iface {
	private final boolean isFE;
	private final static int BE_WORKER_THREADS_NUM = 2;
	private final static int BE_MULTI_THREAD_THRESHOLD = 2;		// should be greater than BE_WORKER_THREADS_NUM
	private final Logger log;
	private final Random rand;

	public BcryptServiceHandler(boolean isFE) {
		this.isFE = isFE;
		rand = new Random();
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
			if (logRounds < 4 || logRounds > 30) {	// 4 - 30
				throw new IllegalArgument("the logRounds argument of hashPassword is out of range");
			}

			String[] input = password.toArray(new String[0]);
			String[] res = new String[n];
			if (isFE) {
				List<String> availableBEs = Coordinator.getAvailableNodes();
				int num = availableBEs.size();
				if (num == 0) {
					// for test only
					// // log.info("hashing on FE!");

					hashPasswordHelper(input, logRounds, 0, n - 1, res);
					return new ArrayList<>(Arrays.asList(res));
				} else {
					if (n == 1) {
						int idx = rand.nextInt(num + 1);
						if (idx == num) {
							hashPasswordHelper(input, logRounds, 0, n - 1, res);
							return new ArrayList<>(Arrays.asList(res));
						} else {
							ExecutorService exec = Executors.newFixedThreadPool(1);

							Future<List<String>> result = exec.submit(new HashAsyncTask(password, logRounds, availableBEs.subList(idx, idx + 1), 0));
							return result.get();
						}
					} else {
						// offload to BE
						ExecutorService exec = Executors.newFixedThreadPool(4);

						Future<List<String>> subResult1;
						Future<List<String>> subResult2 = null;
						subResult1 = exec.submit(new HashAsyncTask(password, logRounds, availableBEs, 0));
						if (num >= 2) {
							subResult2 = exec.submit(new HashAsyncTask(password, logRounds, availableBEs, 1));
						}

						// leave some work on FE
						int splitSize = n / (num + 1);
						int FEstart = splitSize * num;
						// log.info("FEstart" + FEstart);
						int batchSize = (n - FEstart) / 2;
						// log.info("batchSize" + batchSize);
						CountDownLatch latch = new CountDownLatch(BE_WORKER_THREADS_NUM);
						// log.info("FE latch initialized！");
						for (int i = 0; i < BE_WORKER_THREADS_NUM; ++i) {
							int start = FEstart + batchSize * i;
							int end;
							if (i == BE_WORKER_THREADS_NUM - 1) {
								end = n - 1;
								// log.info("end is: " + end);
							} else {
								end = start + batchSize - 1;
								// log.info("end is: " + end);
							}
							exec.execute(new HashTask(input, logRounds, start, end, res, latch));
						}
						latch.await();
						// log.info("FE calculation done!");

						List<String> result = subResult1.get();
						// log.info("BE1 subResult1 size: " + result.size());
						if (num >= 2) {
							List<String> tmpResult = subResult2.get();
							result.addAll(tmpResult);
							// log.info("BE2 subResult2 size: " + tmpResult.size());
						}
						// log.info("GET BE result");

						List<String> FEResult = new ArrayList<>(Arrays.asList(res)).subList(FEstart, n);
						// log.info("FEResult size: " + FEResult.size());
						result.addAll(FEResult);
						// log.info(result.size());

						exec.shutdown();
						return result;
					}
				}

			} else {	// BE
				// log.info("enter BE");
				if (n < BE_MULTI_THREAD_THRESHOLD) {
					// for test only
					// // log.info("single-threaded hashing on BE!");

					hashPasswordHelper(input, logRounds, 0, n - 1, res);
				} else {
					// for test only
					// // log.info("multi-threaded hashing on BE!");

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
						new Thread(new HashTask(input, logRounds, start, end, res, latch)).start();
					}
					latch.await();
					// log.info("BE calculation done!");
				}
			}
			return new ArrayList<>(Arrays.asList(res));

		} catch (IllegalArgument e) {
			throw new IllegalArgument(e);
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
				List<String> availableBEs = Coordinator.getAvailableNodes();

				int num = availableBEs.size();
				if (num == 0) {
					// for test only
					// // log.info("checking on FE!");

					checkPasswordHelper(passwordArray, hashArray, 0, n - 1, res);
					return new ArrayList<>(Arrays.asList(res));
				} else {
					if (n == 1) {
						int idx = rand.nextInt(num + 1);
						if (idx == num) {
							checkPasswordHelper(passwordArray, hashArray, 0, n - 1, res);
							return new ArrayList<>(Arrays.asList(res));
						} else {
							ExecutorService exec = Executors.newFixedThreadPool(1);

							Future<List<Boolean>> result = exec.submit(new CheckAsyncTask(password, hash, availableBEs.subList(idx, idx + 1), 0));
							return result.get();
						}
					} else {
						// offload to BE
						ExecutorService exec = Executors.newFixedThreadPool(2);

						Future<List<Boolean>> subResult1;
						Future<List<Boolean>> subResult2 = null;
						subResult1 = exec.submit(new CheckAsyncTask(password, hash, availableBEs, 0));
						if (num >= 2) {
							subResult2 = exec.submit(new CheckAsyncTask(password, hash, availableBEs, 1));
						}

						// leave some work on FE
						int splitSize = n / (num + 1);
						int FEstart = splitSize * num;
						int batchSize = (n - FEstart) / 2;
						CountDownLatch latch = new CountDownLatch(BE_WORKER_THREADS_NUM);
						// log.info("FE latch initialized！");
						for (int i = 0; i < BE_WORKER_THREADS_NUM; ++i) {
							int start = FEstart + batchSize * i;
							int end;
							if (i == BE_WORKER_THREADS_NUM - 1) {
								end = n - 1;
								// log.info("end is: " + end);
							} else {
								end = start + batchSize - 1;
								// log.info("end is: " + end);
							}
							exec.execute(new CheckTask(passwordArray, hashArray, start, end, res, latch));
						}
						latch.await();
						// log.info("FE calculation done!");


						List<Boolean> result = new ArrayList<>(subResult1.get());
						if (num >= 2) {
							result.addAll(subResult2.get());
						}

						List<Boolean> FEResult = new ArrayList<>(Arrays.asList(res)).subList(FEstart, n);
						// log.info("FEResult size: " + FEResult.size());
						result.addAll(FEResult);

						exec.shutdown();
						return result;
					}
				}

			} else {	//BE
				if (n < BE_MULTI_THREAD_THRESHOLD) {
					// for test only
					// // log.info("single-threaded checking on BE!");

					checkPasswordHelper(passwordArray, hashArray, 0, n - 1, res);
				} else {
					// for test only
					// // log.info("multi-threaded checking on BE!");

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
						new Thread(new CheckTask(passwordArray, hashArray, start, end, res, latch)).start();
					}
					latch.await();
				}
			}
			return new ArrayList<>(Arrays.asList(res));

		} catch (IllegalArgument e) {
			throw new IllegalArgument(e);
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
//		// // log.info("Get connection request from BE node: " + hostBE + ":" + portBE);

		try {
			String address = hostBE + ":" + portBE;
			if (!Coordinator.containsNode(address)) {
//				// // log.info("does not contain this node, register it at coordinator");
				Coordinator.addNode(address, new NodeInfo(hostBE, portBE));

//				// for test only
//				int idx = 0;
//				// // log.info("====== Current NodeMap =====");
//				for (String s: Coordinator.nodeMap.keySet()) {
//					++idx;
//					// // log.info(idx + ": " + s);
//				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
