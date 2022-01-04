import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

//import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;

public class Client {
	public static void main(String[] args) throws InterruptedException {
		if (args.length != 3) {
			System.err.println("Usage: java Client FE_host FE_port password");
			System.exit(-1);
		}

		long before = System.currentTimeMillis();
		runBasicCheck(args);
		long after = System.currentTimeMillis();
		System.out.printf("Took %s seconds\n", (after - before) / 1000.0);
	}

	public static void runBasicCheck(String[] args) {
		try {
			TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
			TTransport transport = new TFramedTransport(sock);
			TProtocol protocol = new TBinaryProtocol(transport);
			BcryptService.Client client = new BcryptService.Client(protocol);
			transport.open();

			List<String> password = new ArrayList<>();
			for(int i = 0;i<149; ++i){
				password.add("pass"+i);
			}
			long start = System.currentTimeMillis();

			List<String> hash = client.hashPassword(password, (short) 10);
			System.out.println(hash.size());

			//System.out.println("here"+(System.currentTimeMillis()-start));
			System.out.println("Positive check: " + client.checkPassword(password, hash));
			//hash.set(0, "$2a$14$reBHJvwbb0UWqJHLyPTVF.6Ld5sFRirZx/bXMeMmeurJledKYdZmG");
			//System.out.println("Negative check: " + client.checkPassword(password, hash));
			try {
				//hash.set(0, "too short");
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

	static int numThreads = 4;
	static int requestsPerThread = 1;

	static int passwordLength = 1024;
	static int passwordsPerRequest = 62;
	static short logRounds = 10;

	public static void runMulithreadedClient(String[] args) throws InterruptedException {
		boolean[][] results = new boolean[numThreads][requestsPerThread];
		Thread[] threads = new Thread[numThreads];

		for (int i = 0; i < numThreads; i++) {
			threads[i] = new Thread(new BigHashPassword(args[0], args[1], i, results));
			threads[i].start();
		}

		// Wait for all threads to complete
		for (Thread t : threads) t.join();

		for (int i = 0; i < numThreads; i++) {
			System.out.println(Arrays.toString(results[i]));
			for (int j = 0; j < requestsPerThread; j++) {
				if (!results[i][j]) {
					System.out.printf("Thread %s failed on request %s\n", i, j);
				}
			}
		}
	}


	private static final String ALPHA_NUMERIC_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

	static String randomStringOfLength(int length) {
		StringBuilder builder = new StringBuilder();
		while (length-- != 0) {
			int character = (int) (Math.random() * ALPHA_NUMERIC_STRING.length());
			builder.append(ALPHA_NUMERIC_STRING.charAt(character));
		}
		return builder.toString();
	}

	private static class BigHashPassword implements Runnable {
		private final String host, port;

		private final List<String> passwords;

		private final int i;
		private final boolean[][] results;

		private BigHashPassword(String host, String port, int i, boolean[][] results) {
			this.i = i;
			this.results = results;

			this.host = host;
			this.port = port;
			System.out.println("Inside BigHash Loop");
			passwords = new ArrayList<>(passwordsPerRequest);
			for (int j = 0; j < passwordsPerRequest; j++) {
				passwords.add(randomStringOfLength(passwordLength));
			}
		}


		@Override
		public void run() {
			try {
				TSocket sock = new TSocket(host, Integer.parseInt(port));
				TTransport transport = new TFramedTransport(sock);
				TProtocol protocol = new TBinaryProtocol(transport);
				BcryptService.Client client = new BcryptService.Client(protocol);
				transport.open();

				for (int j = 0; j < requestsPerThread; j++) {
					System.out.println(Thread.currentThread().getName() + " sending passwords");
					List<String> hashes = client.hashPassword(passwords, logRounds);
					//List<Boolean> pp = client.checkPassword(passwords,hashes);
					//System.out.println(pp);
					results[i][j] = true;
					System.out.println(Thread.currentThread().getName() + " success!");
				}
				transport.close();
			} catch (TException x) {
				x.printStackTrace();
				System.out.println(Thread.currentThread().getName() + " failed!");
			}
		}
	}
}
