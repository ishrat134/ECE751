import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import org.apache.thrift.TException;
import org.mindrot.jbcrypt.BCrypt;

public class BcryptServiceHandler implements BcryptService.Iface {

	@Override
	public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException
	{

		if(logRounds > 30.00 )
			throw new IllegalArgument("log rounds must be less than 16");
		if(logRounds < 4.00 )
			throw new IllegalArgument("log rounds must be greater than 4");

		List<String> ret = new ArrayList<>();
		int passwordListSize = password.size();
		try {

			ExecutorService pool = Executors.newCachedThreadPool();
			List<FutureTask<List<String>>> taskList = new ArrayList<>();

			FutureTask<List<String>> futureTask_1 = new FutureTask<>(new Callable<List<String>>() {
				@Override
				public List<String> call() {
					return hashPwd(password.subList(0, passwordListSize / 2), logRounds);
				}
			});

			taskList.add(futureTask_1);
			pool.execute(futureTask_1);

			FutureTask<List<String>> futureTask_2 = new FutureTask<>(new Callable<List<String>>() {
				@Override
				public List<String> call() {
					return hashPwd(password.subList(passwordListSize / 2, passwordListSize), logRounds);
				}
			});
			taskList.add(futureTask_2);
			pool.execute(futureTask_2);

			for (int j = 0; j < 2; j++) {
				try {
					ret.addAll( taskList.get(j).get());
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}

			// close executorService
			pool.shutdown();

			try {
				pool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}


		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}

		return ret;
	}

	public List<String> hashPwd(List<String> password, short logRounds){
		List<String> hashed = new ArrayList<>();
		for (String pwd: password) {
			hashed.add(BCrypt.hashpw(pwd, BCrypt.gensalt(logRounds)));
		}
		return hashed;
	}

	@Override
	public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException
	{
		if(password.isEmpty() || hash.isEmpty())
			throw new IllegalArgument("Password or hash list is empty");
		if (password.size() != hash.size()){
			throw new IllegalArgument("Unequal password and hash list size ");
		}

		List<Boolean> result = new ArrayList<>();
		try {

			int size = password.size();

			ExecutorService pool = Executors.newFixedThreadPool(2);
			List<FutureTask<List<Boolean>>> taskList = new ArrayList<>();

			FutureTask<List<Boolean>> futureTask_1 = new FutureTask<>(new Callable<List<Boolean>>() {
				@Override
				public List<Boolean> call() throws TException {
					return checkPwd(password.subList(0, size / 2), hash.subList(0, size / 2));
				}
			});

			taskList.add(futureTask_1);
			pool.execute(futureTask_1);

			FutureTask<List<Boolean>> futureTask_2 = new FutureTask<List<Boolean>>(new Callable<List<Boolean>>() {
				@Override
				public List<Boolean> call() throws  TException {
					return checkPwd(password.subList(size/2,size), hash.subList(size/2,size));
				}
			});
			taskList.add(futureTask_2);
			pool.execute(futureTask_2);

			for (int j = 0; j < 2; j++) {
				FutureTask<List<Boolean>> futureTask = taskList.get(j);
				try {
					result.addAll(futureTask.get());
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}

			// close executorService
			pool.shutdown();

			try {
				pool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		catch (Exception e) {
			throw new IllegalArgument("Invalid hash");
		}

		return result;
	}

	public List<Boolean> checkPwd(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException
	{
		if(password.isEmpty() || hash.isEmpty())
			throw new IllegalArgument("Password or hash list is empty");
		if (password.size() != hash.size()){
			throw new IllegalArgument("Unequal password and hash list size ");
		}
		try {
			List<Boolean> ret = new ArrayList<>();
			for(int i = 0; i < password.size(); ++i) {
				ret.add(BCrypt.checkpw(password.get(i), hash.get(i)));
			}
			return ret;
		} catch (Exception e) {
			throw new IllegalArgument("Invalid hash");
		}
	}
	@Override
	public Map<String, Integer> updateBE(String hostBE, int portBE) throws TException {
		return  null;
	}
}
