import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.mindrot.jbcrypt.BCrypt;

import java.util.*;
import java.util.concurrent.*;

public class FEBCryptServiceHandler implements BcryptService.Iface{

    HashMap<String, Integer> activeBE = new HashMap<>();
    HashMap<String, Boolean> freeBE = new HashMap<>();
    HashMap<String,BcryptService.Client> clientPool = new HashMap<>();
    HashMap<String, TTransport> transportHashMap = new HashMap<>();
    static Logger log =  Logger.getLogger(BENode.class.getName());
    static Semaphore semaphore = new Semaphore(100);

    /**
     implementations for hash password
     **/

    @Override
    public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument {

        List<String> totalList = new ArrayList<>();

        if (password == null || password.size() == 0) {
            throw new IllegalArgumentException("Invalid password list input");
        }

        if(logRounds > 30.00 )
            throw new IllegalArgument("log rounds must be less than 30");
        if(logRounds < 4.00 )
            throw new IllegalArgument("log rounds must be greater than 4");

        int count_free_be = countFreeBE(freeBE);

        if((activeBE.isEmpty())) {
            totalList = WorkloadFE(password,logRounds);
        }
        else if ((password.size() == 1) && (count_free_be !=0)){
            checkConnections();
            return offloadToBE(password, logRounds);
        }
        else{
            checkConnections();
            ExecutorService executorService = Executors.newFixedThreadPool(2);
            List<FutureTask<List<String>>> taskList = new ArrayList<>();
            int incomingPasswordList = password.size();
            int workForFENode = 0;
            workForFENode = (int) (0.40 * incomingPasswordList);
            if((incomingPasswordList-workForFENode)%2!=0){
                workForFENode++;
            }
            int finalWorkForFENode = workForFENode;
            FutureTask<List<String>> futureTask_1 = new FutureTask<List<String>>(new Callable<List<String>>() {
                @Override
                public List<String> call() {
                    return WorkloadFE(password.subList(0, finalWorkForFENode),logRounds);
                }
            });
            taskList.add(futureTask_1);
            executorService.execute(futureTask_1);

            // Start thread for the second half of the numbers

            List<String> be_hash = new ArrayList<>();
            int finalWorkForFENode1 = workForFENode;
            FutureTask<List<String>> futureTask_2 = new FutureTask<List<String>>(new Callable<List<String>>() {
                @Override
                public List<String> call() {
                    checkConnections();
                    log.info("Passwords Executed by BE " + password.subList(finalWorkForFENode1,incomingPasswordList).size() );
                    List<String> subList = offloadToBE(password.subList(finalWorkForFENode1,incomingPasswordList), logRounds);
                    be_hash.addAll(subList);
                    return be_hash;
                }
            });
            taskList.add(futureTask_2);
            executorService.execute(futureTask_2);

            // Wait until all results are available and combine them at the same time
            for (int j = 0; j < 2; j++) {
                try {
                    totalList.addAll(taskList.get(j).get());
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }

            // close executorService
            executorService.shutdown();

            try {
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
        return totalList;
    }

    public List<String> WorkloadFE(List<String> password, short logRounds){
        List<String> fe_hash = new ArrayList<>();
        int passwordListSize = password.size();
        ExecutorService pool = Executors.newFixedThreadPool(2);
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
                fe_hash.addAll( taskList.get(j).get());
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
        return fe_hash;
    }

    public List<String> offloadToBE(List<String> password, short logRounds){
        final ConcurrentHashMap<Integer,List<String>> mapToMerge = new ConcurrentHashMap<>();
        int passwordListSize = password.size();
        List<String> hostnames = getFreeBE();
        int no_partitions =0;
        int passwords_per_BE=0;
        int count_free_be = countFreeBE(freeBE);

        if (count_free_be != 0 ){
            passwords_per_BE = passwordListSize/count_free_be;
            ExecutorService threadPool = Executors.newCachedThreadPool();
            List<List<String>> subSets_passwords = Lists.partition(password, passwords_per_BE);
            no_partitions = subSets_passwords.size();
            int index = 0;
            for (List<String> sublist: subSets_passwords) {
                String hostname = hostnames.get(index);
                TTransport t = transportHashMap.get(hostname);
                if(!validateObject(t)){
                    startConnections(hostname,activeBE.get(hostname));
                }
                BcryptService.Client client = clientPool.get(hostname);

                int finalIndex = index;
                threadPool.submit(new Runnable() {
                    public void run() {
                        Integer finalI = finalIndex;
                            synchronized (finalI) {
                                mapToMerge.putIfAbsent(finalI, work(hostname, client, sublist, logRounds));
                            }
                    }
                });
                index ++;
            }
            threadPool.shutdown();

            try {
                threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            List<String> hashedPasswords = new ArrayList<>();
            for(int i =0; i<no_partitions;++i){
                hashedPasswords.addAll(mapToMerge.get(i));
            }
            return hashedPasswords;
        }
        // System.out.println("Map to merge");
        return WorkloadFE(password, logRounds);
    }

    public List<String> hashPwd(List<String> password, short logRounds){
        List<String> hashed = new ArrayList<>();
        for (String pwd: password) {
            hashed.add(BCrypt.hashpw(pwd, BCrypt.gensalt(logRounds)));
        }
        return hashed;
    }

    public List<String> work(String hostname, BcryptService.Client client, List<String> al, short logRounds){
        freeBE.put(hostname,false);
        try {
            return client.hashPassword(al,logRounds);
        }catch (TException tException){
            tException.getMessage();
        }
        freeBE.put(hostname,true);
        return new ArrayList<>();
    }

    /**
     implementations for check password
     **/

    @Override
    public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, TException {
        List<Boolean> totalList = new ArrayList<>();
        if (password.isEmpty() || hash.isEmpty())
            throw new IllegalArgument("Password or hash list is empty");
        int passwordSize = password.size();
        int hashSize = hash.size();

        if (passwordSize != hashSize) {
            throw new IllegalArgument("Unequal password and hash list size ");
        }


        int count_free_be = countFreeBE(freeBE);
        if((activeBE.isEmpty())) {
            totalList = WorkloadCheckPasswordFE(password,hash);
        }
        else {
            checkConnections();
            if ((passwordSize == 1) && (count_free_be !=0)){
                return offloadCheckPasswordToBE(password, hash);
            }
            else{
                passwordSize= password.size();
                //  int incomingPasswordList = password.size();
                int workForFENode;
                workForFENode = (int) (0.40 * passwordSize);
                if((passwordSize-workForFENode)%2!=0){
                    workForFENode++;
                }
                List<Boolean> be_hash = new ArrayList<>();
                ExecutorService executorService = Executors.newFixedThreadPool(2);
                List<FutureTask<List<Boolean>>> taskList = new ArrayList<FutureTask<List<Boolean>>>();
                int finalWorkForFENode = workForFENode;
                FutureTask<List<Boolean>> futureTask_1 = new FutureTask<List<Boolean>>(new Callable<List<Boolean>>() {
                    @Override
                    public List<Boolean> call() throws TException {

                        return WorkloadCheckPasswordFE(password.subList(0, finalWorkForFENode), hash.subList(0,finalWorkForFENode));
                    }
                });
                taskList.add(futureTask_1);
                executorService.execute(futureTask_1);

                // Start thread for the second half of the numbers
                int finalWorkForFENode1 = workForFENode;
                int finalPasswordSize = password.size();
                FutureTask<List<Boolean>> futureTask_2 = new FutureTask<>(new Callable<List<Boolean>>() {
                    @Override
                    public List<Boolean> call() throws TException {
                        checkConnections();
                        log.info("Passwords Executed by BE " + password.subList(finalWorkForFENode1, finalPasswordSize));
                        List<Boolean> subList = offloadCheckPasswordToBE(password.subList(finalWorkForFENode1, finalPasswordSize), hash.subList(finalWorkForFENode1, finalPasswordSize));
                        be_hash.addAll(subList);
                        return be_hash;
                    }
                });
                taskList.add(futureTask_2);
                executorService.execute(futureTask_2);

                // Wait until all results are available and combine them at the same time
                for (int j = 0; j < 2; j++) {
                    FutureTask<List<Boolean>> futureTask = taskList.get(j);
                    try {
                        totalList.addAll(futureTask.get());
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                }

                // close executorService
                executorService.shutdown();

                try {
                    executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }

        return totalList;
    }

    public List<Boolean> offloadCheckPasswordToBE(List<String> password, List<String> hash) throws TException {
        final ConcurrentHashMap<Integer,List<Boolean>> mapToMerge = new ConcurrentHashMap<>();
        List<String> hostnames = getFreeBE();
        int no_partitions;
        int passwords_per_BE;
        int count_free_be = countFreeBE(freeBE);

        if (count_free_be != 0 ){
            passwords_per_BE = password.size()/count_free_be;
            ExecutorService threadPool = Executors.newCachedThreadPool();
            List<List<String>> subSets_passwords = Lists.partition(password, passwords_per_BE);
            List<List<String>> subSets_hash = Lists.partition(hash, passwords_per_BE);
            no_partitions = subSets_passwords.size();

            for(int i=0; i<no_partitions;i++) {
                String hostname = hostnames.get(i);
                TTransport t = transportHashMap.get(hostname);
                if(!validateObject(t)){
                    startConnections(hostname,activeBE.get(hostname));
                }
                BcryptService.Client client = clientPool.get(hostname);

                int finalI = i;
                threadPool.submit(new Runnable() {
                    public void run() {
                        Integer finalInt = finalI;
                        synchronized (finalInt) {
                            mapToMerge.putIfAbsent(finalInt,workOffloadCheckPassword(hostname,client,subSets_passwords.get(finalInt),subSets_hash.get(finalInt) ));

                        }
                    }
                });
            }
            threadPool.shutdown();

            try {
                threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            List<Boolean> hashedPasswords = new ArrayList<>();
            for(int i =0; i<no_partitions;++i){
                hashedPasswords.addAll(mapToMerge.get(i));
            }
            return hashedPasswords;
        }
        // System.out.println("Map to merge");
        return WorkloadCheckPasswordFE(password, hash);
    }

    public List<Boolean> workOffloadCheckPassword(String hostname, BcryptService.Client client, List<String> password, List<String> hash){
        freeBE.put(hostname,false);

        try {
            return   client.checkPassword(password,hash);
        }catch (TException tException){
            tException.getMessage();
        }
        freeBE.put(hostname,true);
        return new ArrayList<>();
    }



    public List<Boolean> WorkloadCheckPasswordFE(List<String> password, List<String> hash) throws TException {

        if(password.size()!= hash.size())
            throw  new IllegalArgument("unequal length of  lists inside WorkloadCheckPasswordFE");

        List<Boolean> result = new ArrayList<>();
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


    /**
     helper methods
     **/

    public void checkConnections(){
        for(Map.Entry entry: activeBE.entrySet()) {
            String hostBE = (String) entry.getKey();
            int port = (int) entry.getValue();
            if(!clientPool.containsKey(hostBE) || !transportHashMap.containsKey(hostBE)){
                startConnections(hostBE, port);
            }
        }
    }

    public void startConnections(String hostBE, int portBE){
        FEBCryptServiceHandler f = null;
        try {
            f = new FEBCryptServiceHandler();
            TTransport transport = (TTransport) f.create(hostBE, portBE, 10000);
            TProtocol protocol = new TBinaryProtocol(transport);
            clientPool.put(hostBE, new BcryptService.Client(protocol));
            transportHashMap.put(hostBE,transport);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Object create(String host, int port,int timeOut) throws Exception {
        TSocket sock = new TSocket(host, port);
        TTransport transport = new TFramedTransport(sock);
        try{
            transport.open();
        }
        catch (TException tException){
            tException.getMessage();
        }
        return transport;
    }

    public boolean validateObject(TTransport transport) {
        return transport.isOpen();
    }

    public void destroyObject(TTransport transport) throws Exception{
        if(transport.isOpen()){
            transport.close();
        }
    }

    @Override//hostFE is host address of be
    public Map<String, Integer> updateBE(String hostBE, int portBE){
        try {
            semaphore.acquire();
            activeBE.put(hostBE, portBE);
            freeBE.put(hostBE, true);
            log.info("here "+portBE);
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return activeBE;
    }


    public List<String> getFreeBE(){
        List<String> result = new ArrayList<>();
        for ( String key : freeBE.keySet()){
            if ( freeBE.get( key ).equals( true )) {
                result.add(key);
            }
        }
        return result;
    }

    public int countFreeBE(HashMap<String, Boolean> mp){
        int count=0;
        for(Map.Entry entry: mp.entrySet()){
            if((Boolean) entry.getValue()==true){
                count++;
            }
        }
        return count;
    }


}