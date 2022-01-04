import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.net.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.atomic.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.api.*;

import com.google.common.util.concurrent.Striped;

import org.apache.log4j.*;

public class KeyValueHandler implements KeyValueService.Iface, CuratorWatcher{
    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;

    private static Logger log;
    private volatile Boolean isPrimary = false;
    private ReentrantLock globalLock = new ReentrantLock();
    private Striped<Lock> stripedLock = Striped.lock(64);
    private volatile ConcurrentLinkedQueue<KeyValueService.Client> backupClients = null;
    private int clientNumber = 32;

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) throws Exception {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;

        log = Logger.getLogger(KeyValueHandler.class.getName());

        curClient.sync();
        List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);

        if (children.size() == 1) {

            this.isPrimary = true;
        } else {
            Collections.sort(children);
            byte[] backupData = curClient.getData().forPath(zkNode + "/" + children.get(children.size() - 1));
            String strBackupData = new String(backupData);
            String[] backup = strBackupData.split(":");
            String backupHost = backup[0];
            int backupPort = Integer.parseInt(backup[1]);

            if (backupHost.equals(host) && backupPort == port) {
                // System.out.println("Is Primary: " + false);
                this.isPrimary = false;
            } else {
                // System.out.println("Is Primary: " + true);
                this.isPrimary = true;
            }
        }

        myMap = new ConcurrentHashMap<String, String>();
    }

    public void setPrimary(boolean isPrimary) throws TException {
        this.isPrimary = isPrimary;
    }

    public String get(String key) throws TException {
        if (isPrimary == false) {
            // System.out.println("Backup is not allowed to get.");
            throw new TException("Backup is not allowed to get.");
        }

        try {
            String ret = myMap.get(key);
            if (ret == null)
                return "";
            else
                return ret;
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    public void put(String key, String value) throws TException {
        if (isPrimary == false) {
            // System.out.println("Backup is not allowed to put.");
            throw new TException("Backup is not allowed to put.");
        }

        Lock lock = stripedLock.get(key);
        lock.lock();

        while (globalLock.isLocked());

        try {
            myMap.put(key, value);

            // has backup clients
            if (this.backupClients != null) {
                // writeToBackup
                KeyValueService.Client currentBackupClient = null;

                while(currentBackupClient == null) {
                    currentBackupClient = backupClients.poll();
                }
    
                currentBackupClient.putBackup(key, value);

                this.backupClients.offer(currentBackupClient);
            }
        } catch (Exception e) {
            e.printStackTrace();
            this.backupClients = null;
        } finally {
            lock.unlock();
        }
    }

    public void putBackup(String key, String value) throws TException {

        Lock lock = stripedLock.get(key);
        lock.lock();

        try {
            myMap.put(key, value);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }
    
    public void copyData(Map<String, String> data) throws TException {
        this.myMap = new ConcurrentHashMap<String, String>(data); 

    }
    
	synchronized public void process(WatchedEvent event) throws TException {
        try {
            curClient.sync();
            List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);

            if (children.size() == 1) {
                this.isPrimary = true;
                return;
            }
            
            Collections.sort(children);
            byte[] backupData = curClient.getData().forPath(zkNode + "/" + children.get(children.size() - 1));
            String strBackupData = new String(backupData);
            String[] backup = strBackupData.split(":");
            String backupHost = backup[0];
            int backupPort = Integer.parseInt(backup[1]);

            if (backupHost.equals(host) && backupPort == port) {
                this.isPrimary = false;
            } else {
                this.isPrimary = true;
            }
            
            if (this.isPrimary && this.backupClients == null) {

                KeyValueService.Client firstBackupClient = null;

                while(firstBackupClient == null) {
                    try {
                        TSocket sock = new TSocket(backupHost, backupPort);
                        TTransport transport = new TFramedTransport(sock);
                        transport.open();
                        TProtocol protocol = new TBinaryProtocol(transport);
                        firstBackupClient = new KeyValueService.Client(protocol);
                    } catch (Exception e) {
                        log.error("cant create connection");
                    }
                }
                
                // Copy data to backup
                globalLock.lock();
                
                firstBackupClient.copyData(this.myMap);

                this.backupClients = new ConcurrentLinkedQueue<KeyValueService.Client>();
    
                for(int i = 0; i < clientNumber; i++) {
                    TSocket sock = new TSocket(backupHost, backupPort);
                    TTransport transport = new TFramedTransport(sock);
                    transport.open();
                    TProtocol protocol = new TBinaryProtocol(transport);
            
                    this.backupClients.add(new KeyValueService.Client(protocol));
                }
                globalLock.unlock();
            } else {
                this.backupClients = null;
            }
        } catch (Exception e) {
            log.error("Unable to determine primary or children");
            this.backupClients = null;
        }
    }
}