import java.net.InetAddress;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.*;


public class BENode {
	static Logger log;

	public static void main(String [] args) throws TTransportException {
		if (args.length != 3) {
			System.err.println("Usage: java BENode FE_host FE_port BE_port");
			System.exit(-1);
		}
		// initialize log4j
		BasicConfigurator.configure();
		log = Logger.getLogger(BENode.class.getName());
		String hostFE = args[0];
		int portFE = Integer.parseInt(args[1]);
		int portBE = Integer.parseInt(args[2]);
		log.info("Launching BE node on port " + portBE + " at host " + getHostName());


			// launch Thrift server
			BcryptService.Processor processor = new BcryptService.Processor<BcryptService.Iface>(new BcryptServiceHandler());
			TNonblockingServerSocket socket = new TNonblockingServerSocket(portBE);
			THsHaServer.Args sargs = new THsHaServer.Args(socket);

			sargs.protocolFactory(new TBinaryProtocol.Factory());
			sargs.transportFactory(new TFramedTransport.Factory());
			sargs.processorFactory(new TProcessorFactory(processor));
			sargs.maxWorkerThreads(48);
			TServer server = new THsHaServer(sargs);
			sendRequestToClient(getHostAddress(),portBE, hostFE, portFE);

			server.serve();

	}

	static void sendRequestToClient(String beHost, int bePort, String feHost, int fePort) {
		boolean heartBeatSuccess = false;

		while (!heartBeatSuccess) {
			try {
				TSocket sock = new TSocket(feHost, fePort);
				TTransport transport = new TFramedTransport(sock);
				TProtocol protocol = new TBinaryProtocol(transport);
				BcryptService.Client client = new BcryptService.Client(protocol);
				transport.open();
				client.updateBE(beHost, bePort);
				transport.close();
				heartBeatSuccess = true;
				log.info("connected to FE");
			} catch (Exception e) {
				// Do nothing
			}
		}
	}

	static String getHostName()
	{
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			return "localhost";
		}
	}


	static String getHostAddress()
	{
		try {
			return InetAddress.getLocalHost().getHostAddress();
		} catch (Exception e) {
			return "127.0.0.1";
		}
	}
}

