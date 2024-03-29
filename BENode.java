import java.net.InetAddress;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.*;

public class BENode {
	static Logger log;

	public static void main(String [] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: java BENode FE_host FE_port BE_port");
			System.exit(-1);
		}

		// initialize log4j
		BasicConfigurator.configure();
		log = Logger.getLogger(BENode.class.getName());

		String hostFE = args[0];
		String hostBE = getHostName();
		int portFE = Integer.parseInt(args[1]);
		int portBE = Integer.parseInt(args[2]);
//		log.info("Launching BE node on port " + portBE + " at host " + getHostName());

		// connect to the FE node
		TSocket sock = new TSocket(hostFE, portFE);
		TTransport transport = new TFramedTransport(sock);
		TProtocol protocol = new TBinaryProtocol(transport);
		BcryptService.Client client = new BcryptService.Client(protocol);
		while (true) {
			boolean succeed = true;
			try {
				transport.open();
			} catch (TTransportException ignored) {
				succeed = false;
			}
			if (succeed) { break; }
		}
		client.connectFE(hostBE, portBE);


		/* launch Thrift TThreadPoolServer: uses one thread to accept connections
		 * and then handles each connection using a dedicated thread
		 */
		BcryptService.Processor<BcryptService.Iface> processor = new BcryptService.Processor<BcryptService.Iface>(new BcryptServiceHandler(false));
		TServerSocket socket = new TServerSocket(portBE);
		TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
		sargs.protocolFactory(new TBinaryProtocol.Factory());
		sargs.transportFactory(new TFramedTransport.Factory());
		sargs.processorFactory(new TProcessorFactory(processor));
		sargs.maxWorkerThreads(64);	//TODO: how to determine the maxWorker size?
		TThreadPoolServer server = new TThreadPoolServer(sargs);
		server.serve();

//		BcryptService.Processor<BcryptService.Iface> processor = new BcryptService.Processor<BcryptService.Iface>(new BcryptServiceHandler(false));
//		TNonblockingServerSocket socket = new TNonblockingServerSocket(portBE);
//		TThreadedSelectorServer.Args sargs = new TThreadedSelectorServer.Args(socket);
//		sargs.protocolFactory(new TBinaryProtocol.Factory());
//		sargs.transportFactory(new TFramedTransport.Factory());
//		sargs.processorFactory(new TProcessorFactory(processor));
////		sargs.maxWorkerThreads(64);	//TODO: how to determine the maxWorker size?
//		TThreadedSelectorServer server = new TThreadedSelectorServer(sargs);
//		server.serve();

		// launch Thrift THsHaServer: can process multiple requests in parallel
//		BcryptService.Processor<BcryptService.Iface> processor = new BcryptService.Processor<BcryptService.Iface>(new BcryptServiceHandler(false));
//		TNonblockingServerSocket socket = new TNonblockingServerSocket(portBE);
//		THsHaServer.Args sargs = new THsHaServer.Args(socket);
//		sargs.protocolFactory(new TBinaryProtocol.Factory());
//		sargs.transportFactory(new TFramedTransport.Factory());
//		sargs.processorFactory(new TProcessorFactory(processor));
//		sargs.maxWorkerThreads(64);	//TODO: how to determine the maxWorker size?
//		THsHaServer server = new THsHaServer(sargs);
//		server.serve();
	}

	static String getHostName()
	{
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			return "localhost";
		}
	}
}
