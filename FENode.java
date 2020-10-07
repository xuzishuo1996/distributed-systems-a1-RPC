import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TFramedTransport;

public class FENode {
	static Logger log;

	public static void main(String [] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: java FENode FE_port");
			System.exit(-1);
		}

		// initialize log4j
		BasicConfigurator.configure();
		log = Logger.getLogger(FENode.class.getName());

		int portFE = Integer.parseInt(args[0]);
//		log.info("Launching FE node on port " + portFE);

		// launch Thrift THsHaServer: can process multiple requests in parallel
//		BcryptService.Processor<BcryptService.Iface> processor = new BcryptService.Processor<BcryptService.Iface>(new BcryptServiceHandler(true));
//		TNonblockingServerSocket socket = new TNonblockingServerSocket(portFE);
//		THsHaServer.Args sargs = new THsHaServer.Args(socket);
//		sargs.protocolFactory(new TBinaryProtocol.Factory());
//		sargs.transportFactory(new TFramedTransport.Factory());
//		sargs.processorFactory(new TProcessorFactory(processor));
//		sargs.maxWorkerThreads(64);	//TODO: how to determine the maxWorker size?
//		THsHaServer server = new THsHaServer(sargs);
//		server.serve();

		// TThreadPoolServer
		BcryptService.Processor<BcryptService.Iface> processor = new BcryptService.Processor<BcryptService.Iface>(new BcryptServiceHandler(true));
		TServerSocket socket = new TServerSocket(portFE);
		TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
		sargs.protocolFactory(new TBinaryProtocol.Factory());
		sargs.transportFactory(new TFramedTransport.Factory());
		sargs.processorFactory(new TProcessorFactory(processor));
		sargs.maxWorkerThreads(64);
		TThreadPoolServer server = new TThreadPoolServer(sargs);
		server.serve();
    }
}
