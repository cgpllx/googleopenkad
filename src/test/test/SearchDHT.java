package test.test;

import com.google.inject.Guice;
import com.google.inject.Injector;

import il.technion.ewolf.kbr.KeybasedRouting;
import il.technion.ewolf.kbr.openkad.KadNetModule;
import il.technion.ewolf.kbr.openkad.net.KadServer;

public class SearchDHT {
	public void searchStart(){
//		KadServer kadServer=KadServer
		Injector injector = Guice.createInjector(new KadNetModule()//
		.setProperty("openkad.keyfactory.keysize", "2")//
		.setProperty("openkad.bucket.kbuckets.maxsize", "3")//
		.setProperty("openkad.net.udp.port", "" + (11 + 1)));

		KadServer kadServer = injector.getInstance(KadServer.class);
		
	}
}
