/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.accumulo.examples.simple.helloworld;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;

import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.nio.ByteBuffer;
import org.apache.hadoop.fs.Path;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import java.util.Collections;
import java.util.Iterator;

import java.io.File;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.tools.DistCp;

public class InsertDnsLogWithBatchWriter {

    private static String getTopLevelDomain(String host, String rawLogLine) {
	
	// http://mxr.mozilla.org/mozilla-central/source/netwerk/dns/effective_tld_names.dat?raw=1
	boolean isSingle = false;
	String[] parts = host.split("\\.");
	int partsLen = parts.length;

	if (partsLen == 0) {
	    return host;
	}

	// Not dealing with unicode TLDs!
	if (parts[partsLen-1].equals("com")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("net")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("org")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("aero")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("cat")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("cc")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("cf")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("cg")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("ch")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("cn")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("coop")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("cv")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("cz")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("de")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("dj")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("dk")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("edu")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("eu")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("fm")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("ga")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("gf")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("gl")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("gq")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("gq")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("gs")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("gw")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("hm")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("hr")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("info")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("jobs")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("ke")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("kh")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("kw")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("li")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("lu")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("md")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("mh")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("mil")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("mobi")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("mq")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("mp")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("ms")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("name")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("ne")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("net")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("nl")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("nu")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("pm")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("post")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("si")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("sr")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("su")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("tc")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("td")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("tel")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("tf")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("tg")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("tk")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("travel")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("tv")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("va")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("vg")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("vu")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("wf")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("yt")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("xxx")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("camera")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("clothing")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("lighting")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("singles")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("ventures")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("voyage")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("guru")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("holdings")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("equipment")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("ventures")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("bike")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("estate")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("tattoo")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("land")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("plumbing")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("contractors")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("sexy")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("uno")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("gallery")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("technology")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("reviews")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("guide")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("graphics")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("construction")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("onl")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("diamonds")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("kiwi")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("enterprises")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("today")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("futbol")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("photography")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("tips")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("directory")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("kitchen")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("kim")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("monash")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("wed")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("pink")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("ruhr")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("buzz")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("careers")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("shoes")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("career")) {
	    isSingle = true;
	}
	else if (parts[partsLen-1].equals("otsuka")) {
	    isSingle = true;
	}

	String result = null;
	if (isSingle && partsLen > 1) {
	    result = String.format("%s.%s",parts[partsLen-2], parts[partsLen-1]);
	} else if (!isSingle) {
	    if (parts.length > 2) {
		result = String.format("%s.%s.%s",parts[partsLen-3], parts[partsLen-2], parts[partsLen-1]);
	    } else if (parts.length > 1) {
		// Is this an ERROR? Or we really don't care???
		result = String.format("%s.%s", parts[partsLen-2], parts[partsLen-1]);
	    } else {
		// Is this an ERROR? Or we really don't care???
		result = parts[partsLen-1];
	    }
	}
	return result;

    }

    public static String stripChars(String input, String strip) {
	StringBuilder result = new StringBuilder();
	for (char c : input.toCharArray()) {
	    if (strip.indexOf(c) == -1) {
		result.append(c);
	    }
	}
	return result.toString();
    }

    public static byte[] toBytes(final int intVal, final int... intArray) {
	if (intArray == null || (intArray.length == 0)) {
	    return ByteBuffer.allocate(4).putInt(intVal).array();
	} else {
	    final ByteBuffer bb = ByteBuffer.allocate(4 + (intArray.length * 4)).putInt(intVal);
	    for (final int val : intArray) {
		bb.putInt(val);
	    }
	    return bb.array();
	}
    }

    public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, MutationsRejectedException, TableExistsException, TableNotFoundException 
    {
	if (args.length < 6) {
	    System.out
		.println("Usage: accumulo examples-simplejar accumulo.examples.helloworld.InsertDnsLogWithBatchWriter <instance name> <zoo keepers> <username> <password> <tableName> <logFile> <rowkey> <start> <maxrecords> <rrtype_details> <graph_details>");
	    System.exit(1);
	}
    
	final String instanceName = args[0];
	final String zooKeepers = args[1];
	final String user = args[2];
	final byte[] pass = args[3].getBytes();
	final String tableName = args[4];
	String records = "1000000";
	String rowkey = null;
	String logFile = "1382375700";
	int maxRecords = 0;
	boolean rrtype_details = false;
	boolean graph_details = false;

	if (args.length > 5) {
	    logFile = args[5];
	}

	if (args.length > 6) {
	    rowkey = args[6];
	}

	int start = 0;
	if (args.length > 7) {
	    try {
		start = Integer.parseInt(args[7]);
	    } catch(Exception e) {
		System.out.println("WARN: Invalid <START>, assuming 0");
	    }
	}

	String maxrecords = "1000000";
	if (args.length > 8)
	    maxrecords = args[8];

	if (args.length > 9)
	    rrtype_details = args[9].equals("true");
    
	if (args.length > 10)
	    graph_details = args[10].equals("true");
    
	Integer val = null;
	try {
	    val = Integer.parseInt(maxrecords);
	} catch (NumberFormatException nfe) { }
	if (val != null) {
	    maxRecords = val;
	}

	System.out.printf("logFile    => %s\n", logFile);
	System.out.printf("rowKey     => %s\n", rowkey);
	System.out.printf("start      => %d\n", start);
	System.out.printf("maxRecords => %d\n", maxRecords);
	System.out.printf("rrtypes    => %b\n", rrtype_details);
	System.out.printf("graphs     => %b\n", graph_details);

	MiniAccumuloCluster accumulo = null;

	try {

	    ZooKeeperInstance instance = null; // new ZooKeeperInstance(instanceName, zooKeepers);
	    Connector connector = null; // instance.getConnector(user, pass);
	    boolean useMiniCluster = false;

	    if (useMiniCluster && accumulo == null) {
		File tempDirectory = new File("/home/accumulo/temp/");
		System.out.printf("Creating MiniCluster\n");
		accumulo = new MiniAccumuloCluster(tempDirectory, new String(pass));
		System.out.printf("Creating Zookeeper Instance\n");
		instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
		System.out.printf("Getting Connector\n");
		connector = instance.getConnector(user, pass); // new PasswordToken(pass));
		System.out.printf("Starting MiniCluster\n");
		accumulo.start();
	    } else {
		instance = new ZooKeeperInstance(instanceName, zooKeepers);
		connector = instance.getConnector(user, pass);
	    }

	    final String graphTable = String.format("graph_%s", tableName);

	    // Create table
	    if (!connector.tableOperations().exists(tableName)) {

		// Create the table
		System.out.printf("Creating table %s\n", tableName);
		connector.tableOperations().create(tableName);
		System.out.printf("Added table %s\n", tableName);

		if (graph_details) {
		    // Create the graph table (need to make this optional)
		    connector.tableOperations().create(graphTable);
		    System.out.printf("Added table %s\n", graphTable);
		}
		
		// Accumulate overall counts
		final IteratorSetting iterCounts = new IteratorSetting (1, "counts", SummingCombiner.class);
		SummingCombiner.setColumns(iterCounts, Collections.singletonList(new IteratorSetting.Column("counts")));
		SummingCombiner.setEncodingType(iterCounts, SummingCombiner.Type.STRING);
		connector.tableOperations().attachIterator(tableName, iterCounts);
		System.out.printf("Added SummingCombiner for 'counts'\n");

		// Accumulate overall per zone counts
		final IteratorSetting iterZones = new IteratorSetting (2, "zones", SummingCombiner.class);
		SummingCombiner.setColumns(iterZones, Collections.singletonList(new IteratorSetting.Column("zones")));
		SummingCombiner.setEncodingType(iterZones, SummingCombiner.Type.STRING);
		connector.tableOperations().attachIterator(tableName, iterZones);
		System.out.printf("Added SummingCombiner for 'zones'\n");

		// Accumulate overall per client counts
		final IteratorSetting iterClients = new IteratorSetting (3, "clients", SummingCombiner.class);
		SummingCombiner.setColumns(iterClients, Collections.singletonList(new IteratorSetting.Column("clients")));
		SummingCombiner.setEncodingType(iterClients, SummingCombiner.Type.STRING);
		connector.tableOperations().attachIterator(tableName, iterClients);
		System.out.printf("Added SummingCombiner for 'clients'\n");

		// Accumulate overall per port counts
		final IteratorSetting iterPorts = new IteratorSetting (4, "ports", SummingCombiner.class);
		SummingCombiner.setColumns(iterPorts, Collections.singletonList(new IteratorSetting.Column("ports")));
		SummingCombiner.setEncodingType(iterPorts, SummingCombiner.Type.STRING);
		connector.tableOperations().attachIterator(tableName, iterPorts);
		System.out.printf("Added SummingCombiner for 'ports'\n");

	    } else {
		System.out.printf("WARNING: table already exists\n");
	    }
	
	    BatchWriter bwGraph = null;

	    /*
	    System.out.printf("Creating MultiTableBatchWriter\n");
	    final MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(200000l, 300, 4);
	    BatchWriter bw = mtbw.getBatchWriter(tableName);
	    */
	
	    System.out.printf("Creating BatchWriter\n");
	    BatchWriter bw = connector.createBatchWriter(tableName, 2000000l, 0, 4);

	    if (graph_details) {
		// bwGraph = mtbw.getBatchWriter(graphTable);
	    }
    
	    BufferedReader in = null;
	    final Value one = new Value("1".getBytes());
	    final Text r_query = new Text("query");
	    final Text r_a = new Text("A");
	    final Text r_aaaa = new Text("AAAA");
	    final Text r_ptr = new Text("PTR");
	    final Text r_txt = new Text("TXT");
	    final Text r_srv = new Text("SRV");
	    final Text r_soa = new Text("SOA");
	    final Text r_spf = new Text("SPF");
	    final Text r_mx = new Text("MX");
	    final Text r_any = new Text("ANY");
	    final Text r_ds = new Text("DS");
	    final Text r_a6 = new Text("A6");
	    final Text r_rrsig = new Text("RRSIG");
	    final Text r_dnskey = new Text("DNSKEY");
	    final Text r_ns = new Text("NS");
	    final Text r_cname = new Text("CNAME");
	    final Text r_naptr = new Text("NAPTR");
	    final Text r_tkey = new Text("TKEY");
	    final Text r_sshfp = new Text("SSHFP");
	    final Text r_hinfo = new Text("HINFO");
	    final Text r_fqdns = new Text("fqdns");
	    final Text r_clientip = new Text("clientip");
	    final Text r_error = new Text("error");
	
	    HashMap uniqueTLD = new HashMap();
	    HashMap uniqueClient = new HashMap();
	    HashMap uniqueLines = new HashMap();
	    Integer errors = 0;
	    int counter = 0;
	    int skips = 0;
	    int ignores = 0;
	    int cracked = 0;

	    // Count & Remember unique rrtypes and top level statistics that we encounter.
	    final Text colf_counts = new Text("counts");

	    // Count & Remember unique zones that we encounter.
	    final Text colf_zones = new Text("zones");

	    // Count & Remember details per zones that we encounter.
	    final Text colf_details = new Text("details");
	
	    // Count & Remember unique client ips
	    final Text colf_clients = new Text("clients");

	    // Count & Remember unique client ips
	    final Text colf_ports = new Text("ports");

	    // Count & Remember tld id
	    final Text colf_tldid = new Text("tldid");

	    // Timestamp is always our rowkey. We store the node that we found things in 
	    final Text row_counts = new Text(rowkey);

	    int tld_id = 0;

	    String node = null;

	    try {
		in = new BufferedReader(new FileReader(logFile));
		while (in.ready()) {
		    final String logLine = in.readLine();
		    counter++;
		    if (counter-start > maxRecords) {
			ignores++;
		    }
		    else if (counter < start) {
			skips++;
		    } else {
			String parts[] = logLine.split(" ");
			if (parts.length == 14) {
			    // 00 Oct
			    // 01 21
			    // 02 17:10:00
			    // 03 dns4-01-fra
			    // 04 named[17652]: 
			    // 05 client
			    // 06 194.109.133.199#12606
			    // 07 (rammeltv.weebly.com):
			    // 08 query:
			    // 09 rammeltv.weebly.com
			    // 10 IN
			    // 11 A 
			    // 12 - 
			    // 13 (204.13.250.29)
			    node = parts[3];
			    final String client_port = parts[6];
			    String client = null;
			    String port = null;
			    final String fqdn = stripChars(parts[7],"():'");
			    final String query = parts[9] + " " + parts[10] + " " + parts[11];
			    final String rrtype = parts[11];
			    final String answer = parts[13];
			    // System.out.printf("%10d Node=%s Client=%s FQDN=%s RRType=%s Answer=%s\n", counter, node, client, fqdn, rrtype, answer);

			    // Cleanup the client/port
			    int portStart = client_port.indexOf("#");
			    client = client_port.substring(0, portStart);
			    port = client_port.substring(portStart+1);
			
			    final Text client_text = new Text(stripChars(client, "()"));

			    // Emit the graph info if we're doing that type of processing
			    if (graph_details) {
				final Text answer_text = new Text(stripChars(answer, "()"));
				final Text rrtype_text = new Text(rrtype);
				final Text fqdn_text = new Text(fqdn);
				Mutation mGraphFQDN = new Mutation(fqdn_text);
				Mutation mGraphAnswer = new Mutation(answer_text);
				mGraphFQDN.put(rrtype_text, answer_text, one);
				mGraphAnswer.put(rrtype_text, fqdn_text, one);
				bwGraph.addMutation(mGraphFQDN);
				bwGraph.addMutation(mGraphAnswer);
			    }
			    
			    Mutation m = new Mutation(row_counts);

			    // Track and remember counts for each unique record type that we answer for
			    // All RRType Counter
			    m.put(colf_counts, r_query, one);

			    // Remember the node that we processed this record for
			    m.put(colf_counts, new Text(node), one);

			    // Remember domain (in a TLD)
			    String tld = getTopLevelDomain(fqdn, logLine);

			    if (tld != null) {
				// Remeber that we've had a query for this TLD
				m.put(colf_zones, new Text(tld), one);

				Integer cur_tld_id = (Integer)uniqueTLD.get(tld);
				if (cur_tld_id == null) {
				    cur_tld_id = tld_id++;
				    uniqueTLD.put(tld, cur_tld_id);
				    uniqueLines.put(tld, 1);
				    // Only count first time we encounter it
				    m.put(colf_counts, r_fqdns, one);

				    // Remember our TLDID. This should be known outside of this. Doing it this
				    // way to get estimate of cost of storage.
				    final String str_tld_id = String.format("%s_id", tld);
				    final Text col_tld_id = new Text(str_tld_id);
				    final String str_tld_id_val = String.format("%d", cur_tld_id);
				    final Value val_tld_id = new Value(str_tld_id_val.getBytes());
				    m.put(colf_zones, col_tld_id, val_tld_id);

				} else {

				    Integer line_cnt = (Integer)uniqueLines.get(tld);
				    line_cnt++;
				    uniqueLines.put(tld, line_cnt);

				    // Count unique client IPs
				    if (uniqueClient.get(client) == null) {
					// Only count first time we encounter it
					uniqueClient.put(client, 1);
					m.put(colf_counts, r_clientip, one);
				    }

				    // Count total per client ip
				    m.put(colf_clients, client_text, one);

				    // Count total per client port
				    m.put(colf_ports, new Text(port), one);

				    // Remember per rrtype per fqdn counts (N rrtype columns)
				    if (rrtype_details) {
					m.put(colf_details, new Text(String.format("%s_%s", tld, rrtype)), one);
				    
					// Column family per TLD containing the raw lines, one per column

					// TODO => Construct smaller raw log entry containing a more concise version of this raw data.
					// For example, we don't care about tld, because we know it. Nor do we care about a second
					// granuality timestamp because it's not useful. Nor do we care about process name.

					final String line = String.format("fra01 %s %s %s %s - %s", parts[6],parts[9],parts[10],parts[11],parts[13]);
					m.put(new Text(String.format("%d", cur_tld_id)), new Text(String.format("%d", line_cnt)), new Value(line.getBytes()));
					// Count & Remember unique details that we encounter in per 
					// mDetails.put(colf_details, new Text("client"), new Value(client.getBytes()));
					// mDetails.put(colf_details, new Text("fqdn"), new Value(fqdn.getBytes()));
					// mDetails.put(colf_details, new Text("reply"), new Value(answer.getBytes()));
					// mDetails.put(colf_details, new Text("rrtype"), new Value(rrtype.getBytes()));
				    }

				    // A Record Counter
				    if (rrtype.equals("A")) {
					m.put(colf_counts, r_a, one);
				    }
				    // AAAA Record Counter
				    else if (rrtype.equals("AAAA")) {
					m.put(colf_counts, r_aaaa, one);
				    }
				    // PTR Record Counter
				    else if (rrtype.equals("PTR")) {
					m.put(colf_counts, r_ptr, one);
				    }

				    // TXT Record Counter
				    else if (rrtype.equals("TXT")) {
					m.put(colf_counts, r_txt, one);
				    }
				    // SRV Record Counter
				    else if (rrtype.equals("SRV")) {
					m.put(colf_counts, r_srv, one);
				    }
				    // SOA Record Counter
				    else if (rrtype.equals("SOA")) {
					m.put(colf_counts, r_soa, one);
				    } 
				    // SPF Record Counter
				    else if (rrtype.equals("SPF")) {
					m.put(colf_counts, r_spf, one);
				    }
				    // MX Record Counter
				    else if (rrtype.equals("MX")) {
					m.put(colf_counts, r_mx, one);
				    }
				    // ANY Record Counter
				    else if (rrtype.equals("ANY")) {
					m.put(colf_counts, r_any, one);
				    }
				    // DS Record Counter
				    else if (rrtype.equals("DS")) {
					m.put(colf_counts, r_ds, one);
				    }
				    // A6 Record Counter
				    else if (rrtype.equals("A6")) {
					m.put(colf_counts, r_a6, one);
				    }
				    // RRSIG Record Counter
				    else if (rrtype.equals("RRSIG")) {
					m.put(colf_counts, r_rrsig, one);
				    }
				    // DNSKEY Record Counter
				    else if (rrtype.equals("DNSKEY")) {
					m.put(colf_counts, r_dnskey, one);
				    }
				    // NS Record Counter
				    else if (rrtype.equals("NS")) {
					m.put(colf_counts, r_ns, one);
				    }
				    // CNAME Record Counter
				    else if (rrtype.equals("CNAME")) {
					m.put(colf_counts, r_cname, one);
				    }
				    // NAPTR Record Counter
				    else if (rrtype.equals("NAPTR")) {
					m.put(colf_counts, r_naptr, one);
				    }
				    // TKEY Record Counter
				    else if (rrtype.equals("TKEY")) {
					m.put(colf_counts, r_tkey, one);
				    }
				    // SSHFP Record Counter
				    else if (rrtype.equals("SSHFP")) {
					m.put(colf_counts, r_sshfp, one);
				    }
				    // HINFO Record Counter
				    else if (rrtype.equals("HINFO")) {
					m.put(colf_counts, r_hinfo, one);
				    }
				    else {
					System.out.printf("%s - WARN: Unknown Record %d\n", rrtype,counter);
				    }


				}
			    } else {
				// Remeber that we've had a query for this TLD
				System.out.printf("%s => ERROR\n", fqdn);
				errors++;
				m.put(colf_counts, r_error, one);
			    }

			    bw.addMutation(m);

			} else {
			    System.out.printf("%d Invalid Record => %s\n", counter, logLine);
			}
			cracked++;
			if (counter % 10000 == 0) {
			    System.out.printf("Count %d\n", counter);
			    // mtbw.flush();
			    bw.flush();
			}
		    }
		}
	    } catch(Exception e) {
		System.out.println(e.getMessage());
	    }
	    finally {
		System.out.printf("Records %d\n", counter);
		System.out.printf("Cracked %d\n", cracked);
		System.out.printf("Ignore  %d\n", ignores);
		System.out.printf("Skips   %d\n", skips);
		System.out.printf("Errors  %d\n", errors);
		System.out.printf("TLDs    %d\n", uniqueTLD.size());
		System.out.printf("Clients %s\n", uniqueClient.size());

		/*
		  Mutation m = new Mutation(row_counts);
		  m.put(colf_counts, new Text(node), new Value(ByteBuffer.allocate(4).putInt(cracked).array()));
		  bw.addMutation(m);
		*/

		if (in != null) { try { in.close(); } catch(Throwable t) { /* ensure close happens */ } }
	    }

	    // mtbw.close();
	    bw.close();

	    try {

		if (accumulo == null) {
		    System.out.printf("Flush %s...\n", tableName);
		    connector.tableOperations().flush(tableName);

		    System.out.printf("Offline %s...\n", tableName);
		    connector.tableOperations().offline(tableName);

		    String targetHdfsDirectory = String.format("/tmp/export_%s", tableName);
		    System.out.printf("Export %s => %s...\n", tableName, targetHdfsDirectory);
		    connector.tableOperations().exportTable(tableName, targetHdfsDirectory);
		
		    // TODO: DISTCP the puppy and get out of HDFS and to local file system
		    System.out.printf("TODO: DistCP %s...\n", tableName);

		    // TODO: Delete the table
		    System.out.printf("TODO: Delete %s...\n", tableName);
		    // connector.tableOperations().delete(tableName);
		
		}
	    } catch(Exception e) {
		System.out.println(e.getMessage());
	    }

	    if (accumulo != null) {
		System.out.printf("Stopping MiniCluster\n");
		accumulo.stop();
	    }

	} catch(Exception e) {
	    System.out.println(e.getMessage());
	}
    }

}
