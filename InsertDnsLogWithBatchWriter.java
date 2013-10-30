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
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.nio.ByteBuffer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

// import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import java.util.Collections;
import java.util.Iterator;

// guava-15.0.jar 
// https://code.google.com/p/guava-libraries/
import com.google.common.net.InternetDomainName;
import com.google.common.collect.ImmutableList;

public class InsertDnsLogWithBatchWriter {

    private static String getTopLevelDomain(String host, String rawLogLine) {
	try {
	    // Why do we get these ??? :-(
	    if (host.startsWith("https//")) {
		host = host.substring(7);
	    }

	    // This is techically valid if NOT in the root. But InternetDomainName will barf on them.
	    if (host.startsWith("*.")) {
		host = host.substring(2);
	    }

	    // This is techically valid if NOT in the root. But InternetDomainName will barf on them.
	    if (host.indexOf("_") != -1) {
		host = stripChars(host, "_");
	    }

	    InternetDomainName domainName = InternetDomainName.from(host);
	    if (domainName.isUnderPublicSuffix())
		return domainName.topPrivateDomain().toString();
	    else {
		// Deal with fastly.net and other similar things. Need to write our own version of InternetDomainName
		if (domainName.parent().hasParent())
		    return domainName.parent().parent().toString();
		else
		    return domainName.parent().toString();
	    }
	}
	catch (Exception e) {
	    System.out.printf("--------------%s\n%s\n", e.getMessage(), rawLogLine);
	}
	return null;
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

    public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, MutationsRejectedException, TableExistsException,
						  TableNotFoundException {
	if (args.length < 6) {
	    System.out
		.println("Usage: accumulo examples-simplejar accumulo.examples.helloworld.InsertDnsLogWithBatchWriter <instance name> <zoo keepers> <username> <password> <tableName> <logFile> <rowkey> <start> <maxrecords> <rrtype_details>");
	    System.exit(1);
	}
    
	final String instanceName = args[0];
	final String zooKeepers = args[1];
	final String user = args[2];
	final byte[] pass = args[3].getBytes();
	final String tableName = args[4];
	String records = "1000000";
	String rowkey = "1382375700";
	String logFile = "1382375700";
	int maxRecords = 0;
	boolean rrtype_details = false;

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
    
	Integer val = null;
	try {
	    val = Integer.parseInt(maxrecords);
	} catch (NumberFormatException nfe) { }
	if (val != null) {
	    maxRecords = val;
	}

	System.out.printf("logFile    => %s\nrowKey     => %s\nstart      => %d\nmaxRecords => %d\nrrtypes    => %b\n", logFile, rowkey, start, maxRecords, rrtype_details);

	final ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zooKeepers);
	final Connector connector = instance.getConnector(user, pass);
	final MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(200000l, 300, 4);
    
	BatchWriter bw = null;
    
	// Create table
	if (!connector.tableOperations().exists(tableName)) {

	    // Create the table
	    connector.tableOperations().create(tableName);
	    System.out.printf("Added table %s\n", tableName);

	    // Accumulate overall counts
	    final IteratorSetting iterCounts = new IteratorSetting (1, "counts", SummingCombiner.class);
	    SummingCombiner.setColumns(iterCounts, Collections.singletonList(new IteratorSetting.Column("counts")));
	    SummingCombiner.setEncodingType(iterCounts, SummingCombiner.Type.STRING);
	    connector.tableOperations().attachIterator(tableName, iterCounts);
	    System.out.printf("Added SummingCombiner for 'counts'\n");

	    // Accumulate overall per zone counts
	    final IteratorSetting iterZones = new IteratorSetting (11, "zones", SummingCombiner.class);
	    SummingCombiner.setColumns(iterZones, Collections.singletonList(new IteratorSetting.Column("zones")));
	    SummingCombiner.setEncodingType(iterZones, SummingCombiner.Type.STRING);
	    connector.tableOperations().attachIterator(tableName, iterZones);
	    System.out.printf("Added SummingCombiner for 'zones'\n");

	    // Accumulate overall per client counts
	    final IteratorSetting iterClients = new IteratorSetting (11, "clients", SummingCombiner.class);
	    SummingCombiner.setColumns(iterClients, Collections.singletonList(new IteratorSetting.Column("clients")));
	    SummingCombiner.setEncodingType(iterClients, SummingCombiner.Type.STRING);
	    connector.tableOperations().attachIterator(tableName, iterClients);
	    System.out.printf("Added SummingCombiner for 'clients'\n");

	}
	
	bw = mtbw.getBatchWriter(tableName);
    
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
	
	HashMap uniqueFQDN = new HashMap();
	HashMap uniqueClient = new HashMap();
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
	
	// Count & Remember details per zones that we encounter.
	final Text colf_clients = new Text("clients");

	// Timestamp is always our rowkey. We store the node that we found things in 
	final Text row_counts = new Text(rowkey);

	String node = null;

	try {
	    in = new BufferedReader(new FileReader(logFile));
	    while (in.ready()) {
		final String logLine = in.readLine();
		counter++;
		if (counter-start >= maxRecords) {
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
			String client = parts[6];
			String fqdn = stripChars(parts[7],"():'");
			String query = parts[9] + " " + parts[10] + " " + parts[11];
			String rrtype = parts[11];
			String answer = parts[13];
			// System.out.printf("%10d Node=%s Client=%s FQDN=%s RRType=%s Answer=%s\n", counter, node, client, fqdn, rrtype, answer);

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

			    Integer fqdn_cnt = (Integer)uniqueFQDN.get(tld);
			    if (fqdn_cnt == null) {
				uniqueFQDN.put(tld, 1);
				// Only count first time we encounter it
				m.put(colf_counts, r_fqdns, one);
			    } else {

				// Count unique client IPs
				if (uniqueClient.get(client) == null) {
				    // Only count first time we encounter it
				    uniqueClient.put(client, 1);
				    m.put(colf_counts, r_clientip, one);
				}

				// Count total per clientip
				m.put(colf_clients, r_clientip, one);

				// Remember per rrtype per fqdn counts (N rrtype columns)
				if (rrtype_details) {
				    m.put(colf_details, new Text(String.format("%s_%s", tld, rrtype)), one);
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
			mtbw.flush();
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
	    System.out.printf("FQDNs   %d\n", uniqueFQDN.size());
	    System.out.printf("Client  %s\n", uniqueClient.size());

	    /*
	    Mutation m = new Mutation(row_counts);
	    m.put(colf_counts, new Text(node), new Value(ByteBuffer.allocate(4).putInt(cracked).array()));
	    bw.addMutation(m);
	    */

	    if (in != null) { try { in.close(); } catch(Throwable t) { /* ensure close happens */ } }
	}

	mtbw.close();
    }
  
}
