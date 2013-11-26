/*
 * @componentry - Dns Log Cracker / Event Acceptor 
 */

package com.dyn.accumulo;

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

public class CrackDnsLog {

    public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, MutationsRejectedException, TableExistsException, TableNotFoundException 
    {
	if (args.length < 6) {
	    System.out.println("Usage: accumulo examples-simplejar accumulo.examples.helloworld.InsertDnsLogWithBatchWriter <instance name> <zoo keepers> <username> <password> <tableName> <logFile> <rowkey> <nodeid> <start> <maxrecords> <rrtype_details> <graph_details> <use-mini-accumulo>");
	    System.exit(1);
	}
    
	final String instanceName = args[0];
	final String zooKeepers = args[1];
	final String user = args[2];
	final byte[] pass = args[3].getBytes();
	final String tableName = args[4];
	String records = "1000000";
	String rowkey = null;
	String nodeid = null;
	String logFile = "1382375700";
	int maxRecords = 0;
	boolean rrtype_details = false;
	boolean graph_details = false;
	boolean useMiniAccumulo = false;
	boolean clientAggregates = false;
	boolean portAggregates = false;
	boolean hostAggregates = false;
	boolean rrtypeAggregates = false;

	if (args.length > 5) {
	    logFile = args[5];
	}

	if (args.length > 6) {
	    rowkey = args[6];
	}

	if (args.length > 7) {
	    nodeid = args[7];
	}

	int start = 0;
	if (args.length > 8) {
	    try {
		start = Integer.parseInt(args[8]);
	    } catch(Exception e) {
		System.out.println("WARN: Invalid <START>, assuming 0");
	    }
	}

	String maxrecords = "1000000";
	if (args.length > 9)
	    maxrecords = args[9];

	if (args.length > 10)
	    rrtype_details = args[10].equals("true");
    
	if (args.length > 11)
	    graph_details = args[11].equals("true");
    
	if (args.length > 12)
	    useMiniAccumulo = args[12].equals("true");
    
	Integer val = null;
	try {
	    val = Integer.parseInt(maxrecords);
	} catch (NumberFormatException nfe) { }
	if (val != null) {
	    maxRecords = val;
	}

	System.out.printf("logFile    => %s\n", logFile);
	System.out.printf("rowKey     => %s\n", rowkey);
	System.out.printf("nodeid     => %s\n", nodeid);
	System.out.printf("start      => %d\n", start);
	System.out.printf("maxRecords => %d\n", maxRecords);
	System.out.printf("rrtypes    => %b\n", rrtype_details);
	System.out.printf("graphs     => %b\n", graph_details);
	System.out.printf("use-mini   => %b\n", useMiniAccumulo);

	MiniAccumuloCluster accumulo = null;

	try {

	    ZooKeeperInstance instance = null; // new ZooKeeperInstance(instanceName, zooKeepers);
	    Connector connector = null; // instance.getConnector(user, pass);

	    if (useMiniAccumulo && accumulo == null) {
		File tempDirectory = new File("/home/accumulo/temp/");
		System.out.printf("Creating MiniCluster\n");
		accumulo = new MiniAccumuloCluster(tempDirectory, new String(pass));
		System.out.printf("Starting MiniCluster\n");
		accumulo.start();
		System.out.printf("Creating Zookeeper Instance\n");
		instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
		System.out.printf("Getting Connector\n");
		connector = instance.getConnector(user, pass); // new PasswordToken(pass));
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
		System.exit(1);
	    }
	
	    BatchWriter bwGraph = null;
	    BatchWriter bw = connector.createBatchWriter(tableName, 2000000l, 0, 4);

	    if (graph_details) {
		 bwGraph = connector.createBatchWriter(graphTable, 2000000l, 0, 4);
	    }
    
	    BufferedReader in = null;
	    final Value v_one = new Value("1".getBytes());
	    final Text col_query = new Text("query");
	    final Text col_a = new Text("A");
	    final Text col_aaaa = new Text("AAAA");
	    final Text col_ptr = new Text("PTR");
	    final Text col_txt = new Text("TXT");
	    final Text col_srv = new Text("SRV");
	    final Text col_soa = new Text("SOA");
	    final Text col_spf = new Text("SPF");
	    final Text col_mx = new Text("MX");
	    final Text col_any = new Text("ANY");
	    final Text col_ds = new Text("DS");
	    final Text col_a6 = new Text("A6");
	    final Text col_rrsig = new Text("RRSIG");
	    final Text col_dnskey = new Text("DNSKEY");
	    final Text col_ns = new Text("NS");
	    final Text col_cname = new Text("CNAME");
	    final Text col_naptr = new Text("NAPTR");
	    final Text col_tkey = new Text("TKEY");
	    final Text col_sshfp = new Text("SSHFP");
	    final Text col_hinfo = new Text("HINFO");
	    final Text col_unknown = new Text("UNKNOWN");
	    final Text col_fqdns = new Text("fqdns");
	    final Text col_clientip = new Text("clientip");
	    final Text col_error = new Text("error");
	    final Text col_nodeid = new Text(nodeid);
	
	    HashMap uniqueTLD = new HashMap();
	    HashMap uniqueFQDN = new HashMap();
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

	    // Timestamp is always our rowkey. We store the node that we found things in 
	    // TODO: We could get much more fine-grained on this (second within the 5 minute range, or millisecond). Current precision in log file doesn't allow this.
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
				final Text col_answer = new Text(stripChars(answer, "()"));
				final Text colf_rrtype = new Text(rrtype);
				final Text col_fqdn = new Text(fqdn);
				Mutation mGraphFQDN = new Mutation(col_fqdn);
				Mutation mGraphAnswer = new Mutation(col_answer);
				mGraphFQDN.put(colf_rrtype, col_answer, v_one);
				mGraphAnswer.put(colf_rrtype, col_fqdn, v_one);
				bwGraph.addMutation(mGraphFQDN);
				bwGraph.addMutation(mGraphAnswer);
			    }
			    
			    Mutation m = new Mutation(row_counts);

			    // Track and remember counts for each unique record type that we answer for
			    // All RRType Counter
			    m.put(colf_counts, col_query, v_one);

			    // Remember the node that we processed this record for
			    m.put(colf_counts, col_nodeid, v_one);

			    // Get TLD
			    String tld = getTopLevelDomain(fqdn, logLine);

			    if (tld != null) {	
				// Remember that we've had a query for this TLD
				final Text col_tld = new Text(tld);
				m.put(colf_counts, col_tld, v_one);

				// m.put(colf_zones, col_tld, v_one);

				if (hostAggregates) {
				    final Text col_fqdn = new Text(fqdn);
				    m.put(col_tld, col_fqdn, v_one);
				}

				Integer cur_tld_id = (Integer)uniqueTLD.get(tld);
				if (cur_tld_id == null) {
				    cur_tld_id = tld_id++;
				    uniqueTLD.put(tld, cur_tld_id);
				    uniqueLines.put(tld, 1);
				    // Only count first time we encounter it
				    m.put(colf_counts, col_fqdns, v_one);

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
					m.put(colf_counts, col_clientip, v_one);
				    }

				    if (clientAggregates) {
					// Count total per client ip
					m.put(colf_clients, client_text, v_one);
				    }
					
				    if (portAggregates) {
					// Count total per client port
					m.put(colf_ports, new Text(port), v_one);
				    }

				    // Remember per rrtype per fqdn counts (N rrtype columns)
				    if (rrtype_details) {
					m.put(colf_details, new Text(String.format("%s_%s", tld, rrtype)), v_one);
				    
					// Column family per TLD containing the raw lines, one per column
					// TODO => Construct smaller raw log entry containing a more concise version of this raw data.
					final String line = String.format("%s %s %s %s %s - %s", nodeid,parts[9],parts[10],parts[11],parts[13]);
					m.put(new Text(String.format("%d", cur_tld_id)), new Text(String.format("%d", line_cnt)), new Value(line.getBytes()));
				    }

				    if (rrtypeAggregates) {
					// A Record Counter
					if (rrtype.equals("A")) {
					    m.put(colf_counts, col_a, v_one);
					}
					// AAAA Record Counter
					else if (rrtype.equals("AAAA")) {
					    m.put(colf_counts, col_aaaa, v_one);
					}
					// PTR Record Counter
					else if (rrtype.equals("PTR")) {
					    m.put(colf_counts, col_ptr, v_one);
					}

					// TXT Record Counter
					else if (rrtype.equals("TXT")) {
					    m.put(colf_counts, col_txt, v_one);
					}
					// SRV Record Counter
					else if (rrtype.equals("SRV")) {
					    m.put(colf_counts, col_srv, v_one);
					}
					// SOA Record Counter
					else if (rrtype.equals("SOA")) {
					    m.put(colf_counts, col_soa, v_one);
					} 
					// SPF Record Counter
					else if (rrtype.equals("SPF")) {
					    m.put(colf_counts, col_spf, v_one);
					}
					// MX Record Counter
					else if (rrtype.equals("MX")) {
					    m.put(colf_counts, col_mx, v_one);
					}
					// ANY Record Counter
					else if (rrtype.equals("ANY")) {
					    m.put(colf_counts, col_any, v_one);
					}
					// DS Record Counter
					else if (rrtype.equals("DS")) {
					    m.put(colf_counts, col_ds, v_one);
					}
					// A6 Record Counter
					else if (rrtype.equals("A6")) {
					    m.put(colf_counts, col_a6, v_one);
					}
					// RRSIG Record Counter
					else if (rrtype.equals("RRSIG")) {
					    m.put(colf_counts, col_rrsig, v_one);
					}
					// DNSKEY Record Counter
					else if (rrtype.equals("DNSKEY")) {
					    m.put(colf_counts, col_dnskey, v_one);
					}
					// NS Record Counter
					else if (rrtype.equals("NS")) {
					    m.put(colf_counts, col_ns, v_one);
					}
					// CNAME Record Counter
					else if (rrtype.equals("CNAME")) {
					    m.put(colf_counts, col_cname, v_one);
					}
					// NAPTR Record Counter
					else if (rrtype.equals("NAPTR")) {
					    m.put(colf_counts, col_naptr, v_one);
					}
					// TKEY Record Counter
					else if (rrtype.equals("TKEY")) {
					    m.put(colf_counts, col_tkey, v_one);
					}
					// SSHFP Record Counter
					else if (rrtype.equals("SSHFP")) {
					    m.put(colf_counts, col_sshfp, v_one);
					}
					// HINFO Record Counter
					else if (rrtype.equals("HINFO")) {
					    m.put(colf_counts, col_hinfo, v_one);
					}
					else {
					    // UNKNOWN Record Counts
					    System.out.printf("%s - WARN: Unknown Record %d\n", rrtype,counter);
					    m.put(colf_counts, col_unknown, v_one);
					}
				    }
				}
			    } else {
				// Remeber that we've had a query for this TLD
				System.out.printf("%s => ERROR\n", fqdn);
				errors++;
				m.put(colf_counts, col_error, v_one);
			    }

			    bw.addMutation(m);

			} else {
			    System.out.printf("%d Invalid Record => %s\n", counter, logLine);
			}
			cracked++;
			if (counter % 10000 == 0) {
			    System.out.printf("Count %d\n", counter);
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
		if (in != null) { try { in.close(); } catch(Throwable t) { /* ensure close happens */ } }
	    }

	    bw.close();
	    if (bwGraph != null) {
		bwGraph.close();
	    }
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

		System.out.printf("Flush %s...\n", tableName);
		connector.tableOperations().flush(tableName);

		System.out.printf("Offline %s...\n", tableName);
		connector.tableOperations().offline(tableName);

		String targetHdfsDirectory = String.format("/tmp/export_%s", tableName);
		System.out.printf("Export %s => %s...\n", tableName, targetHdfsDirectory);
		connector.tableOperations().exportTable(tableName, targetHdfsDirectory);
		
		// TODO: DISTCP the puppy and get out of HDFS and to local file system
		System.out.printf("TODO: DistCP %s...\n", tableName);

		System.out.printf("Stopping MiniCluster\n");
		accumulo.stop();
	    }

	} catch(Exception e) {
	    System.out.println(e.getMessage());
	}
    }

    private static String getTopLevelDomain(String host, String rawLogLine) {
	
	// http://mxr.mozilla.org/mozilla-central/source/netwerk/dns/effective_tld_names.dat?raw=1
	boolean isSingle = false;
	String[] parts = host.toLowerCase().split("\\.");
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


}
