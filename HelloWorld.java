import com.google.common.net.InternetDomainName;
import com.google.common.collect.ImmutableList;

public class HelloWorld { 

    public static String stripChars(String input, String strip) {
	StringBuilder result = new StringBuilder();
	for (char c : input.toCharArray()) {
	    if (strip.indexOf(c) == -1) {
		result.append(c);
	    }
	}
	return result.toString();
    }

    private static String getTopPrivateDomain(String host) {
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
		if (domainName.parent().hasParent())
		    return String.format("%s => %s", domainName.toString(), domainName.parent().parent().toString());
		else
		    return String.format("%s => %s", domainName.toString(), domainName.parent().toString());
	    }
	}
	catch (Exception e) {
	    System.out.println(e.getMessage());
	}
	return null;
    }

    public static void main(String[] args) { 

	System.out.println("Hello");

	ImmutableList<String> hosts = ImmutableList.of(
						      "example.google.com", 
						      "ebay.co.uk", 
                                                      "_adsp._domainkey.resources.sourceforge.com",
                                                      "*.amazon.com",
						      "b.ssl.fastly.net"
						      );
	for (String host : hosts) {
	    System.out.println(host + " -> " + getTopPrivateDomain(host));
	}

    }
}
