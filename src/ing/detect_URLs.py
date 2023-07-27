

def detect_URLs(s):
	# input: s, a string
	# output: a list of strings, each containing a URL
	tokens = s.split()
	urls = []
	for token in tokens:
		if token.find("://") != -1: # check for the :// that seperates the protocol from the subdomain first
			urls.append(token)
		else:
			t = [x for x in token.split(".") if x != ''] # even a shortened url must contain at least one period, separating the domain name from the domain extension
			if len(t) > 1:
				urls.append(token)
	return urls


