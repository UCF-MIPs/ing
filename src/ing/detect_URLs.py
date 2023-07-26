import re

def detect_URLs(s):
	# input: s, a string
	# output: a list of strings, each containing a URL

	html_pattern = r"<(?:\s*(?:\\|/)*\s*(?:wbr)\s*(?:\\|/)*\s*)?>"
	html_strip = re.sub(html_pattern, '', s)

	url_pattern = r"(?:[A-z+.-]+://)?[A-z0-9]+\.[^<>\"#{}|\\^~[\]`\s]+"
	prog = re.compile(url_pattern)
	urls = prog.findall(html_strip)
	return urls

if __name__ == '__main__':
	main()