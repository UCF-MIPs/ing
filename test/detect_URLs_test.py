""" Include containing folder for testing """
import sys, os
import datetime

#sys.path.insert(0, os.path.abspath('/home/ec2-user/SageMaker/ing/src'))
sys.path.insert(0, os.path.abspath('../src'))
print(os.path.abspath('.'))
# ---------------------------------------

""" 
Test in local pycharm env:
    Uncomment the lines below to test in local machine.
    Otherwise keep them commented 
"""
#from .ing.src import ing
#from ing.src import S3Access
# -----------------------------


""" 
Test in aws:
    Uncomment the lines below to test on aws sagemaker. 
    Otherwise keep them commented.
"""
import ing
# -----------------------------

# s3 specific libraries
#import s3fs
#import boto3
#s3 = s3fs.S3FileSystem(anon=False)


"""
Usual module/package imports go below here
"""
import pandas as pd
# -----------------------------

'''
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
'''
def main():
	s = 'hello, 2wenty2wenty2.tumblr.com/post/678377331402932224, http://2wenty2wenty2.tumblr.com/post/678377331402932224, What Happened on Day 14 of Russia\'s Invasion of Ukraine - The New York Times\n"What Happened on Day 14 of Russia\'s Invasion of Ukraine - The New York Times" https://www.nytimes.com/live/2022/03/09/world/ukraine-russia-war\nA Russian strike hit a maternity hospital in the besieged southern city of Mariupol. The Kremlin accused the United States of waging "an eco, undefined contents'
	urls = ing.detect_URLs(s)
	print(s)
	print(urls)

if __name__ == '__main__':
	main()