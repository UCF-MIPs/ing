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
	s = "ELECTRONIC messages sent to Moscow on the day Sergei Skripal and his daughter were poisoned included the phrase \"the package has been delivered\". <br><br><span class=\"quote\">&gt;During a routine trawl through the previous 24 hours' intercepts, an RAF Signals Intelligence Officer alerted a senior officer to an electronic message which had been spotted the previous day. It said two named individuals had made a successful departure. </span><br><br>the message was sent on the same day of the poisonings, from a location near Damascus in Syria to \"an official\" in Moscow including the phrase 'the package has been delivered\" and saying that two individuals had \"made a successful egress\". <br>http://archive.is/IemtK<br><span class=\"quote\">&gt;https://www.express.co.uk/news/wor<wbr>ld/942903/Sergei-Skripal-russian-sp<wbr>y-poisoning-russian-message-interce<wbr>pted</span><br>also scores dead in Syria gas attack, rescuers and medics say<br><span class=\"quote\">&gt;http://www.bbc.com/news/world-midd<wbr>le-east-43686157</span>, w0ABfHYp, 167211216 npr.org www.bbc.com npc.org The United Nations Security Council (UNSC) has gathered at Russia's request at its New York headquarters, <br>to discuss the poisoning of ex-double agent Sergei Skripal and his daughter Yulia in the UK last month.<br>The extraordinary meeting was requested by Moscow, following the announcement made by the secretive British Porton Down chemical laboratory, <br>that it had not established that the nerve agent used to poison the Skripals was of Russian origin.<br /><br><br>https://www.rt.com/news/423329-unsc<wbr>-meeting-skripal-case/, 9gTf6hzK, 166870849 <a href=\"https://www.w3schools.com\">Visit W3Schools.com!</a>"
	urls = ing.detect_URLs(s)
	print(s)
	print(urls)

if __name__ == '__main__':
	main()