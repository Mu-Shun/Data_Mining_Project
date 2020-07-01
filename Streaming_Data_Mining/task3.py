import tweepy
import json
import sys
import random
import time

start_time = time.time()
port = int(sys.argv[1])
output_file = sys.argv[2]
# output_file = "covid3.csv"
with open(output_file, "w", encoding="utf-8") as output_file1:
	output_file1.close()

class MyStreamListener(tweepy.StreamListener):
	count = 1
	dic_for_seq = {}
	dic_for_tag = {}
	def on_status(self, status):
		if not status.entities['hashtags']:
			return
		messages = status.entities['hashtags']
		tag_list = []
		for messages in messages:
			tag_list.append(messages['text'])
		if self.count <= 100:
			self.dic_for_seq[self.count] = tag_list

			for tag in tag_list:
				if tag in self.dic_for_tag:
					self.dic_for_tag[tag] = self.dic_for_tag[tag] + 1
				else:
					self.dic_for_tag[tag] = 1
		else:
			pro_to_choice = random.choices([0,1], weights=(1-100/(self.count-1),100/(self.count-1)))[0]
			if pro_to_choice != 0:
				remove_num = random.choice(list(range(1,101)))
				for i in self.dic_for_seq[remove_num]:
					self.dic_for_tag[i] = self.dic_for_tag[i] - 1
				self.dic_for_seq[remove_num] = tag_list

				for tag in tag_list:
					if tag in self.dic_for_tag:
						self.dic_for_tag[tag] = self.dic_for_tag[tag] + 1
					else:
						self.dic_for_tag[tag] = 1

		res = []
		for i in self.dic_for_tag:
			res.append((i, self.dic_for_tag[i]))
		res.sort(key = lambda x: (-x[1], x[0]))
		# print(res)
		top_3 = 0
		cur_hig = res[0][1]
		index = 0
		while top_3 < 3:
			if index > len(res)-1 :
				break
			if res[index][1] == cur_hig:
				index = index + 1
			elif res[index][1] < cur_hig:
				cur_hig = res[index][1]
				top_3 = top_3 + 1
				if top_3 < 3:
					index = index + 1
				else:
					break

		print("The number of tweets with tags from the beginning: " + str(self.count), "Duration: ", time.time()- start_time)

		if res[:index]:
			with open(output_file, "a", encoding="utf-8") as output_file1:
				output_file1.write("The number of tweets with tags from the beginning: " + str(self.count) + "\n")
				for i in res[:index]:
					output_file1.write(str(i[0]) + " : " + str(i[1]) + "\n")
				output_file1.write("\n")
			output_file1.close()

		self.count = self.count + 1

if __name__ == "__main__":
    API_key = "tzEmtVNESCFSn5IxH7gLJI1Jw"
    API_secret_key = "qL2MQBbXTmohwXeSRbdJ9kO4b9sCcxheOv7EkjARKtNJ4PVdUs"
    access_token = "1274324961994235906-EScgwgsw0BFbBCMLZBeFPiqCQtnxZr"
    access_token_secret = "UDjy6LrUYaYIxVjm9Ps1z28qWVHSEyKvFR9A3CxiA10bU"

    auth = tweepy.OAuthHandler(API_key, API_secret_key)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    DataStream = tweepy.Stream(auth = api.auth, listener = MyStreamListener())
    DataStream.filter(track=['covid'])