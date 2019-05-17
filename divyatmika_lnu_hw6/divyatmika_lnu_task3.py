from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from collections import Counter, defaultdict
import sys
import random
from operator import itemgetter

consumer_key = "oxTrM53VKMV3j8h9Hin2CToQO"
consumer_secret = "QIlhfrAa61VhsN1SJgZa13HE6yv18hfmtOncxu7YrcsB8QSD7V"
access_token = "3045669103-bzMLk8pKYeKNKUMifYfbW1TssjloUDZ74RWCrxJ"
access_token_secret = "GBIXv9wyolEWM9U6kTnRp7F14dgvvlFufnt145Aduq0LD"


class StdOutListener(StreamListener):
    mylist = []
    mydct = {}
    i = 0
    mylst = []
    len_list = []
    item_index = 0
    cnt = 0
    hash_dict = dict()
    tweet_len_dict = dict()
    hashtag_count_dict = defaultdict(lambda: 0)
    hashes_list = []

    sum_of_len = 0

    def decision(self, probability):
        return random.random() < probability

    def on_status(self, status):

        if self.item_index < 100:

            self.hashes_list = status.entities.get('hashtags')

            for each in self.hashes_list:
                if len(each['text']) != 0:
                    self.hashtag_count_dict[each['text']] += 1

            self.len_list.append(len(status.text))

            self.tweet_len_dict[self.item_index] = len(status.text)

            if len(self.len_list) == 100:
                self.sum_of_len = sum(self.len_list)

            if len(self.hashes_list) != 0:

                for each in self.hashes_list:
                    self.hash_dict[self.item_index] = each['text']

            self.item_index += 1

        else:

            self.item_index += 1
            self.prob = float(100.0/self.item_index)

            self.decision_to_include = self.decision(self.prob)

            if self.decision_to_include:
                index_to_replace_1 = random.uniform(1, 100)
                index = int(index_to_replace_1)

                if index in self.hash_dict:
                    hash_to_decrement = self.hash_dict[index]
                    self.hashtag_count_dict[hash_to_decrement] = self.hashtag_count_dict[hash_to_decrement] - 1

                    self.hash_dict[index] = None

                self.hashes_list = status.entities.get('hashtags')

                for each in self.hashes_list:
                    if len(each['text']) != 0:
                        self.hashtag_count_dict[each['text']] += 1
                        self.hash_dict[index] = each['text']

                if None in self.hashtag_count_dict:
                    del self.hashtag_count_dict[None]
                    
                self.sum_of_len = self.sum_of_len - self.tweet_len_dict[index] + len(status.text)
                sorted_hashtags = sorted(self.hashtag_count_dict.items(), key=itemgetter(0),reverse=False)
                sorted_hashtags1 = sorted(sorted_hashtags, key=itemgetter(1),reverse=True)
                
                top_3_hashtags = sorted_hashtags[:3]

                print("\n")
                print("The number of tweets with tags from the beginning:" + str(self.item_index))
                
                #print("Top 5 hot hashtags :")
                #for each in top_3_hashtags:
                    #print(each[0] + ":" + str(each[1]))
                flag_count = 1
                value = sorted_hashtags1[0][1]
                #print(sorted_hashtags1)
                for each in sorted_hashtags1: 
                    if each[1] < value:
                        flag_count +=1
                        value = each[1]
                        if flag_count < 4:
                            print(each[0] + ":" + str(each[1]))
                    elif each[1] == value:
                        if flag_count < 4:
                            print(each[0] + ":" + str(each[1]))
                
                

        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    stream.filter(track=['GameOfThrones','ML','Data','USA','France','Trump','India','Los Angeles','NY','Maryland'],languages=['en'])
