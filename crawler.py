import argparse
import json
import logging
import os
import time
import traceback
import pandas as pd

from omegaconf import OmegaConf
from typing import List
from tqdm import tqdm
from tweepy import Client, User, Tweet, Paginator


# Logging format.
_LOG_FMT = '%(asctime)s - %(levelname)s - %(name)s -   %(message)s'
_DATE_FMT = '%m/%d/%Y %H:%M:%S'

# Set global logging.
logging.basicConfig(format=_LOG_FMT, datefmt=_DATE_FMT, level=logging.INFO)
LOG = logging.getLogger('__main__')


def parse_args():
    """ Arguments for running the model.
    
    Returns:
        Arguments.
    """
    parser = argparse.ArgumentParser('Twitter Social Network Crawler.')

    parser.add_argument('--cfg', type=str, default='config.yaml',
                        help='Path to config')
    
    return parser.parse_args()


def log_trace(e):
    """ Log expection traceback.
    
    Args:
        e: Exception.
    """
    LOG.error(''.join(traceback.TracebackException.from_exception(e).format()))


class Crawler:
    """ Twitter social network crawler (Twitter API v2). """
    
    def __init__(self, config):
        """ Constructor for `Crawler`.
        
        Args:
            config: Configuration for crawler.
        """
        self.config = config
        
        # Authenticate to the Twitter API v2.
        self.client = self.authenticate()
        
        # Create dictionary for storing tweet data.
        self.tweet_dict = {field:[] for field in self.config.TWEET_FIELDS if field != "entities"}
        if "entities" in self.config.TWEET_FIELDS:
            self.tweet_dict.update({"tag":[]})
        
        # Create dictionary for storing user data.
        self.user_dict = {field:[] for field in self.config.USER_FIELDS if field != "public_metrics"}
        if "public_metrics" in self.config.USER_FIELDS:
            self.user_dict.update({"followers_count":[], "following_count":[], "tweet_count":[]})

        # Create dictionary for storing social network interactions.
        self.follow_dict = {"user_id":[], "following_id":[]}
        self.post_dict = {"user_id":[], "tweet_id":[]}
        self.retweet_dict = {"user_id":[], "tweet_id":[]}
        self.like_dict = {"user_id":[], "tweet_id":[]}
    
    
    def authenticate(self):
        """ Authenticate to the Twitter API v2.
        
        Returns:
            Twitter API v2 Client.
        """
        client = Client(bearer_token=self.config.BEARER_TOKEN, wait_on_rate_limit=True)
        LOG.info(f"Authentication success ({'Academic' if self.config.ACADEMIC_ACCESS else 'General'} access).")
        return client
    
    
    def read_init(self):
        """ Read the initial query for the crawler.
        
        Returns:
            Initial query for the crawler.
        """      
        f = open(self.config.INIT_PATH)
        init_query = json.load(f)
        f.close()
        return init_query
    
    
    def update_tweet(self, tweet: Tweet):
        """ Update tweet data to the dictionary.
        
        Args:
            tweet: Tweet object.
        """
        if tweet.id not in self.tweet_dict["id"]:
            for field in self.tweet_dict:
                # Get hashtags from `entities`.
                if field == "tag" and tweet.entities and "hashtags" in tweet.entities:
                    value = [tag["tag"] for tag in tweet.entities["hashtags"]]
                
                # Get data from each field.
                else:
                    value = tweet.get(field)
    
                self.tweet_dict[field].append(value)
    
    
    def update_tweets(self, tweets: List[Tweet]):
        """ Update the list of tweets to the dictionary.
        
        Args:
            tweets: List of tweet objects.
        """
        for tweet in tweets:
            self.update_tweet(tweet)
    
    
    def update_user(self, user: User):
        """ Update the user data to the dictionary.
        
        Args:
            user: User object.
        """
        if user.id not in self.user_dict["id"]:
            for field in self.user_dict:
                # Get followers, followings, and tweet count from `public_metrics`.
                if field in ["followers_count", "following_count", "tweet_count"]:
                    value = user.public_metrics[field]
                
                # Get data from each field.
                else:
                    value = user.get(field)
                    
                self.user_dict[field].append(value)


    def update_users(self, users: List[User]):
        """ Update the list of user to the dictionary.
        
        Args:
            users: List of user objects.
        """
        for user in users:
            self.update_user(user)
    
    
    def update_following(self, user_id, following_id):
        """ Update the `follow` interaction to the dictionary.
        
        Args:
            user_id: User ID.
            following_id: Following user ID.
        """
        self.follow_dict["user_id"].append(user_id)
        self.follow_dict["following_id"].append(following_id)
    
    
    def update_post(self, user_id, tweet_id):
        """ Update the `post` interaction to the dictionary.
        
        Args:
            user_id: User ID.
            tweet_id: Tweet ID.
        """
        self.post_dict["user_id"].append(user_id)
        self.post_dict["tweet_id"].append(tweet_id)
    
    
    def update_retweet(self, user_id, tweet_id):
        """ Update the `retweet` interaction to the dictionary.
        
        Args:
            user_id: User ID.
            tweet_id: Tweet ID.
        """
        self.retweet_dict["user_id"].append(user_id)
        self.retweet_dict["tweet_id"].append(tweet_id)
    
    
    def update_like(self, user_id, tweet_id):
        """ Update the `like` interaction to the dictionary.
        
        Args:
            user_id: User ID.
            tweet_id: Tweet ID.
        """
        self.like_dict["user_id"].append(user_id)
        self.like_dict["tweet_id"].append(tweet_id)
    
    
    def get_user_followings(self, user_id):
        """ Get the users that the inputted user ID is following. 
        
        Args:
            user_id: User ID.
        """
        # Set paginator.
        pages = Paginator(self.client.get_users_following,
            id=user_id,
            user_fields=list(self.config.USER_FIELDS),
            max_results=self.config.FOLLOWING_MAX_RESULTS,
            limit=self.config.FOLLOWING_LIMIT
        )
        
        # Crawl data from each page.
        for following_users in pages:
            time.sleep(self.config.SLEEP_TIME) # Sleep to avoid rate limit (Tweepy docs: https://bit.ly/3L5qxbA).
            if not following_users.errors and following_users.meta["result_count"] > 0:
                for following_user in following_users.data:
                    if not following_user.protected: # Filter out protected user account.
                        self.update_following(user_id=user_id, following_id=following_user.id)
                        self.update_user(following_user)
    
    
    def get_user_posts(self, user=None, context=None, keyword=None, is_init=False, search_all=False):
        """ Get the user posts.
        
        Args:
            user: User ID or username.
            context: Specific domain id/entity id pair.
                (e.g., 131.840160819388141570 for Tech news).
                [See all available contexts.](https://github.com/twitterdev/twitter-context-annotations)
            keyword: Keyword or hashtag.
            is_init: Whether it is an initial crawl (Default=`False`).

        [Learn how to build queries](https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query)
        """
        # Build query.
        query = self.config.QUERY
        query += "" if not user else f" ({user})" if user.startswith("from:") else f" (from:{user})"
        query += "" if not context else f" ({context})" if context.startswith("context:") else f" (context:{context})"
        query += "" if not keyword else f" ({keyword})"
        
        # Set result limit.
        max_results = self.config.INIT_MAX_RESULTS if is_init else self.config.POST_MAX_RESULTS
        limit = self.config.INIT_LIMIT if is_init else self.config.POST_LIMIT
        
        # Set search method.
        method = self.client.search_all_tweets if search_all else self.client.search_recent_tweets
        
        # Set paginator.
        pages = Paginator(method,
            query=query,
            tweet_fields=list(self.config.TWEET_FIELDS),
            user_fields=list(self.config.USER_FIELDS),
            expansions=self.config.EXPANSIONS,
            start_time=self.config.START_TIME,
            end_time=self.config.END_TIME,
            max_results=max_results,
            limit=limit
        )

        # Crawl data from each page.
        for posts in pages:
            time.sleep(self.config.SLEEP_TIME) # Sleep to avoid rate limit (Tweepy docs: https://bit.ly/3L5qxbA).
            if not posts.errors and posts.meta["result_count"] > 0:
                for post in posts.data:
                    self.update_post(user_id=post.author_id, tweet_id=post.id)
                    self.update_tweet(post)
        
                # if not user:
                self.update_users(users=posts.includes["users"])
    
    
    def get_user_retweets(self, user, search_all=False):
        """ Get the user retweets.
        
        Args:
            user: User ID or username.
        
        [Learn how to build queries](https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query)
        """
        # Build query.
        query = self.config.RETWEET_QUERY
        query += "" if not user else f" ({user})" if user.startswith("from:") else f" (from:{user})"
        
        # Set search method.
        method = self.client.search_all_tweets if search_all else self.client.search_recent_tweets
        
        # Set paginator.
        pages = Paginator(method,
            query=query,
            tweet_fields=list(self.config.TWEET_FIELDS),
            user_fields=list(self.config.USER_FIELDS),
            expansions=self.config.EXPANSIONS,
            start_time=self.config.START_TIME,
            end_time=self.config.END_TIME,
            max_results=self.config.RETWEET_MAX_RESULTS,
            limit=self.config.RETWEET_LIMIT
        )
        
        # Crawl data from each page.
        for retweets in pages:
            time.sleep(self.config.SLEEP_TIME) # Sleep to avoid rate limit (Tweepy docs: https://bit.ly/3L5qxbA).
            if not retweets.errors and retweets.meta["result_count"] > 0:
                for retweet in retweets.data:
                    self.update_retweet(user_id=user, tweet_id=retweet.id)
                    self.update_tweet(retweet)
    
    
    def get_user_liked_tweets(self, user_id):
        """ Get the user liked tweets.
        
        Args:
            user_id: User ID.
        """
        # Set paginator.
        pages = Paginator(self.client.get_liked_tweets,
            id=user_id,
            tweet_fields=list(self.config.TWEET_FIELDS),
            user_fields=list(self.config.USER_FIELDS),
            expansions=self.config.EXPANSIONS,
            max_results=self.config.LIKE_MAX_RESULTS,
            limit=self.config.LIKE_LIMIT
        )
        
        # Crawl data from each page.
        for likes in pages:
            time.sleep(self.config.SLEEP_TIME) # Sleep to avoid rate limit (Tweepy docs: https://bit.ly/3L5qxbA).
            if not likes.errors and likes.meta["result_count"] > 0:
                for like in likes.data:
                    self.update_like(user_id=user_id, tweet_id=like.id)
                    self.update_tweet(like)
    
    
    def init_seed_tweets(self):
        """ Initial crawl for seed tweets. """
        LOG.info(f"Initialize seed tweets from '{self.config.INIT_PATH}' ...")
        init_query = self.read_init()
        for query in init_query:
            self.get_user_posts(**query, is_init=True, search_all=self.config.ACADEMIC_ACCESS)
    
    
    def crawl(self):
        """ Crawl social networks in Twitter. """
        try:
            # Initialize seed tweets and seed users.
            self.init_seed_tweets()
            seed_users = list(self.user_dict["id"])
            
            LOG.info(f"Number of seed tweets: {len(self.tweet_dict['id'])}")
            LOG.info(f"Number of seed users: {len(seed_users)}")

            # Get `follow` interactions.
            LOG.info("Get follow interactions ...")
            for user_id in tqdm(seed_users):
                self.get_user_followings(user_id=user_id)

            # Get `post/retweet/like` interactions.
            LOG.info("Get post/retweet/like interactions ...")
            users = list(self.user_dict["id"])
            for user_id in tqdm(users):
                self.get_user_posts(user=str(user_id), search_all=self.config.ACADEMIC_ACCESS)
                self.get_user_retweets(user=str(user_id), search_all=self.config.ACADEMIC_ACCESS)
                self.get_user_liked_tweets(user_id=user_id)
            
            # Log summary.
            LOG.info("-"*50)
            LOG.info("Summary")
            LOG.info("-"*50)
            LOG.info(f"Total of 'follow' interactions: {len(self.follow_dict['user_id'])}")
            LOG.info(f"Total of 'post' interactions: {len(self.post_dict['user_id'])}")
            LOG.info(f"Total of 'retweet' interactions: {len(self.retweet_dict['user_id'])}")
            LOG.info(f"Total of 'like' interactions: {len(self.like_dict['user_id'])}")
            LOG.info(f"Total of 'users': {len(self.user_dict['id'])}")
            LOG.info(f"Total of 'tweets': {len(self.tweet_dict['id'])}")
            LOG.info("-"*50)

        except Exception as e:
            log_trace(e)
    
    
    def save(self):
        """ Save data to file. """
        if not os.path.exists(self.config.SAVE_PATH):
            os.makedirs(self.config.SAVE_PATH)
        
        pd.DataFrame(self.tweet_dict).to_csv(os.path.join(self.config.SAVE_PATH, "tweet.csv"), index=False)
        pd.DataFrame(self.user_dict).to_csv(os.path.join(self.config.SAVE_PATH, "user.csv"), index=False)
        pd.DataFrame(self.follow_dict).to_csv(os.path.join(self.config.SAVE_PATH, "follow.csv"), index=False)
        pd.DataFrame(self.post_dict).to_csv(os.path.join(self.config.SAVE_PATH, "post.csv"), index=False)
        pd.DataFrame(self.retweet_dict).to_csv(os.path.join(self.config.SAVE_PATH, "retweet.csv"), index=False)
        pd.DataFrame(self.like_dict).to_csv(os.path.join(self.config.SAVE_PATH, "like.csv"), index=False)
        
        LOG.info(f"Data saved at '{self.config.SAVE_PATH}'")


if __name__=='__main__':
    args = parse_args()
    config = OmegaConf.load(args.cfg)

    crawler = Crawler(config)
    crawler.crawl()
    crawler.save()
    LOG.info("Crawling finished!")