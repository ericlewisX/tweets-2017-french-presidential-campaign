---
# Tweets on the 2017 French Presidential Campaign EDA







by: Jordan Fairbanks, Vamsidhar Vurimindi, Samuel Cespedes, Eric H. Lewis

## Table of Content 

* Line Graph

* Confidence Interval

* The data

## Objective

<img src="img/mlpvsem" width="350"/>


* What Happened? 

* Actual Results


## Data

The data that is being used is from a dataset that includes all of the tweets from France during the 2017 election. 

* 42,855 tweets (sample)
* Twitter-API
---
* What do we want..


## Exploratory Data Analysis

<img src="img/macronVlepin.png" width="350"/>


```python
ci_(mac_list)
```

lowerbound: 733.8739740301321

upperbound: 734.8990515197613 


```python
ci_(lep_list)
```

lowerbound: 605.5764156151093

upperbound: 607.0618443364211 


```python
import pyspark as ps
spark = ps.sql.SparkSession.builder.master("local[4]").appName("df case study").config("spark.sql.caseSensitive", "true").getOrCreate()
tweets_df = spark.read.json('data/french_tweets.json').sample(False, 0.2)

sc = spark.sparkContext
```


```python
tweets_df.printSchema()
```

    root
     |-- contributors: string (nullable = true)
     |-- coordinates: struct (nullable = true)
     |    |-- coordinates: array (nullable = true)
     |    |    |-- element: double (containsNull = true)
     |    |-- type: string (nullable = true)
     |-- created_at: string (nullable = true)
     |-- display_text_range: array (nullable = true)
     |    |-- element: long (containsNull = true)
     |-- entities: struct (nullable = true)
     |    |-- hashtags: array (nullable = true)
     |    |    |-- element: struct (containsNull = true)
     |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |-- text: string (nullable = true)
     |    |-- media: array (nullable = true)
     |    |    |-- element: struct (containsNull = true)
     |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |-- id: long (nullable = true)
     |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |-- source_user_id: long (nullable = true)
     |    |    |    |-- source_user_id_str: string (nullable = true)
     |    |    |    |-- type: string (nullable = true)
     |    |    |    |-- url: string (nullable = true)
     |    |-- symbols: array (nullable = true)
     |    |    |-- element: struct (containsNull = true)
     |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |-- text: string (nullable = true)
     |    |-- urls: array (nullable = true)
     |    |    |-- element: struct (containsNull = true)
     |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |-- url: string (nullable = true)
     |    |-- user_mentions: array (nullable = true)
     |    |    |-- element: struct (containsNull = true)
     |    |    |    |-- id: long (nullable = true)
     |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |-- name: string (nullable = true)
     |    |    |    |-- screen_name: string (nullable = true)
     |-- extended_entities: struct (nullable = true)
     |    |-- media: array (nullable = true)
     |    |    |-- element: struct (containsNull = true)
     |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |-- id: long (nullable = true)
     |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |-- source_user_id: long (nullable = true)
     |    |    |    |-- source_user_id_str: string (nullable = true)
     |    |    |    |-- type: string (nullable = true)
     |    |    |    |-- url: string (nullable = true)
     |    |    |    |-- video_info: struct (nullable = true)
     |    |    |    |    |-- aspect_ratio: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- duration_millis: long (nullable = true)
     |    |    |    |    |-- variants: array (nullable = true)
     |    |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |    |-- bitrate: long (nullable = true)
     |    |    |    |    |    |    |-- content_type: string (nullable = true)
     |    |    |    |    |    |    |-- url: string (nullable = true)
     |-- extended_tweet: struct (nullable = true)
     |    |-- display_text_range: array (nullable = true)
     |    |    |-- element: long (containsNull = true)
     |    |-- entities: struct (nullable = true)
     |    |    |-- hashtags: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- text: string (nullable = true)
     |    |    |-- media: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |    |-- source_user_id: long (nullable = true)
     |    |    |    |    |-- source_user_id_str: string (nullable = true)
     |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |    |-- video_info: struct (nullable = true)
     |    |    |    |    |    |-- aspect_ratio: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- duration_millis: long (nullable = true)
     |    |    |    |    |    |-- variants: array (nullable = true)
     |    |    |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |    |    |-- bitrate: long (nullable = true)
     |    |    |    |    |    |    |    |-- content_type: string (nullable = true)
     |    |    |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |-- symbols: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- text: string (nullable = true)
     |    |    |-- urls: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- url: string (nullable = true)
     |    |    |-- user_mentions: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- name: string (nullable = true)
     |    |    |    |    |-- screen_name: string (nullable = true)
     |    |-- extended_entities: struct (nullable = true)
     |    |    |-- media: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |    |-- source_user_id: long (nullable = true)
     |    |    |    |    |-- source_user_id_str: string (nullable = true)
     |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |    |-- video_info: struct (nullable = true)
     |    |    |    |    |    |-- aspect_ratio: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- duration_millis: long (nullable = true)
     |    |    |    |    |    |-- variants: array (nullable = true)
     |    |    |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |    |    |-- bitrate: long (nullable = true)
     |    |    |    |    |    |    |    |-- content_type: string (nullable = true)
     |    |    |    |    |    |    |    |-- url: string (nullable = true)
     |    |-- full_text: string (nullable = true)
     |-- favorite_count: long (nullable = true)
     |-- favorited: boolean (nullable = true)
     |-- filter_level: string (nullable = true)
     |-- geo: struct (nullable = true)
     |    |-- coordinates: array (nullable = true)
     |    |    |-- element: double (containsNull = true)
     |    |-- type: string (nullable = true)
     |-- id: long (nullable = true)
     |-- id_str: string (nullable = true)
     |-- in_reply_to_screen_name: string (nullable = true)
     |-- in_reply_to_status_id: long (nullable = true)
     |-- in_reply_to_status_id_str: string (nullable = true)
     |-- in_reply_to_user_id: long (nullable = true)
     |-- in_reply_to_user_id_str: string (nullable = true)
     |-- is_quote_status: boolean (nullable = true)
     |-- lang: string (nullable = true)
     |-- limit: struct (nullable = true)
     |    |-- timestamp_ms: string (nullable = true)
     |    |-- track: long (nullable = true)
     |-- place: struct (nullable = true)
     |    |-- bounding_box: struct (nullable = true)
     |    |    |-- coordinates: array (nullable = true)
     |    |    |    |-- element: array (containsNull = true)
     |    |    |    |    |-- element: array (containsNull = true)
     |    |    |    |    |    |-- element: double (containsNull = true)
     |    |    |-- type: string (nullable = true)
     |    |-- country: string (nullable = true)
     |    |-- country_code: string (nullable = true)
     |    |-- full_name: string (nullable = true)
     |    |-- id: string (nullable = true)
     |    |-- name: string (nullable = true)
     |    |-- place_type: string (nullable = true)
     |    |-- url: string (nullable = true)
     |-- possibly_sensitive: boolean (nullable = true)
     |-- quoted_status: struct (nullable = true)
     |    |-- contributors: string (nullable = true)
     |    |-- coordinates: struct (nullable = true)
     |    |    |-- coordinates: array (nullable = true)
     |    |    |    |-- element: double (containsNull = true)
     |    |    |-- type: string (nullable = true)
     |    |-- created_at: string (nullable = true)
     |    |-- display_text_range: array (nullable = true)
     |    |    |-- element: long (containsNull = true)
     |    |-- entities: struct (nullable = true)
     |    |    |-- hashtags: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- text: string (nullable = true)
     |    |    |-- media: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |    |-- source_user_id: long (nullable = true)
     |    |    |    |    |-- source_user_id_str: string (nullable = true)
     |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |-- url: string (nullable = true)
     |    |    |-- symbols: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- text: string (nullable = true)
     |    |    |-- urls: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- url: string (nullable = true)
     |    |    |-- user_mentions: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- name: string (nullable = true)
     |    |    |    |    |-- screen_name: string (nullable = true)
     |    |-- extended_entities: struct (nullable = true)
     |    |    |-- media: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |    |-- source_user_id: long (nullable = true)
     |    |    |    |    |-- source_user_id_str: string (nullable = true)
     |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |    |-- video_info: struct (nullable = true)
     |    |    |    |    |    |-- aspect_ratio: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- duration_millis: long (nullable = true)
     |    |    |    |    |    |-- variants: array (nullable = true)
     |    |    |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |    |    |-- bitrate: long (nullable = true)
     |    |    |    |    |    |    |    |-- content_type: string (nullable = true)
     |    |    |    |    |    |    |    |-- url: string (nullable = true)
     |    |-- extended_tweet: struct (nullable = true)
     |    |    |-- display_text_range: array (nullable = true)
     |    |    |    |-- element: long (containsNull = true)
     |    |    |-- entities: struct (nullable = true)
     |    |    |    |-- hashtags: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- text: string (nullable = true)
     |    |    |    |-- media: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |    |    |-- source_user_id: long (nullable = true)
     |    |    |    |    |    |-- source_user_id_str: string (nullable = true)
     |    |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |    |    |-- video_info: struct (nullable = true)
     |    |    |    |    |    |    |-- aspect_ratio: array (nullable = true)
     |    |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |    |-- duration_millis: long (nullable = true)
     |    |    |    |    |    |    |-- variants: array (nullable = true)
     |    |    |    |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |    |    |    |-- bitrate: long (nullable = true)
     |    |    |    |    |    |    |    |    |-- content_type: string (nullable = true)
     |    |    |    |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |-- symbols: array (nullable = true)
     |    |    |    |    |-- element: string (containsNull = true)
     |    |    |    |-- urls: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |-- user_mentions: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- name: string (nullable = true)
     |    |    |    |    |    |-- screen_name: string (nullable = true)
     |    |    |-- extended_entities: struct (nullable = true)
     |    |    |    |-- media: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |    |    |-- source_user_id: long (nullable = true)
     |    |    |    |    |    |-- source_user_id_str: string (nullable = true)
     |    |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |    |    |-- video_info: struct (nullable = true)
     |    |    |    |    |    |    |-- aspect_ratio: array (nullable = true)
     |    |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |    |-- duration_millis: long (nullable = true)
     |    |    |    |    |    |    |-- variants: array (nullable = true)
     |    |    |    |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |    |    |    |-- bitrate: long (nullable = true)
     |    |    |    |    |    |    |    |    |-- content_type: string (nullable = true)
     |    |    |    |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |-- full_text: string (nullable = true)
     |    |-- favorite_count: long (nullable = true)
     |    |-- favorited: boolean (nullable = true)
     |    |-- filter_level: string (nullable = true)
     |    |-- geo: struct (nullable = true)
     |    |    |-- coordinates: array (nullable = true)
     |    |    |    |-- element: double (containsNull = true)
     |    |    |-- type: string (nullable = true)
     |    |-- id: long (nullable = true)
     |    |-- id_str: string (nullable = true)
     |    |-- in_reply_to_screen_name: string (nullable = true)
     |    |-- in_reply_to_status_id: long (nullable = true)
     |    |-- in_reply_to_status_id_str: string (nullable = true)
     |    |-- in_reply_to_user_id: long (nullable = true)
     |    |-- in_reply_to_user_id_str: string (nullable = true)
     |    |-- is_quote_status: boolean (nullable = true)
     |    |-- lang: string (nullable = true)
     |    |-- place: struct (nullable = true)
     |    |    |-- bounding_box: struct (nullable = true)
     |    |    |    |-- coordinates: array (nullable = true)
     |    |    |    |    |-- element: array (containsNull = true)
     |    |    |    |    |    |-- element: array (containsNull = true)
     |    |    |    |    |    |    |-- element: double (containsNull = true)
     |    |    |    |-- type: string (nullable = true)
     |    |    |-- country: string (nullable = true)
     |    |    |-- country_code: string (nullable = true)
     |    |    |-- full_name: string (nullable = true)
     |    |    |-- id: string (nullable = true)
     |    |    |-- name: string (nullable = true)
     |    |    |-- place_type: string (nullable = true)
     |    |    |-- url: string (nullable = true)
     |    |-- possibly_sensitive: boolean (nullable = true)
     |    |-- quoted_status_id: long (nullable = true)
     |    |-- quoted_status_id_str: string (nullable = true)
     |    |-- retweet_count: long (nullable = true)
     |    |-- retweeted: boolean (nullable = true)
     |    |-- scopes: struct (nullable = true)
     |    |    |-- followers: boolean (nullable = true)
     |    |-- source: string (nullable = true)
     |    |-- text: string (nullable = true)
     |    |-- truncated: boolean (nullable = true)
     |    |-- user: struct (nullable = true)
     |    |    |-- contributors_enabled: boolean (nullable = true)
     |    |    |-- created_at: string (nullable = true)
     |    |    |-- default_profile: boolean (nullable = true)
     |    |    |-- default_profile_image: boolean (nullable = true)
     |    |    |-- description: string (nullable = true)
     |    |    |-- favourites_count: long (nullable = true)
     |    |    |-- follow_request_sent: string (nullable = true)
     |    |    |-- followers_count: long (nullable = true)
     |    |    |-- following: string (nullable = true)
     |    |    |-- friends_count: long (nullable = true)
     |    |    |-- geo_enabled: boolean (nullable = true)
     |    |    |-- id: long (nullable = true)
     |    |    |-- id_str: string (nullable = true)
     |    |    |-- is_translator: boolean (nullable = true)
     |    |    |-- lang: string (nullable = true)
     |    |    |-- listed_count: long (nullable = true)
     |    |    |-- location: string (nullable = true)
     |    |    |-- name: string (nullable = true)
     |    |    |-- notifications: string (nullable = true)
     |    |    |-- profile_background_color: string (nullable = true)
     |    |    |-- profile_background_image_url: string (nullable = true)
     |    |    |-- profile_background_image_url_https: string (nullable = true)
     |    |    |-- profile_background_tile: boolean (nullable = true)
     |    |    |-- profile_banner_url: string (nullable = true)
     |    |    |-- profile_image_url: string (nullable = true)
     |    |    |-- profile_image_url_https: string (nullable = true)
     |    |    |-- profile_link_color: string (nullable = true)
     |    |    |-- profile_sidebar_border_color: string (nullable = true)
     |    |    |-- profile_sidebar_fill_color: string (nullable = true)
     |    |    |-- profile_text_color: string (nullable = true)
     |    |    |-- profile_use_background_image: boolean (nullable = true)
     |    |    |-- protected: boolean (nullable = true)
     |    |    |-- screen_name: string (nullable = true)
     |    |    |-- statuses_count: long (nullable = true)
     |    |    |-- time_zone: string (nullable = true)
     |    |    |-- url: string (nullable = true)
     |    |    |-- utc_offset: long (nullable = true)
     |    |    |-- verified: boolean (nullable = true)
     |-- quoted_status_id: long (nullable = true)
     |-- quoted_status_id_str: string (nullable = true)
     |-- retweet_count: long (nullable = true)
     |-- retweeted: boolean (nullable = true)
     |-- source: string (nullable = true)
     |-- text: string (nullable = true)
     |-- timestamp_ms: string (nullable = true)
     |-- truncated: boolean (nullable = true)
     |-- user: struct (nullable = true)
     |    |-- contributors_enabled: boolean (nullable = true)
     |    |-- created_at: string (nullable = true)
     |    |-- default_profile: boolean (nullable = true)
     |    |-- default_profile_image: boolean (nullable = true)
     |    |-- description: string (nullable = true)
     |    |-- favourites_count: long (nullable = true)
     |    |-- follow_request_sent: string (nullable = true)
     |    |-- followers_count: long (nullable = true)
     |    |-- following: string (nullable = true)
     |    |-- friends_count: long (nullable = true)
     |    |-- geo_enabled: boolean (nullable = true)
     |    |-- id: long (nullable = true)
     |    |-- id_str: string (nullable = true)
     |    |-- is_translator: boolean (nullable = true)
     |    |-- lang: string (nullable = true)
     |    |-- listed_count: long (nullable = true)
     |    |-- location: string (nullable = true)
     |    |-- name: string (nullable = true)
     |    |-- notifications: string (nullable = true)
     |    |-- profile_background_color: string (nullable = true)
     |    |-- profile_background_image_url: string (nullable = true)
     |    |-- profile_background_image_url_https: string (nullable = true)
     |    |-- profile_background_tile: boolean (nullable = true)
     |    |-- profile_banner_url: string (nullable = true)
     |    |-- profile_image_url: string (nullable = true)
     |    |-- profile_image_url_https: string (nullable = true)
     |    |-- profile_link_color: string (nullable = true)
     |    |-- profile_sidebar_border_color: string (nullable = true)
     |    |-- profile_sidebar_fill_color: string (nullable = true)
     |    |-- profile_text_color: string (nullable = true)
     |    |-- profile_use_background_image: boolean (nullable = true)
     |    |-- protected: boolean (nullable = true)
     |    |-- screen_name: string (nullable = true)
     |    |-- statuses_count: long (nullable = true)
     |    |-- time_zone: string (nullable = true)
     |    |-- url: string (nullable = true)
     |    |-- utc_offset: long (nullable = true)
     |    |-- verified: boolean (nullable = true)
    



```python
tweets_df.createOrReplaceTempView('french_tweets')
```

### SQL Select statement


```python
result = spark.sql('''SELECT quoted_status.`retweet_count`, 
                        quoted_status.`entities`.`user_mentions` as mentions, 
                        quoted_status.`entities`.`hashtags`.`text` as hashtags
                       FROM french_tweets
                       WHERE quoted_status.`entities`.`hashtags` IS NOT null
                       AND cardinality(quoted_status.`entities`.`hashtags`) >0
                       AND quoted_status.`retweet_count`> 0 ;''')
result.show(30)
```

    +-------------+--------------------+--------------------+
    |retweet_count|            mentions|            hashtags|
    +-------------+--------------------+--------------------+
    |           71|                  []| [Macron, Whirlpool]|
    |           21|                  []|[Presidentielle20...|
    |           14|                  []|         [whirlpool]|
    |          434|                  []| [Macron, Whirlpool]|
    |          936|[{181561712, 1815...|[HarryStylesChezC...|
    |           24|[{133663801, 1336...|             [Lepen]|
    |         3217|                  []|[Chirac, Presiden...|
    |           21|                  []|      [daddydaycare]|
    |            7|[{33893706, 33893...|            [TeamOL]|
    |         1244|                  []|             [EXCLU]|
    |            4|                  []|                [FN]|
    |           15|                  []|    [Medias, Macron]|
    |            1|                  []|         [whirlpool]|
    |           64|[{801822323825410...|          [Whirpool]|
    |           33|                  []|         [whirlpool]|
    |          238|                  []|          [Whirpool]|
    |           37|                  []|         [Whirlpool]|
    |          438|                  []|             [bgc17]|
    |          293|                  []| [Macron, Whirlpool]|
    |           28|                  []|[Whirlpool, Macro...|
    |          363|                  []|         [whirlpool]|
    |          107|                  []|     [BangHai, Drum]|
    |            1|                  []|         [Whirlpool]|
    |          158|                  []|              [LMEF]|
    |          283|[{217749896, 2177...|[Whirlpool, JeVot...|
    |           91|[{157153018, 1571...|           [LÉGENDE]|
    |         1437|                  []|            [Europe]|
    |           98|                  []|[Messi, Messi500,...|
    |         2818|[{87171878, 87171...|[Quotidien, Harry...|
    |           13|[{20193041, 20193...|            [JeVote]|
    +-------------+--------------------+--------------------+
    only showing top 30 rows
    


@Em and @MLP


```python
results_em = spark.sql('''SELECT
                    text, quoted_status.`retweet_count` 
                    FROM french_tweets 
                    WHERE quoted_status.`retweet_count` IS NOT null 
                    and quoted_status.`retweet_count` > 0 and text LIKE '%@EmmanuelMacron%'
                    ''')
results_em.show()
```

    +--------------------+-------------+
    |                text|retweet_count|
    +--------------------+-------------+
    |Qui s'occupe de l...|          434|
    |@MLP_officiel apr...|          283|
    |OK. Mais sachant ...|            3|
    |@BFMTV @Le_Figaro...|           66|
    |vous me débectez ...|          846|
    |Salut @BFMTV elle...|          230|
    |@EmmanuelMacron l...|         1953|
    |Hombre @EmmanuelM...|          124|
    |Mddrrrr ça y est ...|        10648|
    |@EmmanuelMacron P...|            2|
    |@jmaphatie consei...|           22|
    |.@EmmanuelMacron ...|           41|
    |.@TPMP 
    @Emmanuel...|          726|
    |Pitoyable @Emmanu...|            3|
    |@EmmanuelMacron :...|         1417|
    |Non. Et faut qu'o...|          350|
    |@EmmanuelMacron @...|            1|
    |Et dire que certa...|            1|
    |@EmmanuelMacron  ...|           23|
    |... La vérité sur...|            9|
    +--------------------+-------------+
    only showing top 20 rows
    



```python
results_mlp = spark.sql('''SELECT
                    text, quoted_status.`retweet_count` 
                    FROM french_tweets 
                    WHERE quoted_status.`retweet_count` IS NOT null 
                    and quoted_status.`retweet_count` > 0 and text LIKE '%@MLP_officiel%'
                    ''')
results_mlp.show()

```

    +--------------------+-------------+
    |                text|retweet_count|
    +--------------------+-------------+
    |. @Valeurs fait o...|          113|
    |@MLP_officiel apr...|          283|
    |Délires de @MLP_o...|            5|
    |On voit la différ...|          284|
    |On ne veut pas de...|            1|
    |Face à @MLP_offic...|            1|
    |.@EmmanuelMacron ...|           41|
    |Ce ne serait pas ...|           16|
    |#Casher #halal in...|         4269|
    |Coucou @MLP_offic...|            2|
    |@YKellran @MajorC...|         8926|
    |#2emeTour ralliem...|           61|
    |The tweet with th...|          344|
    |.@fhollande a fai...|          228|
    |Aucun étonnement ...|          112|
    |T'es sérieux là ?...|          129|
    +--------------------+-------------+
    


### Helper Functions


```python
import scipy.stats as stats
import matplotlib.pyplot as plt
import numpy as np 
def get_column_vals(query, column_name):
    """ 
    get_retweet_counts gets the values from a spark dataframe and returns them as a list
        inputs:
        query 
        - sql query
        tableview 
        - spark tableView
        column_name
        - column name to return as a list
        returns:
        list of values
    """
    rows = spark.sql(query).collect()
    final_list = []
    for i in rows:
        final_list.append(i[column_name])
    return final_list
def make_distribution_and_graph(value_lst, ax, label):
    """
        make_distribution_and_graph takes a list and makes a normal distribution, takes a sample
        from that distribution, graphs it, and saves the graph.
        inputs:
        value_lst
        returns:
            None
    """
    dist = stats.norm(loc=np.mean(value_lst), scale=np.std(value_lst)/len(value_lst)**.5)
    x = np.linspace(0,int(dist.ppf(.9999999)),250)
    y = dist.pdf(x)
    return ax.plot(x, y, label=label)

def ci_(vals):
    dist = stats.norm(loc=np.mean(vals), scale=np.std(vals)/len(vals)**.5)
    means = []
    for i in range(10000):
        means.append(np.mean(dist.rvs(100)))
    mean_dist = stats.norm(loc=np.mean(means),scale=np.std(means)/len(means)**.5 )
    print(f"lowerbound: {mean_dist.ppf(.025)}")
    print(f"upperbound: {mean_dist.ppf(.975)} ")
    return 
```


```python
macron_query = '''SELECT
                    text, entities.`hashtags` as hashtags, quoted_status.`retweet_count` 
                    FROM french_tweets 
                    WHERE quoted_status.`retweet_count` IS NOT null and quoted_status.`retweet_count` > 0 and
                    text LIKE '%@EmmanuelMacron%'
                    '''
lepin_query = '''SELECT
                    text, entities.`hashtags` as hashtags, quoted_status.`retweet_count` 
                    FROM french_tweets 
                    WHERE quoted_status.`retweet_count` IS NOT null and quoted_status.`retweet_count` > 0 and
                    text LIKE '%@MLP_officiel%'
                    '''

mac_list = get_column_vals(macron_query, 'retweet_count')
lep_list = get_column_vals(lepin_query, 'retweet_count')
fig, ax = plt.subplots()
make_distribution_and_graph(mac_list, ax, f'macron \n tweets = {len(mac_list)} ')
make_distribution_and_graph(lep_list, ax, f'lepin \n tweets = {len(lep_list)} ')
ax.set_xlabel('Re-tweets')
ax.set_ylabel('Probability')
ax.legend()
plt.show()
```

## Conclusion

<img src="img/winner.jpg" width="350"/>

# The End
