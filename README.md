# ga-python-utils
Repo used for sharing. I'm learning python so no judging :)

Apparently GA has a cap for 1M rows for each getBatch request. Although the total amount of rows is a random number between 900k-1M. Mind that the response will come saying that the data hasn't been sampled. For instance, I got 999.435 rows for May (1-31) and got 999.320 rows for May (1-10). I observed the same throughout several other months. So I've tweaked around to adjust the requested dates accordingly to amount of rows. All you need to do is fill in the months that you want to extract data, which view you want to see and the dimensions/metrics.

exportGAEvents.py

Get events data of desired months and outputs to a CSV.

exportGAPageViews.py

Get pageviews data of desired months and outputs to a CSV.
  
  Feedbacks are very welcome!
