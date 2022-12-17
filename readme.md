# YouTube Dataset of Different Countries ETL Project Using Spark and Hive

In this project, We will transform and transfer data and build Hive Data Warehouse to make analysis easier

## DataSets

###  YouTube Dataset of different countries:
### About Dataset
YouTube (the world-famous video sharing website) maintains a list of the top trending videos on the platform. According to Variety magazine, “To determine the year’s top-trending videos, YouTube uses a combination of factors including measuring users interactions (number of views, shares, comments and likes). Note that they’re not the most-viewed videos overall for the calendar year”. Top performers on the YouTube trending list are music videos (such as the famously virile “Gangam Style”), celebrity and/or reality TV performances, and the random dude-with-a-camera viral videos that YouTube is well-known for.

This dataset is a daily record of the top trending YouTube videos.

Note that this dataset is a structurally improved version of this dataset.

Content
This dataset includes several months (and counting) of data on daily trending YouTube videos. Data is included for the US, GB, DE, CA, and FR regions (USA, Great Britain, Germany, Canada, and France, respectively), with up to 200 listed trending videos per day.

EDIT: Now includes data from RU, MX, KR, JP and IN regions (Russia, Mexico, South Korea, Japan and India respectively) over the same time period.

Each region’s data is in a separate file. Data includes the video title, channel title, publish time, tags, views, likes and dislikes, description, and comment count.

The data also includes a category_id field, which varies between regions. To retrieve the categories for a specific video, find it in the associated JSON. One such file is included for each of the five regions in the dataset.

For more information on specific columns in the dataset refer to the column metadata.

Acknowledgements
This dataset was collected using the YouTube API.

Inspiration
Possible uses for this dataset could include:

Sentiment analysis in a variety of forms
Categorising YouTube videos based on their comments and statistics.
Training ML algorithms like RNNs to generate their own YouTube comments.
Analysing what factors affect how popular a YouTube video will be.
Statistical analysis over time.
For further inspiration, see the kernels on this [dataset](https://www.kaggle.com/datasets/singole/youtube-dataset-of-countries)!







## What You Need To Use Your Project

  01. Hadoop ( i use Hortonworks Sandbox HDP 2.6.5 ).
  02. Python  
  03. Spark
  04. Hive


## files and folders
#### 1 - etl.py -> Contain Proccess Data Functions

## How To Run Project
    1 - Download Data 
    2 - Copy Data To HDFS in Any Folder
    3 - Create Database In Hive Call `youtuob`
    4 - Run file in cmd `spark-submit etl.py`
    5 - Check Data From Hive



## Contact

## **Abdelrhman Yassein  :**  [LinkedIn](https://www.linkedin.com/in/Abdelrhman-Yassein/) - [GitHub](https://github.com/Abdelrhman-Yassein?tab=repositories)

