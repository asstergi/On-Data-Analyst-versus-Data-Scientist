# On Data Analyst versus Data Scientist

The full description of the analysis can be found on my LinkedIn account:
[INSERT POST LINK HERE]

To repeat the analysis, you have to crawl reddit.com and download the data:

1. Download the 'reddit.com_crawler.py' script
2. Open the command line
3. Go the folder where the script is saved and type `python reddit.com_crawler.py`
4. Combine the .csv files created typing `copy *.csv all_reddit_data.csv` in the command line

Then, assuming you have a Spark environment set up, you should run 'Spark analysis.py' script.
I used [Domino](https://www.dominodatalab.com/) that provides a free tier to run the analysis.
You'll need a bigger tier if you use much more data.

The result will be a file called 'PMIs for data_analyst & data_scientist.csv'.
You should hand-pick the words that make more sense to you and run the R script to create a plot similar to the one shown in the LinkedIn post.
