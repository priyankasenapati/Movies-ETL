# Movies-ETL
The Movies-ETL analysis merged data extracted from the Wikipedia Movies, Kaggle Metadata and Kaggle Ratings and saved it into a Postgres SQL database, using the ETL process to extract file data, clean, transform and join the data and load the dataset into SQL database. The following are the assumptions made in the process.

1. New updated data will continue to remain in the same format for the three data sources.
2. The titles of the columns of the converted DataFrames will remain the same. The data types also remain the same. This will enable merging of the tables.
3. To help the cleaning process,  all the box office, budget, release date, running time formats remain the same
4. Columns that have 90% of their values NULL should be dropped as a part of the data cleaning process.
5. Drop the rows that take a long time to clean up.
6. While writing the ratings to the ratings SQL table, drop and recreate the ratings table to reset the data in the table since the program adds data into the table in append mode.
7. Make sure that the try and except blocks have meaning full error messages, function call names and file path names so that it is easily understood to which file and function call the error is related to.
8. Sometimes you will need to google for solutions. I had to add a new module since my config import was failing to import my password. I had to add another module since there was another obscure error. Google is your best friend when you see bizarre errors.
9. Always restart and clear and run the program from the beginning. This simulates how a real program runs each time it is invoked.
10. Pay attention to the names, an ’s’ at the end etc, so that the merges go smoothly.
11. If some table merge, pivot table is not going well, open the source file and look at the column names and type of data. Then check if all the operations that are needed for the latest operation have been done correctly in the code. Try printing intermediate values to see if the data is getting transformed how the latest operation expects it to be.
12. Most importantly, keep your calm all through the challenge. The computer is dumb. It is doing what you are telling it to do. You have to fix it, no matter how frustrating it is sometimes. But at the end, when it works, it is all worth it.
