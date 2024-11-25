# AllsvenskanGamePredictor
ETL - project to analyze allsvenskan games
 
 
 
Step 1: Develop a web scraping method to collect data about the last three seasons of "Allsvenskan," including a wide range of information on match results and game statistics. I implemented this using a Jupyter Notebook named scraping.ipynb. After extracting the data, I saved it as a CSV file for further use.
 
![image](https://github.com/user-attachments/assets/c7941705-ecbc-4d7f-b533-7dd63ca65adc)
 
 
 
Step 2: Create a solution for transferring the data for later transformations. I set up a storage queue in Azure and used a script in VS Code to send my CSV file to the Azure storage queue. Once the data reaches Azure, I can use a notebook in Databricks to retrieve the data and load it into Databricks for further processing. Also then i can save that notebook and have it as a workflow so it will run once per week, since Allsvenskan is once per week.
 
 
Here we can see all data saved in a catalog in Databricks. 
![Sample data](https://github.com/user-attachments/assets/306934c8-f6f0-4c92-b27a-e5b5f477aabd)


Step 3: Clean up the dataset by removing columns with only null values and any unnecessary columns. Add new measures for future analysis:

1. Goal Difference: Calculated as scored goals minus goals conceded.
2. Team Form: Calculated as the median points from the last five games.
3. Points Column: Assigns points based on match results (win, loss, or draw).
I also added an ID column to make it easier to work with the data in Power BI later.

![Workspaces](https://github.com/user-attachments/assets/010a2ab6-a9bc-4577-8669-249631ebfb6a)

Step 4: This is my Power BI dashboard. The first one provides an overview with some statistics about the season. The second one offers a more detailed analysis, allowing you to view information about how two teams perform against each other.

Overview: 
![Power bi - overview](https://github.com/user-attachments/assets/c4402403-8213-4445-838e-7798cb67ca78)

More individual: 
![Power bi - dashboard individual](https://github.com/user-attachments/assets/5030b9bf-4df1-4a40-bc26-e38f122121f1)


