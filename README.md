# Allsvenskan_Game_Analysis

In this project, I will show the tools I used step by step to first web scrape a CSV file containing data from the last three seasons of Allsvenskan, transform it, and then visualize it to make interesting comparisons between different teams. By analyzing historical data, the goal is to draw valuable conclusions that could, for example, help with betting predictions. Here is an overview of the tools I will be using.

![flow](https://github.com/user-attachments/assets/6492908f-32b1-4082-9bf7-5be2d22b5c9b)

 
 
 
Step 1: Develop a web scraping method to collect data about the last three seasons of "Allsvenskan," including a wide range of information on match results and game statistics. I implemented this using a Jupyter Notebook named scraping.ipynb. After extracting the data, I saved it as a CSV file for further use.
 
![image](https://github.com/user-attachments/assets/c7941705-ecbc-4d7f-b533-7dd63ca65adc)
 
 
 
Step 2: Create a solution for transferring the data for later transformations. I set up a storage queue in Azure and used a script in VS Code to send my CSV file to the Azure storage queue. Once the data reaches Azure, I can use a notebook in Databricks to retrieve the data and load it into Databricks for further processing. Also then i can save that notebook and have it as a workflow so it will run once per week, since Allsvenskan is once per week.
 
Here we can see the notebook that collects the data from Azure storage queue:
![image](https://github.com/user-attachments/assets/07e07959-9955-4492-8d02-930e60a12552)

 
Here we can see all data saved in a catalog in Databricks: 
![Sample data](https://github.com/user-attachments/assets/306934c8-f6f0-4c92-b27a-e5b5f477aabd)


Step 3: Clean up the dataset by removing columns with only null values and any unnecessary columns. Add new measures for future analysis:

1. Goal Difference: Calculated as scored goals minus goals conceded.
2. Team Form: Calculated as the median points from the last five games.
3. Points Column: Assigns points based on match results (win, loss, or draw).
I also added an ID column to make it easier to work with the data in Power BI later.

![Workspaces](https://github.com/user-attachments/assets/010a2ab6-a9bc-4577-8669-249631ebfb6a)

One of all transformations:
![image](https://github.com/user-attachments/assets/f4887a15-951f-4d89-874e-463912c6e697)


Step 4: This is my Power BI dashboard. The first one provides an overview with some statistics about the season. The second one offers a more detailed analysis, allowing you to view information about how two teams perform against each other.

Overview: 
![power bi - överblick](https://github.com/user-attachments/assets/04690a0f-31b7-44ed-aaf2-713c08146333)


More individual: 
![power bi - individuell jämförelse](https://github.com/user-attachments/assets/0cc17e3f-a235-4c4a-b7ea-3f24ff0f0c4d)



