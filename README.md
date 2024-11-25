# AllsvenskanGamePredictor
ETL - project to analyze allsvenskan games
 
 
 
Step 1: Develop a web scraping method to collect data about the last three seasons of "Allsvenskan," including a wide range of information on match results and game statistics. I implemented this using a Jupyter Notebook named scraping.ipynb. After extracting the data, I saved it as a CSV file for further use.
 
![image](https://github.com/user-attachments/assets/c7941705-ecbc-4d7f-b533-7dd63ca65adc)
 
 
 
Step 2: Create a solution for transferring the data for later transformations. I set up a storage queue in Azure and used a script in VS Code to send my CSV file to the Azure storage queue. Once the data reaches Azure, I can use a notebook in Databricks to retrieve the data and load it into Databricks for further processing. Also then i can save that notebook and have it as a workflow so it will run once per week, since Allsvenskan is once per week.
 
 
Here we can see all data saved in a catalog in Databricks. 
![Sample data](https://github.com/user-attachments/assets/306934c8-f6f0-4c92-b27a-e5b5f477aabd)


  
