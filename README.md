Project Overview  
I am  building a complete data engineering pipeline using real Brazilian e-commerce data. 
By the end you will have a working pipeline that reads Kaggle CSV files, cleans and loads them into a Postgres database, 
transforms the data with dbt, tests everything with CI/CD on GitHub Actions, and presents a Tableau dashboard showing revenue, product performance, and delivery metrics.

What i am Building  
•	Ingestion — Python reads 9 Kaggle CSV files and loads them into the raw schema in Postgres  
•	Raw layer — Untouched source tables live in a raw schema — never modified directly  
•	Staging layer — dbt cleans, renames, and casts every raw table into consistent formats  
•	Mart layer — dbt builds three business-ready tables answering specific sales questions  
•	Dashboard — Tableau connects to your mart tables and displays sales KPIs  

Questions my Dashboard Will Answer  
•	What is monthly and quarterly revenue over time?  
•	Which product categories generate the most revenue?  
•	Which states have the most customers and highest order values?  
•	What is the average delivery time and how does it vary by state?  
•	What payment methods do customers prefer?  

Tech Stack and Project Structure  

<img width="293" height="264" alt="image" src="https://github.com/user-attachments/assets/9045a0e8-2f3d-4d45-bfcb-ac90542d40b1" />  
 
<img width="571" height="37" alt="image" src="https://github.com/user-attachments/assets/905f5650-aa28-482c-9078-1c777c291e94" />  

<img width="620" height="33" alt="image" src="https://github.com/user-attachments/assets/9ff21ac1-ec0c-4cae-80dc-a0c76335b570" />  

<img width="627" height="44" alt="image" src="https://github.com/user-attachments/assets/9850fb4f-0f52-42ea-bc58-1d6d56b43f80" />  






