## Apache Spark Data Manipulation and Analysis

### Overview
_This repository showcases the results of the labs completed during my first semester at Igor Sikorsky Kyiv Polytechnic Institute, where I pursued a Master's degree in Informatics and Software Engineering🎓_ - ![Link of faculty](https://fiot.kpi.ua/)

This repository contains Python code that demonstrates data manipulation and analysis tasks using Apache Spark and PySpark library. The code works with a dataset related to crime rates in different regions and showcases various functionalities of Spark for big data processing.

### Part 1: Data Loading and Initial Analysis - First.ipynb

1. Data Loading and Schema Definition:
   - The code starts by installing the `pyspark` library and importing necessary modules.
   - It mounts Google Drive to access the dataset file named "Crime.csv".
   - Defines the schema for the dataset using the `StructType` and `StructField` classes to specify the data types for each column.

2. Creating a Spark Session and Loading Data:
   - A Spark session is created using `SparkSession.builder` to enable Spark functionalities.
   - The dataset is read from the CSV file and loaded into a DataFrame (`df`) using `spark.read.format("csv")`.

3. Task 1: Finding the Average Crime Rate in Southern States:
   - The code uses the `select` and `agg` functions to filter rows where the `Southern` column is greater than 0 (indicating a southern state) and calculates the average of the `CrimeRate` column.

4. Task 2: Calculating Total Police Expenditure:
   - The code uses the `groupBy` and `sum` functions to calculate the total sum of the `ExpenditureYear0` column.

5. Task 3: Filtering Data for High Crime Rates in Northern States with High Poverty:
   - The code uses the `select` function to filter rows where the `Southern` column is greater than 0 (indicating a northern state) and `BelowWage` column is greater than 200.
   - The filtered DataFrame is displayed.

6. Task 4: Sorting Data by Youth Unemployment:
   - The code uses the `sort` function to sort the DataFrame by the `YouthUnemployment` column in ascending order.

7. Task 5: Sorting Data by Wage in Descending Order:
   - The code uses the `sort` function to sort the DataFrame by the `Wage` column in descending order.

### Part 2: Extended Data Manipulation and Analysis - Second.ipynb

8. Task 1: Create a New Table with Expenditure on Police and Array of Males Count:
   - The code creates a new table that contains two columns: "ExpenditureYear0" representing police expenditure and "Males," an array of the count of males per 1000 population corresponding to each expenditure frequency. This is achieved using the `collect_set` function to gather unique values of "Males" for each "ExpenditureYear0" group.

9. Task 2: Transforming Second Column into Multiple Columns:
   - The code transforms the second column of the table from Task 1 into several separate columns. This is done using the `select` function and accessing each element of the "Males" array by index (e.g., `df2.Males[0], df2.Males[1], df2.Males[2]`). The result displays the "ExpenditureYear0" and corresponding "Males" values in different columns.

10. Task 3: Finding the Average Crime Rate for Combinations of "Southern" and "HighYouthUnemploy":
   - The code finds the average crime rate for all possible combinations of "Southern" and "HighYouthUnemploy" parameters. It groups the DataFrame based on these two columns and calculates the average crime rate using the `collect_set` function.

11. Task 4: Adding a New Column with the Difference between Average and Current Police Expenditure:
   - The code adds a new column named "ExpenditureYear0_avg-ExpenditureYear0," which represents the difference between the average police expenditure and the current "ExpenditureYear0" value, depending on whether the state is southern or not. It calculates the average expenditure for both southern and non-southern states and then applies the difference calculation accordingly. The resulting DataFrame is saved to a new CSV file named "Crime2.csv."

