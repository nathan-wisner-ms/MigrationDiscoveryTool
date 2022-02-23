# Instructions

* Fill out ```config.ini``` with information about the database. You can get all the data from the azure portal and look in the Connection String for psql
* Using python, either in a terminal using ```python main.py``` or ```python3 main.py```
* After this, 3 folders should be created, and their files will be in the format of {DBNAME}{FILETYPE}. E.g: pgValidTables.csv for the database named pg
	* Errors
	  * This table will show you all errors that come up during the discovery process. These can include incompatable extensions, incompatable data types, tables with no primary keys, etc
	* ValidTables
	  * This shows the list of all tables that are valid along with thier sizing 
	* Analysis
	  * Analysis shows the detailed information about each database, their name, if they are missing primary keys, the table size, if there are collations and the schema results