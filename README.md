This Java-based application identifies and restores the original structure of an SQLite database after unintended relation-based modifications. It uses a detection algorithm to classify relationships between database tables, such as union and Cartesian product, and reconstructs the database by reverse-applying the detected relations to restore the original tables.

The program:  
Identifies which tables in the database were combined into a new table via union or Cartesian product.  
Restores the original tables by removing the extraneous table while preserving the detected source tables.  
Handles cases where no such relationships exist and outputs "NO MATCH" accordingly.  

Cloning the program:  
git clone https://github.com/Atukas77/SQL-DB-Structure-Recovery-Tool.git  
cd SQL-DB-Structure-Recovery-Tool 

Prerequisites:   
Java Development Kit (JDK) 8 or higher  
SQLite JDBC driver (sqlite-jdbc-3.47.0.0.jar provided inside the directory)  
SQLite3  

Running the application:  
The program is pre-compiled and ready to run. It receives a .db file as input and is executed from the terminal using the following command:  
java -cp ".:sqlite-jdbc-3.47.0.0.jar" Driver input.db output.db  
A sample input.db file is provided in the directory.  

To recompile the program, run:  
javac -cp ".:sqlite-jdbc-3.47.0.0.jar" *.java


