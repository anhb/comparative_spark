<p align="center">
  <img src="https://i1.wp.com/exitcondition.com/wp-content/uploads/2018/08/apache_spark_logo-e1550545881680.png" width="450px" height="200px" />
</p>

# Description:

This project allows you to compare two equal dataframes to get differences between them by column

# First Compilation

Make a clean install in the root directory of the project

# First Execution

In this example app we make a word-count.

Go to your IDE run configurations window and set the next configuration:
* Enable the maven profile `run-local`
* Set the next VM Option =>  `-Dspark.master=local[*]`
* Set the main class => `com.anhb.comparative.spark.AutoComparative`
* Set as program argument a valid path to a configuration file (not empty)
* Set environment variables INPUT_PATH and OUTPUT_PATH for process external files

