# mlb-statcast-ml
Build a MLB StatCast database pipeline for analysis and machine learning prediction

## **Project Objective**
The general goal of this project is to enable a streamlined way to create a personal and customized database conatining the avaiable MLB Statcast data from 
Baseball Savant using a combination of python programming and SQL. Once the database is created, the project will be continually updated with 
scripts that explore how the Statcast data can be used for analytical insights and machine learning predctions. This will be done through a combination of python scripting, SQL queries, and Tableu vizzies. 


### Creating the Database
The first step in this project is to create a customized database containing the MLB Statcast data available from Baseball Savant, which has the PitchFX and Statcast data from 2008-Present. The new-age Statcast data started being recorded in 2015, with even more advanced data starting in 2020; more information about Statcast data can be found [here](http://m.mlb.com/glossary/statcast/#:~:text=Statcast%20is%20a%20state%2Dof,never%20possible%20in%20the%20past.&text=The%20Hawk%2DEye%20Statcast%20system,pitch%2C%20hit%20and%20player%20tracking.) and [here](https://en.wikipedia.org/wiki/Statcast).      
The current project state supports building a database on either **Microsoft SQL Sever** or **MySQL**, wich are considered professional quality RDMS solutions. In future iterations of the project as the data grows and experiments involving machine learning are created, added support for different types of databases could be added. A quick general overview of the steps to create the database and then create a data stream pipeline for the DB to be updated in-season are as follows:  

**[Run the initial run_build.py script]** --> **[GUI pops up to enter personal DB info]** --> **[RAW and WRK(organized/transformed data) tables will be created and filled in 'statcast' DB]** --> **[create scheduled task to run run_stream.py]**  
As it is prudent to keep the RAW data in tact, there will be RAW tables created for each year from 2008-(previously created season) with the 'statcast' DB, and then also WRK tables for each year from 2008-(previously created season) that contains orgranized, transformed, and added data that is ready for working analysis and prediction. Detailed information on what the WRK tables contain can be found in the methods that create the transformations within the *mlb_statcast.py* script. 

**_NOTE_**: There needs to be a databse within the RDMS used with the name of **'statcast'**

To begin, all files from the **/csv**, **/dicts**, and **/lists** folders will be needed on the local machine. From the **/scripts** folder; the *config.yaml*, *run_build.py*, and *mlb_statcast.py* files will be needed. Explaination of these folders and scripts are as follows:  
- **/csv**: conatains .csv file that holds relevant information for building the database for each MLB team from 2008-Present.
- **/dicts**: contains .txt files written in JSON format containing all dictionaries that used for creating aditional infomation to increased level of detail to the RAW Statcast data. The scripts that were used to create the dictionaries are found [here](https://github.com/benjaminmielke/mlb-statcast-ml/blob/main/scripts/create_bop_pos_dcts.py). A quick overview: the scripts pull data from Baseball Reference website to add what position in batter order and what field position the batter was in for each pitch. Pretty awesome!
- **/lists**: contains .txt files holding lists that are used to organize the RAW data into an organized state. 
- *config.yaml*: this is the configuration file to have setup to have a folder path saved that is used for the project and also datase information for when in-season streaming data is active. 
- *run_build.py*: script to run the databse builder GUI
- *mlb_statcast.py*: main script that holds Classes and Methods to build, stream, analyze, and predict with the MLB Statcast data. 

**_NOTE_**: There needs to be a databse within the RDMS used with the name of **'statcast'**

To build your very own MLB Statcast databse its easy as 1,2,3:  
    
    Step 1: download all neccessary folders/files (see above)
    Step 2: edit config.yaml file
    Step 3: excute run_build.py
    
You now have your very own MLB Statcast database with endless things to do with!
