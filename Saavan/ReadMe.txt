Overall Flow:

Implemented Mapper , partitioner and reducer class as part of project com.saavn:

a) Mapper : got SongId and Day as Key value pair <SongId,Day>
b) Partitioner: To partition the data based on day so that it's
   easier to work with output file for a given day as per requirements
c) Reducers: Reducer was implemented to aggregate the count per songId.
   I have also excluded the songId's which have count less than 200 to
   make the code a little bit efficient as it will reduce the communication cost.
 

Obtained all the raw files from above MapR program by running saavn.jar with below command:

hadoop jar saavn.jar com.saavn.LogDriver s3a://mapreduce-project-bde/part-00000 s3a://saavanprojectnavjot/output1/

a) Output contained 31 files based on count of songs played per day.

b) Copied all the files from S3 folder(s3a://saavanprojectnavjot/output1/) to local folder
c) Ran the count.java by entering input parameter as raw file obtained from MapR and output
   parameter as output file naming it the day.txt for which we calculated trending pattern
   E.g. for generating trending data for 31st December:
   input file -- part-r-00030.dms
   output file -- 31.txt



