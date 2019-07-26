import datetime  
import arcpy  
import sys  
import os

#print ("AAAAA: " + os.getcwd())

#sys.path.append('D:\Tillamook_911\TillamookQA\Tools\Library')
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "\\Library")
import GPRoutines


#print("Hey!! --- " + arcpy.GetInstallInfo()['Version'])
#print("Hey!! --- " + arcpy.GetInstallInfo()['SourceDir'])
#print("Hey!! --- " + arcpy.GetInstallInfo()['InstallDir'])

############### MAIN ###################################
########################################################
# User Params  
gdb_path = "D:\Tillamook_911\TillamookQA\Tillamook_911.gdb"
output_path = "D:\Tillamook_911\TillamookQA\Output" #C:\ColumbiaCarto\deployment\Output"

#gdb_path = arcpy.GetParameterAsText(0) # Path to FGDB 
#output_path = arcpy.GetParameterAsText(1) # Path to output file  
 
#-----------------------------------------------
#gdb_name = arcpy.GetParameterAsText(1)
#file_gdb = gdb_path + "\\" + gdb_name
#table_name  = arcpy.GetParameterAsText(2) 
#output_file = output_path + "\\" + output_name`
##r"C:\ColumbiaCarto\work\GDB\Tillamook_911.gdb
#table_name = r"Address_Point"
#arcpy.GetParameterAsText(1) # Table or feature class name  
#----------------------------------------------- 

arcpy.env.workspace = gdb_path #file_gdb  
  
#if file_gdb.split(".")[-1] != "gdb":
if gdb_path.split(".")[-1] != "gdb":    
    arcpy.AddMessage("The input workspace is not a file geodatabase!")  
    sys.exit() 

# Use the ListFeatureClasses function to return a list of
#  shapefiles.
featureclasses = arcpy.ListFeatureClasses()

f = open(output_path + "\LastChanged.txt","w+")

f.write('{0:12}  {1:16}\n'.format('Date','Feature Class'))
f.write('------------------------------------------------------------\n\n')

#print(GPRoutines.GetLibPath())


# Copy shapefiles to a file geodatabase
for fc in featureclasses:
    # Call GetModifiedDate function to get the number of seconds  
    num_seconds = GPRoutines.GetModifiedDate(gdb_path, arcpy.Describe(os.path.splitext(fc)[0]).baseName)  
    # Translate the number of seconds into a formatted date  
    #date_modified = datetime.datetime.fromtimestamp(num_seconds).strftime('%Y-%m-%d %H:%M:%S')
    date_modified = datetime.datetime.fromtimestamp(num_seconds).strftime('%Y-%m-%d') 
    #f.write(os.path.splitext(fc)[0] + " \t" + date_modified + "\n")
    f.write('{0:12}  {1:16}\n'.format(date_modified,os.path.splitext(fc)[0]))

    #print os.path.splitext(fc)[0] + " \t " + date_modified
  
print "Done"
f.close()

#print "Feature Class = " + os.path.splitext(fc)[0]
#lastModified = doIt(file_gdb, arcpy.Describe(table_name).baseName)








