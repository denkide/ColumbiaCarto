import datetime  
import arcpy  
import sys  
import os
sys.path.append('./ccutil')

import GPRoutines


  

############### MAIN ###################################
########################################################
# User Params  
file_gdb = r"C:\ColumbiaCarto\work\GDB\Tillamook_911.gdb"
#arcpy.GetParameterAsText(0) # Path to FGDB  

table_name = r"Address_Point"
#arcpy.GetParameterAsText(1) # Table or feature class name  
  
arcpy.env.workspace = file_gdb  
  
if file_gdb.split(".")[-1] != "gdb":  
    arcpy.AddMessage("The input workspace is not a file geodatabase!")  
    sys.exit() 

# Use the ListFeatureClasses function to return a list of
#  shapefiles.
featureclasses = arcpy.ListFeatureClasses()

# Copy shapefiles to a file geodatabase
for fc in featureclasses:
    # Call GetModifiedDate function to get the number of seconds  
    num_seconds = GPRoutines.GetModifiedDate(file_gdb, arcpy.Describe(os.path.splitext(fc)[0]).baseName)  
    # Translate the number of seconds into a formatted date  
    date_modified = datetime.datetime.fromtimestamp(num_seconds).strftime('%Y-%m-%d %H:%M:%S') 
    print os.path.splitext(fc)[0] + " - " + date_modified
  
print "Done"

#print "Feature Class = " + os.path.splitext(fc)[0]
#lastModified = doIt(file_gdb, arcpy.Describe(table_name).baseName)








