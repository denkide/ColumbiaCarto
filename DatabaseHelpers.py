import arcpy
from arcpy import env

TILLAMOOK_GDB = r"C:\ColumbiaCarto\work\GDB\Tillamook_911.gdb"


def returnValidStreetDirection():   
    env.Workspace = TILLAMOOK_GDB
    with arcpy.da.SearchCursor(TILLAMOOK_GDB + "\Valid_Street_Direction",["code"]) as cursor:
            return sorted({row[0] for row in cursor})
