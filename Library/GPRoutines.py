#import Snippets  
import datetime  
import arcpy  
import sys  
import os
  
# Snippets.py
# ************************************************
# Updated for ArcGIS 10.7
# ************************************************
# Requires installation of the comtypes package
# Available at: http://sourceforge.net/projects/comtypes/
# Once comtypes is installed, the following modifications
# need to be made for compatibility with ArcGIS 10.2:
# 1) Delete automation.pyc, automation.pyo, safearray.pyc, safearray.pyo
# 2) Edit automation.py
# 3) Add the following entry to the _ctype_to_vartype dictionary (line 794):
#    POINTER(BSTR): VT_BYREF|VT_BSTR,
# ************************************************

#**** Initialization ****

def GetLibPath():
    """Return location of ArcGIS type libraries as string"""
    # This will still work on 64-bit machines because Python runs in 32 bit mode
    #import _winreg
    #keyESRI = _winreg.OpenKey(_winreg.HKEY_LOCAL_MACHINE, "SOFTWARE\\ESRI\\Desktop10.7")
    #return _winreg.QueryValueEx(keyESRI, "InstallDir")[0] + "com\\"
    #print("ZZZZZ:: " + test)
    #return "C:\\Program Files (x86)\\ArcGIS\\Desktop10.7\\com\\"
    
    retVal = arcpy.GetInstallInfo()['InstallDir'] + "\\com\\"
    return retVal

def GetModule(sModuleName):
    """Import ArcGIS module"""
    from comtypes.client import GetModule
    sLibPath = GetLibPath()
    GetModule(sLibPath + sModuleName)

def GetStandaloneModules():
    """Import commonly used ArcGIS libraries for standalone scripts"""
    GetModule("esriSystem.olb")
    
    #GetModule("esriGeometry.olb")
    #GetModule("esriCarto.olb")
    #GetModule("esriDisplay.olb")
    GetModule("esriGeoDatabase.olb")
    GetModule("esriDataSourcesGDB.olb")
    GetModule("esriDataSourcesFile.olb")
    GetModule("esriOutput.olb")

def GetDesktopModules():
    """Import basic ArcGIS Desktop libraries"""
    GetModule("esriFramework.olb")
    GetModule("esriArcMapUI.olb")
    GetModule("esriArcCatalogUI.olb")

#**** Helper Functions ****

def NewObj(MyClass, MyInterface):
    """Creates a new comtypes POINTER object where\n\
    MyClass is the class to be instantiated,\n\
    MyInterface is the interface to be assigned"""
    from comtypes.client import CreateObject
    try:
        ptr = CreateObject(MyClass, interface=MyInterface)
        return ptr
    except:
        return None

def CType(obj, interface):
    """Casts obj to interface and returns comtypes POINTER or None"""
    try:
        newobj = obj.QueryInterface(interface)
        return newobj
    except:
        return None

def CLSID(MyClass):
    """Return CLSID of MyClass as string"""
    return str(MyClass._reg_clsid_)

def InitStandalone():
    """Init standalone ArcGIS license"""
    # Set ArcObjects version
    import comtypes
    from comtypes.client import GetModule
    g = comtypes.GUID("{6FCCEDE0-179D-4D12-B586-58C88D26CA78}")
    GetModule((g, 1, 0))
    import comtypes.gen.ArcGISVersionLib as esriVersion
    import comtypes.gen.esriSystem as esriSystem
    pVM = NewObj(esriVersion.VersionManager, esriVersion.IArcGISVersion)
    ### CHANGE PATH!!!
    if not pVM.LoadVersion(esriVersion.esriArcGISDesktop, "10.4"):  # arcpy.GetInstallInfo()['Version']):  #"10.7"):
        return False
    # Get license
    pInit = NewObj(esriSystem.AoInitialize, esriSystem.IAoInitialize)
    ProductList = [esriSystem.esriLicenseProductCodeAdvanced, \
                   esriSystem.esriLicenseProductCodeStandard, \
                   esriSystem.esriLicenseProductCodeBasic]
    for eProduct in ProductList:
        licenseStatus = pInit.IsProductCodeAvailable(eProduct)
        if licenseStatus != esriSystem.esriLicenseAvailable:
            continue
        licenseStatus = pInit.Initialize(eProduct)
        return (licenseStatus == esriSystem.esriLicenseCheckedOut)
    return False


#**** Standalone ****

def Standalone_OpenFileGDB(sPath):

    GetStandaloneModules()
    if not InitStandalone():
        print "We've got lumps of it 'round the back..."
        return
    import comtypes.gen.esriGeoDatabase as esriGeoDatabase
    import comtypes.gen.esriDataSourcesGDB as esriDataSourcesGDB

    #sPath = "c:/apps/Demo/Montgomery_full.gdb"
    pWSF = NewObj(esriDataSourcesGDB.FileGDBWorkspaceFactory, \
                  esriGeoDatabase.IWorkspaceFactory)
    pWS = pWSF.OpenFromFile(sPath, 0)
    pDS = CType(pWS, esriGeoDatabase.IDataset)
    #print "Workspace name: " + pDS.BrowseName
    #print "Workspace category: " + pDS.Category
    return pWS



#**** ArcCatalog ****

def GetModifiedDate(gdb, tableName):      
    # Setup      
    GetStandaloneModules()      
    InitStandalone()      
    import comtypes.gen.esriSystem as esriSystem      
    import comtypes.gen.esriGeoDatabase as esriGeoDatabase      
    import comtypes.gen.esriDataSourcesGDB as esriDataSourcesGDB      
  
    # Open the FGDB      
    pWS = Standalone_OpenFileGDB(gdb)    
    
    # Create empty Properties Set      
    pPropSet = NewObj(esriSystem.PropertySet, esriSystem.IPropertySet)      
    pPropSet.SetProperty("database", gdb)      
  
    # Cast the FGDB as IFeatureWorkspace      
    pFW = CType(pWS, esriGeoDatabase.IFeatureWorkspace)      
  
    # Open the table      
    pTab = pFW.OpenTable(tableName)      
  
    # Cast the table as a IDatasetFileStat      
    pDFS = CType(pTab, esriGeoDatabase.IDatasetFileStat)      
  
    # Get the date modified      
    return pDFS.StatTime(2) 
  
