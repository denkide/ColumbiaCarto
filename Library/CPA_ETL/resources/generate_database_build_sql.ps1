# Execution: powershell.exe -ExecutionPolicy Bypass -File {filename}

Param([string]$instance, [string]$database, [string]$output)
 
set-psdebug -strict
$ErrorActionPreference = 'stop'
# Load SMO assembly.
$v = [System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Smo")
# If running SQL Server 2008 DLLs, load SMOExtended & SQLWMIManagement libs.
if ((($v.FullName.Split(','))[1].Split('='))[1].Split('.')[0] -ne '9') {
    [System.Reflection.Assembly]::LoadWithPartialName(
        "Microsoft.SqlServer.SMOExtended"
        ) | out-null
    }
$s = New-Object ("Microsoft.SqlServer.Management.Smo.Server") $instance
if ($s.Version -eq  $null ){Throw "Can't find the instance $instance"}
$db = $s.Databases[$database]
if ($db.name -ne $database){
    Throw "Can't find the database '$database' in $instance"
    };
$transfer = New-Object ("Microsoft.SqlServer.Management.Smo.Transfer") $db
$transfer.Options.ScriptBatchTerminator = $true
$transfer.Options.ToFileOnly = $true
$transfer.Options.Filename = $output;
$transfer.ScriptTransfer()
