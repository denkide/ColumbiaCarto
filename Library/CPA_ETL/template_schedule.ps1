# Execution:
# powershell.exe -ExecutionPolicy Bypass -File {filename}.ps1

$batch_name = '##TODO: Batch name must be in ETL_Load_A.dbo.Metadata_ETL_Batch'

switch($env:computername) {
    'CLWRKGIS' {
        $python2 = 'C:\Python27\envs\ETL\Scripts\python.exe'
        $python3 = 'C:\Program Files\ArcGIS\Pro\bin\Python\envs\etl3\python.exe'
        ##TODO: Add new computers & python paths here if running elsewhere.
        }
    }
Set-Location \\clsrv111\gis\Development\CPA_ETL\scripts


& $python2 exec_file.py  # If no arguments, will run the default pipeline(s).
& $python3 exec_batch_notification.py $batch_name
