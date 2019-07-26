# Execution:
# powershell.exe -ExecutionPolicy Bypass -File weekly_03_monday.ps1

$batch_name = 'CPA_Weekly_03_Monday'

switch($env:computername) {
    'CLWRKGIS' {
        $python2 = 'C:\Python27\envs\ETL\Scripts\python.exe'
        $python3 = 'C:\Program Files\ArcGIS\Pro\bin\Python\envs\etl3\python.exe'
    }
}
Set-Location \\gisrv100.ris5.net\work\Processing\CPA_ETL\scripts


& $python2 exec_gimap_datasets.py WEEKLY_02_JOB
& $python2 exec_eugene_parcel_database.py WEEKLY_JOB
& $python2 exec_state_deliveries.py WEEKLY_JOB
& $python2 exec_batch_notification.py $batch_name
