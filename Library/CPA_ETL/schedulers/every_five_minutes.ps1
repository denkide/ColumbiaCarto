# Execution:
# powershell.exe -ExecutionPolicy Bypass -File every_five_minutes.ps1

$batch_name = 'CPA_Every_Five_Minutes'

switch($env:computername) {
    'CLWRKGIS' {
        $python2 = 'C:\Python27\envs\ETL\Scripts\python.exe'
        $python3 = 'C:\Program Files\ArcGIS\Pro\bin\Python\envs\etl3\python.exe'
    }
}
Set-Location \\gisrv100.ris5.net\work\Processing\CPA_ETL\scripts


& $python3 exec_license_usage.py FIVE_MINUTE_JOB
# No notification for short-interval batches.
# & $python3 exec_batch_notification.py $batch_name
