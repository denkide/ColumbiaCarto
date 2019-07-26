# Execution:
# powershell.exe -ExecutionPolicy Bypass -File monthly.ps1

$batch_name = 'CPA_Monthly'

switch($env:computername) {
    'CLWRKGIS' {
        $python2 = 'C:\Python27\envs\ETL\Scripts\python.exe'
        $python3 = 'C:\Program Files\ArcGIS\Pro\bin\Python\envs\etl3\python.exe'
    }
}
Set-Location \\gisrv100.ris5.net\work\Processing\CPA_ETL\scripts


& $python2 exec_rlidgeo.py MONTHLY_JOB
& $python2 exec_address_reports.py MONTHLY_JOB
& $python2 exec_state_deliveries.py MONTHLY_JOB
& $python2 exec_batch_notification.py $batch_name
