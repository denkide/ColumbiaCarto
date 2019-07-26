# Execution:
# powershell.exe -ExecutionPolicy Bypass -File daily.ps1

$batch_name = 'CPA_Daily'

switch($env:computername) {
    'CLWRKGIS' {
        $python2 = 'C:\Python27\envs\ETL\Scripts\python.exe'
        $python3 = 'C:\Program Files\ArcGIS\Pro\bin\Python\envs\etl3\python.exe'
    }
}
Set-Location \\gisrv100.ris5.net\work\Processing\CPA_ETL\scripts


& $python3 exec_rlid_documents.py DAILY_JOB
& $python2 exec_assess_tax_datasets.py DAILY_JOB
& $python2 exec_gimap_datasets.py DAILY_JOB
& $python3 exec_batch_notification.py $batch_name
