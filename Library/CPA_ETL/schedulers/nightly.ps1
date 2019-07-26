# Execution:
# powershell.exe -ExecutionPolicy Bypass -File nightly.ps1

$batch_name = 'CPA_Nightly'

switch($env:computername) {
    'CLWRKGIS' {
        $python2 = 'C:\Python27\envs\ETL\Scripts\python.exe'
        $python3 = 'C:\Program Files\ArcGIS\Pro\bin\Python\envs\etl3\python.exe'
    }
}
Set-Location \\gisrv100.ris5.net\work\Processing\CPA_ETL\scripts

& $python2 exec_production_datasets.py NIGHTLY_JOB
& $python2 exec_address_assess_tax_info.py NIGHTLY_JOB
& $python2 exec_address_issues.py NIGHTLY_JOB
& $python3 exec_rlid_documents.py NIGHTLY_JOB
& $python2 exec_geodatabase_maintenance.py NIGHTLY_JOB
& $python3 exec_batch_notification.py $batch_name
