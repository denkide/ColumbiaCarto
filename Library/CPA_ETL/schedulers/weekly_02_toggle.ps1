# Execution:
# powershell.exe -ExecutionPolicy Bypass -File weekly_02_toggle.ps1

$batch_name = 'CPA_Weekly_02_Toggle'

switch($env:computername) {
    'CLWRKGIS' {
        $python2 = 'C:\Python27\envs\ETL\Scripts\python.exe'
        $python3 = 'C:\Program Files\ArcGIS\Pro\bin\Python\envs\etl3\python.exe'
    }
}
Set-Location \\gisrv100.ris5.net\work\Processing\CPA_ETL\scripts


& $python2 exec_gis_toggle.py RLID_GIS_JOB
& $python2 exec_rlidgeo.py WEEKLY_JOB
& $python2 exec_regional_data_warehouse.py WEEKLY_JOB
& $python2 exec_batch_notification.py $batch_name
