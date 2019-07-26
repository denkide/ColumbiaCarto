# Execution:
# powershell.exe -ExecutionPolicy Bypass -File weekly_01_weekend.ps1

$batch_name = 'CPA_Weekly_01_Weekend'

switch($env:computername) {
    'CLWRKGIS' {
        $python2 = 'C:\Python27\envs\ETL\Scripts\python.exe'
        $python3 = 'C:\Program Files\ArcGIS\Pro\bin\Python\envs\etl3\python.exe'
    }
}
Set-Location \\gisrv100.ris5.net\work\Processing\CPA_ETL\scripts


& $python2 exec_boundary_datasets.py WEEKLY_JOB
& $python2 exec_assess_tax_datasets.py WEEKLY_JOB
& $python2 exec_planning_development_datasets.py BOUNDARY_DATASETS_JOB
& $python2 exec_public_safety_datasets.py BOUNDARY_DATASETS_JOB
& $python2 exec_land_use_datasets.py WEEKLY_JOB
# Will fail under LCOG\clgisadmin. Run with LCOG1\clgisadmin until corrected.
& $python2 exec_address_postal_info.py WEEKLY_JOB
& $python2 exec_address_datasets.py WEEKLY_JOB
& $python2 exec_msag.py WEEKLY_JOB
& $python2 exec_taxlot_datasets.py WEEKLY_JOB
& $python2 exec_transportation_datasets.py WEEKLY_JOB
& $python2 exec_spatial_reference_datasets.py WEEKLY_JOB
& $python2 exec_public_safety_datasets.py TAXLOT_FIRE_PROTECTION_JOB
& $python2 exec_taxlot_xrefs.py FLOOD_HAZARD_JOB
# Will fail under LCOG\clgisadmin. Run with LCOG1\clgisadmin until corrected.
& $python2 exec_taxlot_xrefs.py PETITION_JOB
# Will fail under LCOG\clgisadmin. Run with LCOG1\clgisadmin until corrected.
& $python2 exec_taxlot_xrefs.py PLAT_JOB
& $python2 exec_taxlot_xrefs.py SOIL_JOB
& $python2 exec_planning_development_datasets.py TAXLOT_ZONING_JOB
& $python2 exec_service_facility_datasets.py WEEKLY_JOB
& $python3 exec_rlid_gis_load.py WEEKLY_JOB
# Will fail under LCOG\clgisadmin. Run with LCOG1\clgisadmin until corrected.
& $python2 exec_rlid_documents.py WEEKLY_JOB
& $python2 exec_rlid_documents_deeds_records.py WEEKLY_JOB
& $python2 exec_gimap_datasets.py WEEKLY_01_JOB
& $python2 exec_batch_notification.py $batch_name
