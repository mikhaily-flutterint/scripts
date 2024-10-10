##ControlServer
$ControlServer = "IMDGNDWADM10"
$DaysBack = -45

##Functions
function EmailNotification {
    param ([parameter(Mandatory=$true)][string]$ErrVarr)
    Invoke-DbaQuery -SqlInstance $ControlServer -Query "
    DECLARE @V_Body NVARCHAR(MAX) = ('Errors occured during HouseKeeping;'
    + CHAR(13)+CHAR(10) + 'Error message: ' + CAST(@TheErrorMessage as NVARCHAR(MAX))
    + CHAR(13)+CHAR(10) + 'Check landscape.dbo.housekeeping_log for more information;')
    DECLARE @V_Profile sysname = 'IOMMTA01'
    DECLARE @V_Recipients NVARCHAR(MAX) = 'dwhinfraalerts@csr.pstars'
    DECLARE @V_Importance VARCHAR(6) = 'High'
    EXEC msdb.dbo.sp_send_dbmail
    @profile_name = @V_Profile
    , @recipients = @V_Recipients
    , @blind_copy_recipients = 'kaloyan_kosev@starsgroup.com'
    , @subject = N'HouseKeeping: Errors during backup deletion!'
    , @body = @V_Body
    , @body_format = 'HTML'
    , @importance = @V_Importance;" -SqlParameters @{"TheErrorMessage" =$ErrVarr}}

## Clear temp tables
Invoke-DbaQuery -SqlInstance $ControlServer -Query "TRUNCATE TABLE landscape.dbo.housekeeping_msdbresult"
Invoke-DbaQuery -SqlInstance $ControlServer -Query "TRUNCATE TABLE landscape.dbo.housekeeping_nsresults"
Write-Host "Truncation completed;"

$AllServers = Invoke-DbaQuery -SqlInstance $ControlServer -Query "SELECT DISTINCT ServerName server_name FROM landscape.dbo.housekeeping_control WHERE Active = 1"

    foreach ($CurrentServer in $AllServers) {
    Write-Host "Now working with :" $CurrentServer.server_name 

    $CurrentMsdbResult = Invoke-DbaQuery -SqlInstance $CurrentServer.server_name -Query "SELECT DISTINCT
    s.server_name
    , s.database_name
    , m.physical_device_name
    , s.backup_finish_date
    , s.type
    FROM msdb.dbo.backupset s
    INNER JOIN msdb.dbo.backupmediafamily m
        ON s.media_set_id = m.media_set_id
    WHERE s.backup_finish_date > DATEADD(DAY, @DaysBack, GETDATE());" -SqlParameters @{"DaysBack"=$DaysBack} -As DataTable
    if ($null -ne $CurrentMsdbResult) {
        Write-Host "Now writing data for " $CurrentServer.server_name
        Write-DbaDataTable -SqlInstance $ControlServer -InputObject $CurrentMsdbResult -Database "landscape" -Table "landscape.dbo.housekeeping_msdbresult"
        Clear-variable -Name "CurrentMsdbResult"
        }
}

$Houses = Invoke-DbaQuery -SqlInstance $ControlServer -Database "landscape" -Query "SELECT DISTINCT PathRoot FROM landscape.dbo.housekeeping_control WHERE Active = 1;"
Write-Host $Houses
foreach ($House in $Houses) {
    Write-Host "Now working with :" $House.PathRoot
    $NWResult = Get-ChildItem -Path $House.PathRoot -Recurse | Where-Object { -not $_.PSIsContainer} | Where-Object {$_.Attributes -eq "Normal"}

    if ($null -ne $NWResult) {
    $NWResult | Add-Member NoteProperty Root ($House.PathRoot)
    $NWResult | Select-Object -Property Root, FullName, LastWriteTime -Unique | Write-DbaDataTable -SqlInstance $ControlServer -Database "landscape" -Table "landscape.dbo.housekeeping_nsresults"
    Clear-variable -Name "NWResult"}
    else {
        Write-Host "Bad NWResult"

    }
}

$FullDeletes = Invoke-DbaQuery -SqlInstance $ControlServer -Database "landscape" -Query "EXECUTE dbo.housekeeping_nextdeletes @BackupType = 'D'" -As DataTable

foreach($File in $FullDeletes){
    #Write-Host "Now attempting to delete the full backup file " $File.ns_physical_device_name
    $File.ns_physical_device_name | Remove-Item -ErrorAction SilentlyContinue -ErrorVariable ErrVarr
    if($ErrVarr){
        EmailNotification -ErrVarr $ErrVarr
        $ErrorMessage = $ErrVarr | Out-String
        $DeleteStatus = "2"
        $Now = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
        $File | Select-Object -Property `
        server_name,database_name,DBType,backup_finish_date,Root,ns_physical_device_name,LastWriteTime,RowNum,FullSets,`
        @{Name = 'TimeStamp'; Expression = { $Now }}, `
        @{Name = 'DeleteStatus'; Expression = { $DeleteStatus }},`
        @{Name = 'ErrorMessage'; Expression = { $ErrorMessage }} | Write-DbaDataTable -SqlInstance $ControlServer -Database "landscape" -Table "landscape.dbo.housekeeping_log"
        Clear-Variable -Name "ErrVarr","ErrorMessage","DeleteStatus","File","Now"
        }
        else{
        $DeleteStatus = "7"
        $Now = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
        $File | Select-Object -Property `
        server_name,database_name,DBType,backup_finish_date,Root,ns_physical_device_name,LastWriteTime,RowNum,FullSets,`
        @{Name = 'TimeStamp'; Expression = { $Now }}, `
        @{Name = 'DeleteStatus'; Expression = { $DeleteStatus }} | Write-DbaDataTable -SqlInstance $ControlServer -Database "landscape" -Table "landscape.dbo.housekeeping_log"
        Clear-Variable -Name "DeleteStatus","File","Now"
        }
}
Clear-Variable -Name "FullDeletes"

$DiffDeletes = Invoke-DbaQuery -SqlInstance $ControlServer -Database "landscape" -Query "EXECUTE dbo.housekeeping_nextdeletes @BackupType = 'I'" -As DataTable
foreach($File in $DiffDeletes){
    #Write-Host "Now attempting to delete the diff backup file " $File.ns_physical_device_name
    $File.ns_physical_device_name | Remove-Item -ErrorAction SilentlyContinue -ErrorVariable ErrVarr
    if($ErrVarr){
        EmailNotification -ErrVarr $ErrVarr
        $ErrorMessage = $ErrVarr | Out-String
        $DeleteStatus = "2"
        $Now = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
        $File | Select-Object -Property `
        server_name,database_name,DBType,backup_finish_date,Root,ns_physical_device_name,LastWriteTime,RowNum,FullSets,`
        @{Name = 'TimeStamp'; Expression = { $Now }}, `
        @{Name = 'DeleteStatus'; Expression = { $DeleteStatus }},`
        @{Name = 'ErrorMessage'; Expression = { $ErrorMessage }} | Write-DbaDataTable -SqlInstance $ControlServer -Database "landscape" -Table "landscape.dbo.housekeeping_log"
        Clear-Variable -Name "ErrVarr","ErrorMessage","DeleteStatus","File","Now"
        }
        else{
        $DeleteStatus = "7"
        $Now = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
        $File | Select-Object -Property `
        server_name,database_name,DBType,backup_finish_date,Root,ns_physical_device_name,LastWriteTime,RowNum,FullSets,`
        @{Name = 'TimeStamp'; Expression = { $Now }}, `
        @{Name = 'DeleteStatus'; Expression = { $DeleteStatus }} | Write-DbaDataTable -SqlInstance $ControlServer -Database "landscape" -Table "landscape.dbo.housekeeping_log"
        Clear-Variable -Name "DeleteStatus","File","Now"
        }
}
Clear-Variable -Name "DiffDeletes"

$TlogDeletes = Invoke-DbaQuery -SqlInstance $ControlServer -Database "landscape" -Query "EXECUTE dbo.housekeeping_nextdeletes @BackupType = 'L'" -As DataTable
foreach($File in $TlogDeletes){
    #Write-Host "Now attempting to delete the log backup file " $File.ns_physical_device_name
    $File.ns_physical_device_name | Remove-Item -ErrorAction SilentlyContinue -ErrorVariable ErrVarr
    if($ErrVarr){
        EmailNotification -ErrVarr $ErrVarr
        $ErrorMessage = $ErrVarr | Out-String
        $DeleteStatus = "2"
        $Now = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
        $File | Select-Object -Property `
        server_name,database_name,DBType,backup_finish_date,Root,ns_physical_device_name,LastWriteTime,RowNum,FullSets,`
        @{Name = 'TimeStamp'; Expression = { $Now }}, `
        @{Name = 'DeleteStatus'; Expression = { $DeleteStatus }},`
        @{Name = 'ErrorMessage'; Expression = { $ErrorMessage }} | Write-DbaDataTable -SqlInstance $ControlServer -Database "landscape" -Table "landscape.dbo.housekeeping_log"
        Clear-Variable -Name "ErrVarr","ErrorMessage","DeleteStatus","File","Now"
        }
        else{
        $DeleteStatus = "7"
        $Now = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
        $File | Select-Object -Property `
        server_name,database_name,DBType,backup_finish_date,Root,ns_physical_device_name,LastWriteTime,RowNum,FullSets,`
        @{Name = 'TimeStamp'; Expression = { $Now }}, `
        @{Name = 'DeleteStatus'; Expression = { $DeleteStatus }} | Write-DbaDataTable -SqlInstance $ControlServer -Database "landscape" -Table "landscape.dbo.housekeeping_log"
        Clear-Variable -Name "DeleteStatus","File","Now"
        }
}
Clear-Variable -Name "TlogDeletes"
# SIG # Begin signature block
# MIINcwYJKoZIhvcNAQcCoIINZDCCDWACAQExCzAJBgUrDgMCGgUAMGkGCisGAQQB
# gjcCAQSgWzBZMDQGCisGAQQBgjcCAR4wJgIDAQAABBAfzDtgWUsITrck0sYpfvNR
# AgEAAgEAAgEAAgEAAgEAMCEwCQYFKw4DAhoFAAQU/47hWnR4BEyvTjGUk2/W5nQn
# 9wWgggrdMIIFDDCCAvSgAwIBAgITFwAAAANoHQGylzOSswAAAAAAAzANBgkqhkiG
# 9w0BAQsFADAXMRUwEwYDVQQDEwxJTURHTkNBMDEtQ0EwHhcNMTUwOTI0MTI0NTMy
# WhcNMjUwOTI0MTI1NTMyWjBIMRYwFAYKCZImiZPyLGQBGRYGcHN0YXJzMRMwEQYK
# CZImiZPyLGQBGRYDY3NyMRkwFwYDVQQDExBjc3ItSU1ER05DQTAyLUNBMIIBIjAN
# BgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAw8zv8TARxL+PCBPi1Chyc/Qq8I9/
# hpq4BzrO4toTpHSLqA2q6ifsNk8MCZiqcG8f+dC2rwlc23lIqC8VMiaNG0YsfnkZ
# ntow1dB9YtC+arCvqHYOyIjNMlUMRonfJcZIU/LBlFR7an+TTDqVqG3d7d8iXaGC
# E/xuHMd5+2hCzPeAP5Pzs/0GNtQjY5nSnQbI7rdr7YWdHiL0OblOgML2oWXyCYJd
# nYUh78/ehZOMLlEQbe1gSIg1+d+2a4pmGHuhkTjYiW80BysXaQU3nSS53igkK/M+
# CIqwdStX8uzF09qRuaaRsSZSunJ/OkZHGsYNgx25VABKRq/LwAtSUe4btwIDAQAB
# o4IBHjCCARowEAYJKwYBBAGCNxUBBAMCAQAwHQYDVR0OBBYEFI7hQGTF14SLzlEo
# /vi8lelMKXd5MBkGCSsGAQQBgjcUAgQMHgoAUwB1AGIAQwBBMAsGA1UdDwQEAwIB
# hjAPBgNVHRMBAf8EBTADAQH/MB8GA1UdIwQYMBaAFLFm416sja8DKvX1xL7G1eiP
# iGgSMDsGA1UdHwQ0MDIwMKAuoCyGKmh0dHA6Ly9zc2wuY3NyLnBzdGFycy9DRFAv
# SU1ER05DQTAxLUNBLmNybDBQBggrBgEFBQcBAQREMEIwQAYIKwYBBQUHMAKGNGh0
# dHA6Ly9zc2wuY3NyLnBzdGFycy9BSUEvSU1ER05DQTAxX0lNREdOQ0EwMS1DQS5j
# cnQwDQYJKoZIhvcNAQELBQADggIBAFfs30FiSEC4uMqH4IurD0rAYzJw5bIG+rqP
# /UlQNQV2V2OIUb70HrpQFL6OCH5oPWfjgfKseqIZXd1uLigoHN8Sum+G55NNYl7j
# AGyAy5IkyetJfuhm5asipL62CTNVp9GrZBKvxFSRax/NKRjR/EM9iMzxfVwRNHSm
# RgZTZDobv9VSLs3WTbgw/Z0dA/3+Urb2ziI3ifqq3W/AVI5zO7w+nRX56k22RgIJ
# aNwJeXIOF1dXlOS6CwAKz8Uc7AKBCWk1e+uJUWtROU4FNMIdgdZCoTIatHzju509
# U6a7DmootD2fLD/pibgMV8iim4vrN58I//Ryq/toa8kIh7XrESTjVsObas1gsKPp
# 5E1eoNQzKLdNoKa1+DU6F6hq1yQzqEnTNlhc7KgwGbiSk68lE5KimWWltfe6ik4l
# APIvRQiXPgRDWmKMWXXFvz4afQWiXqZHHu01R6wsCIJ6WZvk/uh6Q9wjqAWk/Q4I
# nKZSDarTKVniSEuAB5NwxAt7+58yK2EgKCeQGBG39/8I6GnVsXZjIYKtZTD2n7IJ
# 9QmctpMeatUMPNfwh1qQHILtEDv54Titncn5Mg1mRgtkxkOb4nWCf5V+k+8rh03d
# L1ZpwByAYrzlArzkBb6EhjvCg+EAJyVAXEQhTTCXWQKEWr8YwAfN6ErTx1lejXOU
# ZsKAmX0RMIIFyTCCBLGgAwIBAgITIQACWN2uan0pdgjQ/gAAAAJY3TANBgkqhkiG
# 9w0BAQsFADBIMRYwFAYKCZImiZPyLGQBGRYGcHN0YXJzMRMwEQYKCZImiZPyLGQB
# GRYDY3NyMRkwFwYDVQQDExBjc3ItSU1ER05DQTAyLUNBMB4XDTIxMDQyNzEwMjQw
# MVoXDTI1MDkyNDEyNTUzMlowFzEVMBMGA1UEAxMMQ29ycG9yYXRlIElUMIIBIjAN
# BgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA3rN562HRTY6ZeQeY6sHzTrKXYU8s
# cvXOTHMaC6VDhXRCg314aznDLFaRJxXsO18c8G6FD53NXTN8Y/fjUOswx89/30i5
# 9sC1y++nqjY1Ej8zPjUM9g2yYMGcy5FKtg/KOezuEly3S31DGFbB6xnR+Hf5fWMO
# 697CoEt4bfSvIY0gyvYwdJcV5ViXdzEoanuHi41zVKaARVpUwTe+1bFMvd8EUpaa
# CrJaZ4ZKTYplngdCSQ9KpxZt5N4kssrrFMzYfkI2GewQJjstU3XgoqWCRvOgEwjv
# WCyaJQKCkL4A/wcIi8p31DNjShcC4l6+IvIQN/NvaPLzR0G/096rCQU4oQIDAQAB
# o4IC2zCCAtcwPAYJKwYBBAGCNxUHBC8wLQYlKwYBBAGCNxUIze0ghMPbW4SVhxqB
# tfcAhdTJfgiFnYBYgf+dGQIBZAIBBzATBgNVHSUEDDAKBggrBgEFBQcDAzALBgNV
# HQ8EBAMCB4AwGwYJKwYBBAGCNxUKBA4wDDAKBggrBgEFBQcDAzAdBgNVHQ4EFgQU
# B9Prpl5u1/rxT+bCFpgXg6mL/IowHwYDVR0jBBgwFoAUjuFAZMXXhIvOUSj++LyV
# 6Uwpd3kwgf8GA1UdHwSB9zCB9DCB8aCB7qCB64aBuGxkYXA6Ly8vQ049Y3NyLUlN
# REdOQ0EwMi1DQSxDTj1JTURHTkNBMDIsQ049Q0RQLENOPVB1YmxpYyUyMEtleSUy
# MFNlcnZpY2VzLENOPVNlcnZpY2VzLENOPUNvbmZpZ3VyYXRpb24sREM9Y3NyLERD
# PXBzdGFycz9jZXJ0aWZpY2F0ZVJldm9jYXRpb25MaXN0P2Jhc2U/b2JqZWN0Q2xh
# c3M9Y1JMRGlzdHJpYnV0aW9uUG9pbnSGLmh0dHA6Ly9zc2wuY3NyLnBzdGFycy9D
# RFAvY3NyLUlNREdOQ0EwMi1DQS5jcmwwggEUBggrBgEFBQcBAQSCAQYwggECMIGu
# BggrBgEFBQcwAoaBoWxkYXA6Ly8vQ049Y3NyLUlNREdOQ0EwMi1DQSxDTj1BSUEs
# Q049UHVibGljJTIwS2V5JTIwU2VydmljZXMsQ049U2VydmljZXMsQ049Q29uZmln
# dXJhdGlvbixEQz1jc3IsREM9cHN0YXJzP2NBQ2VydGlmaWNhdGU/YmFzZT9vYmpl
# Y3RDbGFzcz1jZXJ0aWZpY2F0aW9uQXV0aG9yaXR5ME8GCCsGAQUFBzAChkNodHRw
# Oi8vc3NsLmNzci5wc3RhcnMvQUlBL0lNREdOQ0EwMi5jc3IucHN0YXJzX2Nzci1J
# TURHTkNBMDItQ0EuY3J0MA0GCSqGSIb3DQEBCwUAA4IBAQAkOKi5XaVoBtGoiYv/
# nNsmi5+Pl2aPrUF3Jgdp69/0aFofxB6w5xy/ygsWpehXPihDImeSOf4AjQLZajGN
# DOSoFFpzVu9cvYpBuOEfg7Y5vf4nyNXgkWjTWKgGPUQ9E6xnf2t+I9N4oRLs9Cy8
# DiIS2xkuQKf/EOP5BYLFF6nb0sRpsDnEDpD8KkX+J7jKVcPiDLXoHYXLYdao56Yk
# xg9CIc/qnQJ+T3A3q2epKW4+VfwYi+bztzh5vSoOe6UbAg91EjVXRXEzwtun5wE6
# dschdp6AKTSBUIRsXVXREcHwbcQFPFo0/g0af2Z2iifEyzMwxCQ9CagZASNOYaiu
# lIwYMYICADCCAfwCAQEwXzBIMRYwFAYKCZImiZPyLGQBGRYGcHN0YXJzMRMwEQYK
# CZImiZPyLGQBGRYDY3NyMRkwFwYDVQQDExBjc3ItSU1ER05DQTAyLUNBAhMhAAJY
# 3a5qfSl2CND+AAAAAljdMAkGBSsOAwIaBQCgeDAYBgorBgEEAYI3AgEMMQowCKAC
# gAChAoAAMBkGCSqGSIb3DQEJAzEMBgorBgEEAYI3AgEEMBwGCisGAQQBgjcCAQsx
# DjAMBgorBgEEAYI3AgEVMCMGCSqGSIb3DQEJBDEWBBTp+Hs/gbOI+HR/qSB/D2wW
# 9Okr2TANBgkqhkiG9w0BAQEFAASCAQCC5aifagiQvyK2f+P8PiV+GJIQyoMHZjaH
# jYs4wj95wpcFTAMuHQBeuXFnBnpM5KTrnPwBqciViChuiE/XA/X+dFiB91+/JmMr
# DSKcGRGNNicfzDYy0mcNTGjM4hAw4u77IQqLCdVieCy0WJF502Bdjy6eJjZKPUNK
# F9LMVgsKZl4+Vl0AvkNODZ2H8/Lm/iuKsgg3jm/gdLQLQ5bJbrkVp1jkzpU7KqO6
# adpKK/ZiAmze3EjpeqaxF59PSOjyjEeWQgI47dH2XUgCciUrsCce5R/KjgEJQOwJ
# igy2G76s3DUdBSCrAOHMLHDbGEK2zadOslt6nYsuAMnvb3lNpAau
# SIG # End signature block
