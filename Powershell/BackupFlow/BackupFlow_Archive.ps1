$ControlServer = "IMDGNDWADM10"
$ControlDatabase = 'landscape'

$List_TargetServers_Query = "SELECT DISTINCT server_name FROM landscape.dbo.BackupFlow_CentralRepo"
$msdb_query = "SELECT 1 ID,
s.backup_set_id,
s.server_name, 
s.database_name, 
m.physical_device_name, 
s.backup_start_date, 
s.backup_finish_date,
s.[type],
s.recovery_model
FROM msdb.dbo.backupset s 
INNER JOIN msdb.dbo.backupmediafamily m ON s.media_set_id = m.media_set_id 
WHERE 1=1
AND server_name=@@SERVERNAME
AND backup_set_id > @var_max_own_backupset_id
ORDER BY s.backup_set_id ASC"
$max_own_backupset_id_query = "SELECT ISNULL((SELECT TOP (1) backup_set_id
FROM landscape.dbo.BackupFlow_Archive
WHERE 1 = 1
AND server_name = @var_server_name
ORDER BY backup_set_id DESC)
, 0
) backup_set_id;"
$Retention_Query = "DELETE FROM landscape.dbo.BackupFlow_Archive 
WHERE backup_start_date <= DATEADD(YEAR,-2,GETDATE())"

$List_TargetServers = Invoke-DbaQuery -SqlInstance $ControlServer -Database $ControlDatabase -Query $List_TargetServers_Query -As DataTable

foreach ($TargetServer in $List_TargetServers) {
    $max_own_backupset_id = Invoke-DbaQuery -SqlInstance $ControlServer -Database $ControlDatabase -Query $max_own_backupset_id_query -SqlParameters @{"var_server_name"=$TargetServer.server_name;} -As SingleValue

    $msdb_result = Invoke-DbaQuery -SqlInstance $TargetServer.server_name -Database 'msdb' -Query $msdb_query -SqlParameters @{"var_max_own_backupset_id"=$max_own_backupset_id} -As DataTable
        if ($null -ne $msdb_result) {
            Write-DbaDataTable -SqlInstance $ControlServer -InputObject $msdb_result -Database $ControlDatabase -Table "landscape.dbo.BackupFlow_Archive"
            Clear-variable -Name "msdb_result"
        }
}

# delete older than 2 years
Invoke-DbaQuery -SqlInstance $ControlServer -Database $ControlDatabase -Query $Retention_Query
# SIG # Begin signature block
# MIINcwYJKoZIhvcNAQcCoIINZDCCDWACAQExCzAJBgUrDgMCGgUAMGkGCisGAQQB
# gjcCAQSgWzBZMDQGCisGAQQBgjcCAR4wJgIDAQAABBAfzDtgWUsITrck0sYpfvNR
# AgEAAgEAAgEAAgEAAgEAMCEwCQYFKw4DAhoFAAQUnvp4F/iS/V8fG11crFEq7EUX
# JzOgggrdMIIFDDCCAvSgAwIBAgITFwAAAANoHQGylzOSswAAAAAAAzANBgkqhkiG
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
# DjAMBgorBgEEAYI3AgEVMCMGCSqGSIb3DQEJBDEWBBTwQV9JIYARbV1PBQ6qD8Y0
# VjsT6TANBgkqhkiG9w0BAQEFAASCAQDHqUMFX1ZcfPjoye8j5LMr0ORZPDDvl3gn
# i7/KVQ7UPJ97HZnyuCuUoZ7LsX6wswiwdiHzm/fgaE/YgAMT+IYWicozxMXubA9C
# W0Za4gOsPuNlvhXJyb01Em/VDejjsYkaPDHZR7mY1gSaaYf4LhMv0eqflBNUEewy
# KNQ2SXuV95VemO4t/3kPwVZ2NvlwDGVziLu6i++QvYj1ztVtkihWPcLXgC53G753
# 8aizemq250HjxDyUwROtQSpFsawdtYvz842Ul7UXST2Z36AN6yLFb7DNPnUMTmKS
# 8zQ4rTOZHhVKyPtrxICs6k0/D710JaJku7MbKft30YPqiNJ073rO
# SIG # End signature block
