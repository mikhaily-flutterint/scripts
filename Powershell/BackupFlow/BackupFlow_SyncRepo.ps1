param(
    [parameter(Mandatory = $True)][string] $ControlServer
)

$ControlDatabase = 'landscape'
$ControlTable = 'dbo.BackupFlow_CentralRepo'
$3Control = $ControlDatabase + '.' + $ControlTable

$TargetDatabase = 'DBA'
$TargetTable = 'dbo.BackupFlow_RepoSync'
$TargetTableRepo = 'dbo.BackupFlow_Repo'

$CopyQuery = "SELECT database_name
, backup_type
, backup_rule
, backup_interval_minutes
, backup_window_start
, backup_window_end_hours
, backup_location
, blocksize
, buffercount
, maxtransfersize
, do_dbcc
, cleanup_time
, forced_day
, max_failure_count
FROM " + $3Control + "
WHERE server_name = '"

$MergeQuery = "MERGE " + $TargetTableRepo + " R USING " + $TargetTable + " RS
ON (R.database_name = RS.database_name
	AND r.backup_type = RS.backup_type
        AND R.backup_rule = RS.backup_rule
    )
WHEN MATCHED
	THEN UPDATE SET 
	  R.backup_interval_minutes =	RS.backup_interval_minutes
	, R.backup_window_start		=	RS.backup_window_start
	, R.backup_window_end_hours =	RS.backup_window_end_hours
	, R.backup_location			=	RS.backup_location
	, R.blocksize				=	RS.blocksize
	, R.buffercount				=	RS.buffercount
	, R.maxtransfersize			=	RS.maxtransfersize
	, R.do_dbcc					=	RS.do_dbcc
	, R.cleanup_time			=	RS.cleanup_time
	, R.forced_day				=	RS.forced_day
	, R.max_failure_count		=	RS.max_failure_count
WHEN NOT MATCHED BY TARGET
	THEN INSERT (
	database_name
	, backup_type
	, backup_rule
	, backup_interval_minutes
	, backup_window_start
	, backup_window_end_hours
	, backup_location
	, blocksize
	, buffercount
	, maxtransfersize
	, do_dbcc
	, cleanup_time
	, forced_day
	, max_failure_count )
	VALUES (
	RS.database_name
	, RS.backup_type
	, RS.backup_rule
	, RS.backup_interval_minutes
	, RS.backup_window_start
	, RS.backup_window_end_hours
	, RS.backup_location
	, RS.blocksize
	, RS.buffercount
	, RS.maxtransfersize
	, RS.do_dbcc
	, RS.cleanup_time
	, RS.forced_day
	, RS.max_failure_count )
WHEN NOT MATCHED BY SOURCE
	THEN DELETE;"

$List_TargetServers_Query = "SELECT DISTINCT
server_name FROM " + $ControlTable
$List_TargetServers = Invoke-DbaQuery -SqlInstance $ControlServer -Database $ControlDatabase -Query $List_TargetServers_Query -As DataTable

foreach ($TargetServer in $List_TargetServers) {
    $3Target = $TargetDatabase + '.' + $TargetTable

    $CopyQuery_Target = $CopyQuery + $TargetServer.server_name + "'"
    # Get data from the central repo
    $copy_params = @{
    SqlInstance = $ControlServer
    Destination = $TargetServer.server_name
    Database = $ControlDatabase
    Table = $3Control
    DestinationDatabase = $TargetDatabase
    DestinationTable = $3Target
    Query = $CopyQuery_Target}
    Copy-DbaDbTableData @copy_params -Truncate

	try {
		Invoke-DbaQuery -SqlInstance $TargetServer.server_name -Database $TargetDatabase -Query $MergeQuery -WarningVariable WarningVarriable
	}
	finally {
		if ($null -eq $WarningVarriable -or $WarningVarriable -eq "" -or !$WarningVarriable)
		{#okay
		}
		else
		{#not okay
			Invoke-DbaQuery -SqlInstance $TargetServer -Query "
			INSERT INTO DBA.dbo.CommandLog
			( DatabaseName
			, Command
			, CommandType
			, StartTime
			, EndTime
			, ErrorNumber
			, ErrorMessage)
			VALUES
			( 'DBA'
			, 'Metadata Sync'
			, 'BackupFlow'
			, GETDATE()
			, GETDATE()
			, 224
			, @ps_WarningVarriable)" -SqlParameters @{"ps_WarningVarriable"=$WarningVarriable}
		}
	}
Clear-Variable WarningVarriable
}
# SIG # Begin signature block
# MIINcwYJKoZIhvcNAQcCoIINZDCCDWACAQExCzAJBgUrDgMCGgUAMGkGCisGAQQB
# gjcCAQSgWzBZMDQGCisGAQQBgjcCAR4wJgIDAQAABBAfzDtgWUsITrck0sYpfvNR
# AgEAAgEAAgEAAgEAAgEAMCEwCQYFKw4DAhoFAAQUQtLx08Tf/mEKj3KCULlXLHDN
# qI6gggrdMIIFDDCCAvSgAwIBAgITFwAAAANoHQGylzOSswAAAAAAAzANBgkqhkiG
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
# DjAMBgorBgEEAYI3AgEVMCMGCSqGSIb3DQEJBDEWBBRg6ftkeYejlMhY1FRWLtES
# W1AwtTANBgkqhkiG9w0BAQEFAASCAQBheRXZg/Vje1BHU5aJXP4uULBo65+Z0sIU
# 1CZqmgYMdB14xGAVm60XICGfdesFlg4g1FpM9W881bwZQPKsJrD8Erf2ZX/JXig8
# N9Kv6OUswPgi5kxjZ01IkrTNmVgcq2XN+5Sxt2U6hKAPyn23Y6EByLvFZftsTr7a
# nc6pv6KDYxWb7SZ20iatbfPggDECjR8Mv5uffjeG64azlMpZFfIG7UC3BmefZGDp
# cAbOB3GTeL4oI8UANpN89LP0WQIzr8ylawgIs9yhoAL+6ozeM6sm2dGG6e5sq5ZF
# V+Yd5acdYyvUgtLnazKFR5iphQ8uh/xyz+greQIMbBuv6KPbfLPK
# SIG # End signature block
