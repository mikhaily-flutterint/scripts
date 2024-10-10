$OnCallGroup = "DWH On Call"
$SqlServer = "IMDGNDWADM10"
$Logfile = "C:\Code\Oncall\log.txt"

function WriteLog
{
    Param ([string]$LogString)
    
    $Stamp = (Get-Date).toString("yyyy/MM/dd HH:mm:ss")
    if ($LogString.Length -gt 1) 
    {
        $LogMessage = "$Stamp    $LogString"
    }
    else
    {
        $LogMessage = ""
    }
    Add-content $LogFile -value $LogMessage
}

try{
	$Connectivity = tnc 10.123.66.10 -port 9389
	if (!($Connectivity.TcpTestSucceeded)){ throw "Failed to connect to 10.123.66.10"}
	#WriteLog "Connected to 10.123.66.10"
	$DC = "10.123.66.10"
}catch{
	try{
        WriteLog "Error: $_"
		$Connectivity = tnc 10.123.66.139 -port 9389
		if (!($Connectivity.TcpTestSucceeded)){ throw "Failed to connect to 10.123.66.139... aborting"}
		WriteLog "Connected to 10.123.66.139"
		$DC = "10.123.66.139"
	}catch{
        WriteLog "Error: $_"
		exit
	}
}

try
{
    
    #Get windows logins of current shift
    $ADGroup = (Get-ADGroupMember -Server $DC -Identity $OnCallGroup | Where objectClass -eq 'user' | Get-ADUser -Server $DC -Properties SamAccountName).SamAccountName 

    #Get windows logins of next shift from OnCall DB
    $results = Invoke-Sqlcmd -ServerInstance $SqlServer -Database "master" -Query "EXEC Oncall.OnCall.GetCurrent"
    $CurrentShift = ($results | select win_login).win_login + ($results | select mentor_login | where {$_.mentor_login -ne $Null -AND $_.mentor_login -ne ''}).mentor_login

    #Find users to add and to remove from AD group
    $add = $CurrentShift | ?{$ADGroup -notcontains $_}
    $remove = $ADGroup | ?{$CurrentShift -notcontains $_}

    #Find Names and Emails of users that will be added to AD group
    $Fullnames = $results.full_name
    $Emails = foreach($u in $Fullnames) {Get-ADUser -Server $DC -Filter 'Name -like $u' -Properties EmailAddress | select EmailAddress} 
    $Emails = $Emails.EmailAddress

    #Convert objects to Strings
    $ofs = ', '
    $Fullnames = "$Fullnames"
    $ofs = ';'
    $Emails = "$Emails"

    #Add users
    if ($add.Length -gt 0) 
    {
        Write-Host 'Adding:' $add
        Add-ADGroupMember -Server $DC -Identity $OnCallGroup -Members $add -Confirm:$False
        $results = Invoke-Sqlcmd -ServerInstance $SqlServer -Database "master" -Query "EXEC Oncall.OnCall.send_notification @FullNames = '$Fullnames', @Emails = '$Emails'"
    }

    #Remove users  
    if ($remove.Length -gt 0) 
    {
        Write-Host 'Removing:' $remove
        Remove-ADGroupMember -Server $DC -Identity $OnCallGroup -Members $remove -Confirm:$False
    }
    #Write to log
    WriteLog "AD group before: $CurrentShift"
    WriteLog "Added: $add"
    WriteLog "Removed: $remove"
    WriteLog "`n"
}
catch
{
   Write-Host "Error: $Error"
   WriteLog "Error:"
   foreach($err in $Error){WriteLog "Error: $err"}
   WriteLog "`n"
}



# SIG # Begin signature block
# MIINcwYJKoZIhvcNAQcCoIINZDCCDWACAQExCzAJBgUrDgMCGgUAMGkGCisGAQQB
# gjcCAQSgWzBZMDQGCisGAQQBgjcCAR4wJgIDAQAABBAfzDtgWUsITrck0sYpfvNR
# AgEAAgEAAgEAAgEAAgEAMCEwCQYFKw4DAhoFAAQUIfRTlOpe00Di7s97ThAwzqKi
# SLegggrdMIIFDDCCAvSgAwIBAgITFwAAAANoHQGylzOSswAAAAAAAzANBgkqhkiG
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
# DjAMBgorBgEEAYI3AgEVMCMGCSqGSIb3DQEJBDEWBBS3JwrhmRr0DiX47d3V1+fD
# TQP/4jANBgkqhkiG9w0BAQEFAASCAQBy/sQxs08vtIFVr9jzmkwAKvhM2c19k7mT
# ArLq5SqzeK4ZdbpezlIDX9w9XXTvO5J19awo98KpvbHinlSUqxCT7xKDywcpELUs
# kFYhL8BnUbaPK+XprcX/l2tJF7SjHY31yHlgMDH1kcszBTr7QRUkk/uekoiiFY1d
# 2gW/EDvG/MPZzWwnDlK3GGu2/O56Bt28wzIcrd0JE+i5gOjg8fXN1tx0Kjk174q9
# H2DRryiDD2SVoXmTy4AJVPpXR7xeA927wi6a/fQAxnYTlijyMHIRzT5WLeKDDs3A
# kFHOBOClgkrGzJ9sy3vhh64fbpyRKafIm0lQNfuBoIpVjzav7qda
# SIG # End signature block
