Clear-Host
#ControlServer
$ControlServer = "IMDGNDWADM10"
Invoke-DbaQuery -SqlInstance $ControlServer -Query 'EXEC DisasterRecovery.dbo.dbcc_check_for_stuck_controllers'

#Book a controller _old _removed at 27 12 2019
# $Controller_ID = Invoke-DbaQuery -SqlInstance $ControlServer -Query 'DECLARE @ID INT = (SELECT TOP (1) ID
#                    FROM DisasterRecovery.dbo.dbcc_controllers
#                    WHERE
#                    Enabled = 1
#                    AND Status = 0);
# IF @ID IS NULL
# SET @ID = 0;
# ELSE
# BEGIN
# UPDATE DisasterRecovery.dbo.dbcc_controllers
# SET Status = 1
# WHERE ID = @ID;
# END;
# SELECT @ID AS CurrentController' -As SingleValue



#Book a controller _new added at 27 12 2019
$Controller_ID = Invoke-DbaQuery -SqlInstance $ControlServer -Query 'SET NOCOUNT ON
DECLARE @UpdatedRows TABLE (ID INT NOT NULL)

UPDATE DisasterRecovery.dbo.dbcc_controllers
SET Status = 1
OUTPUT Inserted.ID
INTO @UpdatedRows
FROM (
	SELECT TOP(1) ID FROM DisasterRecovery.dbo.dbcc_controllers
	WHERE 1=1
	AND Enabled		= 1
	AND Status		= 0
) qualified
WHERE dbcc_controllers.ID = qualified.ID 

IF (SELECT ID FROM @UpdatedRows) IS NULL
BEGIN
	SELECT 0 AS CurrentController
END
ELSE
BEGIN
	SELECT ID AS CurrentController FROM @UpdatedRows
END' -As SingleValue





if ($Controller_ID -ne 0)
{
## There is free controller
Write-Host 'There is free controller!'

$Workset = Invoke-DbaQuery -SqlInstance $ControlServer -Query "DECLARE @dbCTE TABLE
(ID INT NOT NULL
, DatabaseName sysname NOT NULL
, SqlInstance sysname NOT NULL
, OriginalSqlInstance sysname NOT NULL);
WITH dbCTE
AS (SELECT TOP (1)
    RL.ID
    , RL.RestoreDatabaseName AS DatabaseName
    , RL.RestoreServerName AS SqlInstance
    , RL.ServerName AS OriginalSqlInstance
    FROM DisasterRecovery.dbo.restore_log RL
    WHERE
    RL.DBCC_Status = 0
    AND RL.EndTime IS NOT NULL
    AND RL.RestoreDatabaseName IS NOT NULL
    AND RL.RestoreServerName IS NOT NULL
    ORDER BY RL.ID ASC)
INSERT INTO @dbCTE
SELECT
ID
, DatabaseName
, SqlInstance
, OriginalSqlInstance
FROM dbCTE;
IF EXISTS (SELECT TOP (1) ID FROM @dbCTE)
BEGIN

UPDATE RL
SET RL.DBCC_Status = 1
FROM DisasterRecovery.dbo.restore_log RL
  JOIN @dbCTE D4
    ON RL.ID = D4.ID
WHERE RL.ID = D4.ID;

UPDATE DC
SET
DC.database_name = D4.DatabaseName
, DC.SqlInstance = D4.SqlInstance
, DC.StartTime = GETDATE()
FROM DisasterRecovery.dbo.dbcc_controllers DC
  JOIN @dbCTE D4
    ON 1 = 1
WHERE DC.ID = @Controller_ID;

END;

SELECT
dbCTE.ID
, dbCTE.DatabaseName
, dbCTE.SqlInstance
, dbCTE.OriginalSqlInstance
FROM @dbCTE dbCTE
  JOIN DisasterRecovery.dbo.dbcc_controllers DC
    ON 1 = 1
WHERE DC.ID = @Controller_ID;" -As DataTable -SqlParameters @{"Controller_ID"=$Controller_ID}
 
    if ($Workset -ne $null) {
    Write-Host "There is even a db to work on !!"

    $StartTime = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'

    $WorksetResult = Invoke-DbaQuery -SqlInstance $Workset.SqlInstance -Query "
    DECLARE @HistoryTableResults TABLE
    ([Error] [INT] NULL
    , [Level] [INT] NULL
    , [State] [INT] NULL
    , [MessageText] [VARCHAR](MAX) NULL
    , [RepairLevel] [NVARCHAR](MAX) NULL
    , [Status] [INT] NULL
    , [DbId] [INT] NULL
    , [DbFragId] [INT] NULL
    , [ObjectId] [BIGINT] NULL
    , IndexId [BIGINT] NULL
    , [PartitionID] [BIGINT] NULL
    , [AllocUnitID] [BIGINT] NULL
    , [RidDbid] [INT] NULL
    , [RidPruid] [INT] NULL
    , [File] [INT] NULL
    , [Page] [INT] NULL
    , [Slot] [INT] NULL
    , [RefDbId] [INT] NULL
    , [RefPruId] [INT] NULL
    , [RefFile] [INT] NULL
    , [RefPage] [INT] NULL
    , [RefSlot] [INT] NULL
    , [Allocation] [INT] NULL);
    DECLARE @DatabaseName sysname = @WorksetDatabaseName
    DECLARE @SQL2Execute NVARCHAR(MAX) = 'DBCC CHECKDB(''' + @DatabaseName + ''') WITH TABLOCK, TABLERESULTS, NO_INFOMSGS, ALL_ERRORMSGS'
    DECLARE @ErrorMessage NVARCHAR(MAX)
    DECLARE @StartTime DATETIME = GETDATE()
    DECLARE @EndTime DATETIME

    INSERT INTO @HistoryTableResults
    (Error, Level, State, MessageText, RepairLevel, Status, DbId, DbFragId, ObjectId, IndexId, PartitionId, AllocUnitId, RidDbId, RidPruId, [File], Page, Slot, RefDbId, RefPruId, RefFile, RefPage, RefSlot, Allocation)
    EXEC sp_executesql @SQL2Execute
    SET @EndTime = GETDATE()

    INSERT INTO DBA.dbo.CommandLog
    ( DatabaseName
    , Command
    , CommandType
    , StartTime
    , EndTime
    , ErrorNumber
    , ErrorMessage)
    SELECT
    @DatabaseName
    , @SQL2Execute
    , N'AUTO DBCC CHECKDB'
    , @StartTime
    , @EndTime
    , Error
    , MessageText
    FROM @HistoryTableResults
    
    IF (SELECT COUNT(*) FROM @HistoryTableResults) > 0
    SET @ErrorMessage = (SELECT TOP(1) MessageText AS Result FROM @HistoryTableResults WHERE Error = 8989)
    ELSE
    SET @ErrorMessage = 'Okay7'
    
    SELECT @ErrorMessage AS Result, @StartTime AS StartTime, @EndTime AS EndTime;" -As DataTable -QueryTimeout 288000 -SqlParameters @{"WorksetDatabaseName"=$Workset.DatabaseName} -ErrorVariable ErrVarr -WarningVariable WarningVar

if ($ErrorVar -or $WarningVar) {$ErrorMessage = $WarningVar + $ErrorVar | Out-String}
else {$ErrorMessage = $WorksetResult.Result}

    if (($WorksetResult.Result) -eq 'Okay7')
    {
      Write-Host "Result is Okay7!!!"
      #Update restore_log dbcc_result to be 7
      Invoke-DbaQuery -SqlInstance $ControlServer -Query "UPDATE DisasterRecovery.dbo.restore_log 
      SET DBCC_Status = 7
      WHERE ID = @restore_log_ID" -SqlParameters @{"restore_log_ID"=$Workset.ID}
      }
      else {
        Write-Host "Result is Bad2 :("
            #Send failure email!
            Invoke-DbaQuery -SqlInstance $ControlServer -Query "
            DECLARE @V_Body NVARCHAR(MAX) = ('Database ' + CAST(@DatabaseName AS NVARCHAR(256)) + ' on server ' + CAST(@ServerName AS NVARCHAR(256)) 
            + ' from server ' + CAST(@OriginalSqlInstance AS NVARCHAR(256))
            + ' has failed a DBCC Check!'
            + CHAR(13)+CHAR(10) + 'Error message: ' + CAST(@TheErrorMessage as NVARCHAR(MAX)))
            DECLARE @V_Profile sysname = (CAST((SELECT TOP(1) ValueCHAR FROM DisasterRecovery.dbo.dbcc_config WHERE Control = 'db_mail_profile') AS sysname))
            DECLARE @V_Recipients NVARCHAR(MAX) = (SELECT TOP(1) ValueCHAR FROM DisasterRecovery.dbo.dbcc_config WHERE Control = 'db_mail_recipients')
            DECLARE @V_Importance VARCHAR(6) = 'High'

            EXEC msdb.dbo.sp_send_dbmail
            @profile_name = @V_Profile
            , @recipients = @V_Recipients
            , @subject = N'DBCC Automation: Integrity check resulted in failure!'
            , @body = @V_Body
            , @importance = @V_Importance;" -SqlParameters @{"DatabaseName"=$Workset.DatabaseName;
                                                              "ServerName"=$Workset.SqlInstance;
                                                              "OriginalSqlInstance"=$Workset.OriginalSqlInstance
                                                              "TheErrorMessage" =$ErrorMessage}

            #Update dbcc_result to be 2
            Invoke-DbaQuery -SqlInstance $ControlServer -Query "UPDATE DisasterRecovery.dbo.restore_log 
            SET DBCC_Status = 2
            WHERE ID = @restore_log_ID" -SqlParameters @{"restore_log_ID"=$Workset.ID}
      }

      if ($WorksetResult.StartTime -ne $null){$StartTime = $WorksetResult.StartTime}
      if ($WorksetResult.EndTime -eq $null) {$EndTime = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'}
      else {$EndTime = $WorksetResult.EndTime}

      ## Update dbcc_log
      Invoke-DbaQuery -SqlInstance $ControlServer -Query "INSERT INTO DisasterRecovery.dbo.dbcc_log
            (
              DatabaseName
            , ServerName
            , OriginalSqlInstance
            , StartTime
            , EndTime
            , ErrorMessage
            )
            VALUES
            ( @DatabaseName
            , @ServerName
            , @OriginalSqlInstance
            , @StartTime
            , @EndTime
            , @TheErrorMessage
            )" -SqlParameters @{
              "DatabaseName"        =$Workset.DatabaseName;
              "ServerName"          =$Workset.SqlInstance;
              "OriginalSqlInstance" =$Workset.OriginalSqlInstance;
              "StartTime"           =$StartTime;
              "EndTime"             =$EndTime;
              "TheErrorMessage"     =$ErrorMessage
            }
    }

    #Update controller to be free
    Invoke-DbaQuery -SqlInstance $ControlServer -Query "UPDATE DisasterRecovery.dbo.dbcc_controllers
    SET Status = 0,
    database_name = NULL,
    SqlInstance = NULL,
    connection_string = NULL,
    StartTime = NULL
    WHERE ID = @Controller_ID" -SqlParameters @{"Controller_ID"=$Controller_ID}
}
else {Write-Host 'There are no free controllers :('}

# SIG # Begin signature block
# MIINcwYJKoZIhvcNAQcCoIINZDCCDWACAQExCzAJBgUrDgMCGgUAMGkGCisGAQQB
# gjcCAQSgWzBZMDQGCisGAQQBgjcCAR4wJgIDAQAABBAfzDtgWUsITrck0sYpfvNR
# AgEAAgEAAgEAAgEAAgEAMCEwCQYFKw4DAhoFAAQUHCQHkgqBAa/xJFIkhzXqejNF
# ODqgggrdMIIFDDCCAvSgAwIBAgITFwAAAANoHQGylzOSswAAAAAAAzANBgkqhkiG
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
# DjAMBgorBgEEAYI3AgEVMCMGCSqGSIb3DQEJBDEWBBR3qi7yRegi5ISLz9pdw82G
# gkoYujANBgkqhkiG9w0BAQEFAASCAQAg92aFM1VBVetxqjgoemwS4TnjHVMOA/Mq
# iLAp1iqDxwFaozJUuLsCzA+CYhP+JJ/MZNW+a8RcVnk7+ZO5ZiRiz9NuTpKUKMxy
# Z9w8GPnB4M3Sgwp5sUl2OfrhVPps+FuUSFftH8CIgufrT0XUKWeWFNlh+Zoh6Z9D
# nBn+8kQhWQPJpV7RQvx9mbl2ZX51g0sM9w3HXJuiaVN9rAJfpYUQuxsd1cewj3OF
# nu8t2DX9n9y7Bg/olJQdjizW8mGuJQxlXvjoezY2J5TDQ+/z6ZvhdYp6uOJf3dHV
# 528yArz2M9D3EGxjjbsCqW3Xyj1n2hx2i9rBUktywsRjz5echBZf
# SIG # End signature block
