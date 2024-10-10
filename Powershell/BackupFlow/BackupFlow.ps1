param(
    [parameter(Mandatory = $False)][string] $TargetServer
)

if ($null -eq $TargetServer -or $TargetServer -eq "" -or !$TargetServer)
{$TargetServer = $env:computername}

$BFDB = 'DBA' # hosting database of the BackupFlow process
$CommandType = 'BackupFlow' # used for CommandLog logging


function EmailNotification {
    param ([parameter(Mandatory = $true)][string]$ErrVarr,
            [parameter(Mandatory = $true)][string]$NotificationType)
    
    $EmailRecipients = Invoke-DbaQuery -SqlInstance $TargetServer -Query "SELECT key_value
    FROM DBA.dbo.BackupFlow_Metadata
    WHERE 1=1
    AND key_code = 'mail'
    AND key_description = @NotificationType;" -As SingleValue -SqlParameters @{"NotificationType" = $NotificationType}
    
    Invoke-DbaQuery -SqlInstance $TargetServer -Query "
            DECLARE @V_Body NVARCHAR(MAX) = CAST(@TheErrorMessage as NVARCHAR(MAX))
            DECLARE @V_Profile sysname = 'IOMMTA01'
            DECLARE @V_Subject NVARCHAR(MAX) = 'BackupFlow: Error detected for ' + @@SERVERNAME
            EXEC msdb.dbo.sp_send_dbmail
            @profile_name = @V_Profile
            , @recipients = @V_Recipients
            , @subject = @V_Subject
            , @body = @V_Body
            , @body_format = 'HTML'
            , @importance = 'High';" -SqlParameters @{"TheErrorMessage" = $ErrVarr;"V_Recipients"=$EmailRecipients;}
        
        Clear-Variable -Name EmailRecipients
    }


function book_NextQueuedBackup {
    Invoke-DbaQuery -SqlInstance $TargetServer -Database $BFDB -Query "
    SET NOCOUNT ON
    DECLARE @UpdatedRows TABLE (ID BIGINT NOT NULL)
    
    IF (SELECT CAST(key_value AS INT) key_value FROM dbo.BackupFlow_Metadata WHERE key_code = 'LP') = 0
    OR (SELECT CAST(key_value AS INT) key_value FROM dbo.BackupFlow_Metadata WHERE key_code = 'LP') IS NULL
    BEGIN 
        -- No log priority
        UPDATE DBA.dbo.BackupFlow_Queue
        SET is_selected = 1
        OUTPUT Inserted.ID
        INTO @UpdatedRows
        FROM (
        SELECT TOP(1) ID FROM DBA.dbo.BackupFlow_Queue
        WHERE 1=1
        AND is_selected = 0 
        AND status = 'queued'
        AND unqueue_time IS NULL
        ORDER BY current_window_start, backup_delta, queue_time) qualified
        WHERE BackupFlow_Queue.ID = qualified.ID  
        SELECT ID FROM @UpdatedRows
    END
    ELSE
    BEGIN
        -- log priority
        DECLARE @booked_ID BIGINT
        UPDATE DBA.dbo.BackupFlow_Queue
        SET is_selected = 1
        OUTPUT Inserted.ID
        INTO @UpdatedRows
        FROM (
        SELECT TOP(1) ID FROM DBA.dbo.BackupFlow_Queue
        WHERE 1=1
        AND backup_type = 'L'
        AND is_selected = 0 
        AND status = 'queued'
        AND unqueue_time IS NULL
        ORDER BY current_window_start, backup_delta, queue_time) qualified
        WHERE BackupFlow_Queue.ID = qualified.ID  
        SET @booked_ID = (SELECT ID FROM @UpdatedRows)

        -- if there are no log backups queued -> get next best
        IF @booked_ID IS NULL
        BEGIN
            UPDATE DBA.dbo.BackupFlow_Queue
            SET is_selected = 1
            OUTPUT Inserted.ID
            INTO @UpdatedRows
            FROM (
            SELECT TOP(1) ID FROM DBA.dbo.BackupFlow_Queue
            WHERE 1=1
            AND is_selected = 0 
            AND status = 'queued'
            AND unqueue_time IS NULL
            ORDER BY current_window_start, backup_delta, queue_time) qualified
            WHERE BackupFlow_Queue.ID = qualified.ID
            SET @booked_ID = (SELECT ID FROM @UpdatedRows)
        END
        
        SELECT @booked_ID
        END" -As SingleValue}
# NextQueuedBackup priority is as follows "current_window_start, backup_delta, queue_time"
# NextQueuedBackup searches for is_select "is_selected = 0 AND status = 'Queued' AND unqueue_time IS NULL
function do_dbcc_checkdb {
    param ([parameter(Mandatory = $true)][string]$TargetServer,
            [parameter(Mandatory = $true)][string]$ps_database_name)
    Invoke-DbaQuery -SqlInstance $TargetServer -Query "
	SET NOCOUNT ON
    DECLARE @SQL2Execute NVARCHAR(MAX)
    DECLARE @ErrorMessage NVARCHAR(MAX)
    DECLARE @StartTime DATETIME, @EndTime DATETIME
	DECLARE @IDList NVARCHAR(MAX)

    DECLARE @V_DBCC_ResultTable TABLE
    (Error INT NULL
    , Level INT NULL
    , State INT NULL
    , MessageText VARCHAR(MAX) NULL
    , RepairLevel NVARCHAR(MAX) NULL
    , Status INT NULL
    , DbId INT NULL
    , DbFragId INT NULL
    , ObjectId BIGINT NULL
    , IndexId BIGINT NULL
    , PartitionID BIGINT NULL
    , AllocUnitID BIGINT NULL
    , RidDbid INT NULL
    , RidPruid INT NULL
    , [File] INT NULL
    , Page INT NULL
    , Slot INT NULL
    , RefDbId INT NULL
    , RefPruId INT NULL
    , RefFile INT NULL
    , RefPage INT NULL
    , RefSlot INT NULL
    , Allocation INT NULL);

    SET @SQL2Execute = (N'DBCC CHECKDB(''' + @V_DatabaseName + N''') WITH TABLERESULTS, NO_INFOMSGS, ALL_ERRORMSGS');

    SET @StartTime = GETDATE();
    INSERT INTO @V_DBCC_ResultTable
    ( Error
    , Level
    , State
    , MessageText
    , RepairLevel
    , Status
    , DbId
    , DbFragId
    , ObjectId
    , IndexId
    , PartitionID
    , AllocUnitID
    , RidDbid
    , RidPruid
    , [File]
    , Page
    , Slot
    , RefDbId
    , RefPruId
    , RefFile
    , RefPage
    , RefSlot
    , Allocation)
    EXEC sp_executesql @SQL2Execute;
    SET @EndTime = GETDATE();

	IF EXISTS (SELECT * FROM @V_DBCC_ResultTable)
    BEGIN

    DECLARE @UpdatedRows TABLE (ID BIGINT NOT NULL)

    INSERT INTO DBA.dbo.CommandLog
    ( DatabaseName
    , Command
    , CommandType
    , StartTime
    , EndTime
    , ErrorNumber
    , ErrorMessage)
		OUTPUT Inserted.ID
		INTO @UpdatedRows
    SELECT
      @V_DatabaseName
    , @SQL2Execute
    , N'DBCC_CHECKDB'
    , @StartTime
    , @EndTime
    , Error
    , MessageText
    FROM @V_DBCC_ResultTable;

	SET @IDList = (SELECT ',', ID AS [text()] FROM @UpdatedRows WHERE 1 = 1 FOR XML PATH(''));

    SET @ErrorMessage = 'Error Message: ' + (SELECT TOP (1) MessageText AS Result FROM @V_DBCC_ResultTable WHERE Error = 8989) + ('; For More information - SELECT * FROM ' + @@SERVERNAME + '.DBA.dbo.CommandLog WHERE ID IN (' + SUBSTRING(@IDList, 2, LEN(@IDList)) + ')')

    END;
    ELSE
    BEGIN
    INSERT INTO DBA.dbo.CommandLog
    ( DatabaseName
    , Command
    , CommandType
    , StartTime
    , EndTime
    , ErrorNumber
    , ErrorMessage)
    VALUES
    (@V_DatabaseName, @SQL2Execute, N'DBCC_CHECKDB', @StartTime, @EndTime, 0, NULL);
    SET @ErrorMessage = 7;
    END;
    SELECT @ErrorMessage as ErrorMessage" -SqlParameters @{"V_DatabaseName" = $ps_database_name} -QueryTimeout 20000 -As SingleValue
}

do {
$BackupQueueID = book_NextQueuedBackup
IF($null -ne $BackupQueueID){

Invoke-DbaQuery -SqlInstance $TargetServer -Query "
UPDATE DBA.dbo.BackupFlow_Queue
SET status = 'Booked'
WHERE ID = @BackupQueueID" -SqlParameters @{"BackupQueueID"=$BackupQueueID}

$NQB = Invoke-DbaQuery -SqlInstance $TargetServer -Query "
SELECT database_name
     , backup_type
     , backup_rule
     , backup_location
     , current_window_start
     , current_window_end
     , blocksize
     , buffercount
     , maxtransfersize
     , do_dbcc
     , max_failure_count
     FROM DBA.dbo.BackupFlow_Queue
     WHERE ID = @BackupQueueID" -SqlParameters @{"BackupQueueID"=$BackupQueueID} -As DataTable

$Now = Get-Date
if ($NQB.current_window_end -gt $Now){

   #do DBCC if required
if ($NQB.do_dbcc -eq 1) {
    Invoke-DbaQuery -SqlInstance $TargetServer -Query "
    UPDATE DBA.dbo.BackupFlow_Queue
    SET status = 'in_dbcc'
    WHERE ID = @BackupQueueID" -SqlParameters @{"BackupQueueID"=$BackupQueueID}
    $get_dbcc_checkdb_result = do_dbcc_checkdb -TargetServer $TargetServer -ps_database_name $NQB.database_name
    #send Notification if DBCC has failed, 7 = Success; Result passed from function do_dbcc_checkdb;
if ($get_dbcc_checkdb_result -ne 7) {
    EmailNotification -ErrVarr $get_dbcc_checkdb_result -NotificationType "general_notification"}
    Clear-Variable -Name get_dbcc_checkdb_result
    }

    #get the backup query
    $backup_query = Invoke-DbaQuery -SqlInstance $TargetServer -Database $BFDB -Query "SELECT key_value FROM DBA.dbo.BackupFlow_Metadata
    WHERE key_code = @ps_backup_rule" -SqlParameters @{"ps_backup_rule"=$NQB.backup_rule} -As SingleValue
    #assign a default backup query if none is found / specified
    if ($null -eq $backup_query) {
        $disable_this_after_that = 1
        $disable_this_after_that_reason = 'no_backup_rule'
        #backup query not configured
        $backup_query_not_configured = "No backup query of type " + $NQB.backup_rule + " for database "+ $NQB.database_name +" is configured in dbo.BackupFlow_Meta;
        Default full backup will be used instead;"
        EmailNotification -ErrVarr $backup_query_not_configured -NotificationType "general_notification"
        #use default backup query
        $backup_query = Invoke-DbaQuery -SqlInstance $TargetServer -Database $BFDB -Query "SELECT key_value FROM DBA.dbo.BackupFlow_Metadata
        WHERE key_code = 'DEF'" -As SingleValue
        # Insert error into dba.CommandLog
        $Now = Get-Date
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
        ( @ps_database_name
        , 'Get backup query'
        , @ps_command_type
        , @ps_StartTime
        , @ps_StartTime
        , 223
        , @ps_backup_query_not_configured)" -SqlParameters @{"ps_database_name"=$NQB.database_name;"ps_command_type"=$CommandType;"ps_StartTime"=$Now;"ps_backup_query_not_configured"=$backup_query_not_configured}
    }

    # initiate with clear failure count
    $FailureCount = 0
    $backup_status = 'IDK'
DO {
    try {
        Invoke-DbaQuery -SqlInstance $TargetServer -Database $BFDB -Query $backup_query -SqlParameters @{"ps_database_name"=$NQB.database_name;"ps_backup_location"=$NQB.backup_location;"ps_blocksize"=$NQB.blocksize;"ps_buffercount"=$NQB.buffercount;"ps_maxtransfersize"=$NQB.maxtransfersize} -QueryTimeout 20000 -WarningVariable WarningVarriable -WarningAction SilentlyContinue
        }
    finally{
        if ($null -eq $WarningVarriable -or $WarningVarriable -eq "" -or !$WarningVarriable)
        {#backup was okay
        $backup_status = 'Okay7'
        }#backup was okay
        else {#backup was NOT okay
            $FailureCount++

            $failure_status = "fail "+$FailureCount
            Invoke-DbaQuery -SqlInstance $TargetServer -Query "
            UPDATE DBA.dbo.BackupFlow_Queue
            SET status = @ps_failure_status
            WHERE ID = @BackupQueueID" -SqlParameters @{"BackupQueueID"=$BackupQueueID;"ps_failure_status"=$failure_status}


             }#backup was NOT okay

    }
} until ($backup_status -eq "Okay7" -or $FailureCount -eq $NQB.max_failure_count )


if ($backup_status -eq "Okay7")
{
    #do the post-success stuff
    $unque_time = Get-Date
    Invoke-DbaQuery -SqlInstance $TargetServer -Query "
    UPDATE DBA.dbo.BackupFlow_Queue
    SET status = 'Completed', unqueue_time = @ps_unqueue_time, is_selected = 0
    WHERE ID = @BackupQueueID" -SqlParameters @{"BackupQueueID"=$BackupQueueID;"ps_unqueue_time"=$unque_time}
}

if ($FailureCount -eq $NQB.max_failure_count)
{
    #do the stuff when it needs to be disabled
    $unque_time = Get-Date
    #Update the queue table stating max failures were done
    Invoke-DbaQuery -SqlInstance $TargetServer -Database $BFDB -Query "
    UPDATE dbo.BackupFlow_Queue
    SET status = 'max_failure', unqueue_time = @ps_unqueue_time, is_selected = 0
    WHERE ID = @BackupQueueID" -SqlParameters @{"BackupQueueID"=$BackupQueueID;"ps_unqueue_time"=$unque_time}

    #send email that backup is disabled
    $err_proc_disabled = 'Backup for database ' + $NQB.database_name + ' is now disabled due to hitting its configured max_failure_count number: ' + $NQB.max_failure_count + `
    '; The last failure error is :' + $WarningVarriable
    EmailNotification -ErrVarr $err_proc_disabled -NotificationType "general_notification"

    #disable the process untill enabled again
    Invoke-DbaQuery -SqlInstance $TargetServer -Query "
    UPDATE DBA.dbo.BackupFlow_Repo
    SET status = 'Disabled'
    WHERE database_name = @ps_database_name
    and backup_type = @ps_backup_type
    and backup_rule = @ps_backup_rule" -SqlParameters @{"ps_database_name"=$NQB.database_name;"ps_backup_type"=$NQB.backup_type;"ps_backup_rule"=$NQB.backup_rule}

    # add to the log
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
    ( @ps_database_name
    , @ps_backup_query
    , @ps_command_type
    , @ps_unque_time
    , @ps_unque_time
    , 222
    , @ps_proc_disabled)" -SqlParameters @{"ps_database_name"=$NQB.database_name;"ps_backup_query"=$backup_query;"ps_command_type"=$CommandType;"ps_unque_time"=$unque_time;"ps_proc_disabled"=$err_proc_disabled}

}

if ($disable_this_after_that -eq 1)
{

    #disable the process untill enabled again
    Invoke-DbaQuery -SqlInstance $TargetServer -Query "
    UPDATE DBA.dbo.BackupFlow_Repo
    SET status = 'Disabled'
    WHERE database_name = @ps_database_name
    and backup_type = @ps_backup_type
    and backup_rule = @ps_backup_rule" -SqlParameters @{"ps_database_name"=$NQB.database_name;"ps_backup_type"=$NQB.backup_type;"ps_backup_rule"=$NQB.backup_rule}

    if($disable_this_after_that_reason -eq 'no_backup_rule')
    {$err_proc_disabled = 'Backup for database ' + $NQB.database_name + ' is now disabled; There is no backup_rule set in dbo.BackupFlow_Metadata'}

    EmailNotification -ErrVarr err_proc_disabled -NotificationType "general_notification"
    Clear-Variable -Name "err_proc_disabled", "disable_this_after_that"
}

    Clear-Variable WarningVarriable
    Clear-Variable FailureCount
    Clear-Variable -Name "backup_status", "FailureCount", "backup_query", "unque_time"

} # IF ($NQB.current_window_end -gt $Now) 
else {
    # TODO add a row to command log that the timeframe was not enough

    #unqueue due to out of window
    $unque_time = Get-Date
    Invoke-DbaQuery -SqlInstance $TargetServer -Query "
    UPDATE DBA.dbo.BackupFlow_Queue
    SET status = 'out_of_window', unqueue_time = @ps_unqueue_time, is_selected = 0
    WHERE ID = @BackupQueueID" -SqlParameters @{"BackupQueueID"=$BackupQueueID;"ps_unqueue_time"=$unque_time}

    #add to the log that the time was not enough
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
    ( @ps_database_name
    , @ps_command_type
    , @ps_command_type
    , @ps_unque_time
    , @ps_unque_time
    , 231
    , 'Predefined window was not enough;')" -SqlParameters @{"ps_database_name"=$NQB.database_name;"ps_command_type"=$CommandType;"ps_unque_time"=$unque_time;}
}
} # IF ($null -ne $BackupQueueID)
else {break}
} while ($null -ne $BackupQueueID)
# SIG # Begin signature block
# MIINcwYJKoZIhvcNAQcCoIINZDCCDWACAQExCzAJBgUrDgMCGgUAMGkGCisGAQQB
# gjcCAQSgWzBZMDQGCisGAQQBgjcCAR4wJgIDAQAABBAfzDtgWUsITrck0sYpfvNR
# AgEAAgEAAgEAAgEAAgEAMCEwCQYFKw4DAhoFAAQUAOxYGqkmDtVj6HpCyLAHfMgL
# GWCgggrdMIIFDDCCAvSgAwIBAgITFwAAAANoHQGylzOSswAAAAAAAzANBgkqhkiG
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
# DjAMBgorBgEEAYI3AgEVMCMGCSqGSIb3DQEJBDEWBBQQlQc3vCgXQe64z6VdqXq+
# MAd8hTANBgkqhkiG9w0BAQEFAASCAQAgvMPE/C8bjyJSnhED1RTUvfe1EvRbctuP
# cPtBQ7oFBaDoioMLaQxLzfk9QDsBHUedTyTAppQgLMlHksWyqonzeo2L4+OjnhEF
# wiV8UavXI+FID5RFDxdgGM/Jxt1Bd795w5HQ4KNcVcJ0L5yZdtDeemkwdg28rPG7
# 6X+Bd/BAC+0lP6m5qZaCNQCfBkQNkF0azkcCGdsOF9JE+z/Rk8OuOnrL1QzSQXp6
# HyD0yIGb39wnBtACC6J3wBy+zsz7FyjgS07OvDdSHmLACC/v+RTKZaZIl8EZPzTG
# 0zruPeQpkOFN8S+QBT9Bw5QLxjRS2QcfW0hkfaO2z3I5Kof4tJLu
# SIG # End signature block
