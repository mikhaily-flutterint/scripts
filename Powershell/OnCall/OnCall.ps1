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

    #Get windows logins of next shift from OnCall DB. Filter out nulls and empty strings
    $results = Invoke-Sqlcmd -ServerInstance $SqlServer -Database "master" -Query "EXEC Oncall.OnCall.GetCurrent"
    $CurrentShift = (($results | select win_login).win_login + ($results | select mentor_login).mentor_login) | Where {$_}

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
    if ([bool]$add) 
    {
        Write-Host "Adding: $add ;"
        Add-ADGroupMember -Server $DC -Identity $OnCallGroup -Members $add -Confirm:$False
        $results = Invoke-Sqlcmd -ServerInstance $SqlServer -Database "master" -Query "EXEC Oncall.OnCall.send_notification @FullNames = '$Fullnames', @Emails = '$Emails'"
    }

    #Remove users  
    if ([bool]$remove) 
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
   Write-Host "Error: $PSItem.Exception.Message"
   WriteLog "Error: $PSItem.Exception.Message"   
}
