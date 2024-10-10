param($Action,$SourceServer,$TargetServer,$Database,$NewDBName)

##$SourceServer = "IMDGNDWSQLD10"
##$TargetServer= "IMDGNDWSQLP79\DEPLUAT"
##$Database    = "BORE"
$CloneName   = $Database+"_Clone"

if (-not($Action -in "DetachAttach","BackupRestore"))
{
  throw "Invalid -Action parameter"
}

if ($Action -eq "DetachAttach")
{
    Write-Output ("Attach clone of " + $Database + " from " + $SourceServer)
    ## 1. Clone DB (schema only) on Sorce
    Invoke-DbaDbClone -SqlInstance $SourceServer -Database $Database -CloneDatabase $CloneName -ExcludeStatistics -ExcludeQueryStore 
    Set-DbaDbState -SqlInstance $SourceServer -Database $CloneName -ReadWrite -Force

    ## 2. Copy Clone to Target server with original name. Re-write if DB exists 
    Copy-DbaDatabase -Source $SourceServer -Destination $TargetServer -Database $CloneName -NewName $NewDBName -DetachAttach -Reattach -Force

    ## 3. Drop Clone
    Remove-DbaDatabase -SqlInstance $SourceServer -Database $CloneName -WhatIf:$false -Confirm:$false
}

if ($Action -eq "BackupRestore")
{
    $Size =  Get-DbaDatabase -SqlInstance $SourceServer -Database $Database

    if ($Size.SizeMB -gt 35000)
    {
        throw ("DB " + $Database + "is too big for " + $Action)
    }

    Write-Output ("Resrore " + $Database + " from " + $SourceServer)
    Copy-DbaDatabase -Source $SourceServer -Destination $TargetServer -Database $Database -NewName $NewDBName -BackupRestore -UseLastBackup -WithReplace
}