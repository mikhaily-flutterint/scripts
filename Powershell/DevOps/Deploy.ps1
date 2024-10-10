param($ConfigFile, $OnlyPrint=$false)

$json = Get-Content $ConfigFile -Raw 
$config = ConvertFrom-Json -InputObject $json
$Env = $config.Environment
$TargetServer = $config.TargetServer

Write-Output "Building ${Env} on ${TargetServer}"

ForEach ($Server in $config.Servers)
{
    ForEach ($Database in $Server.Databases)
    {
        if ($OnlyPrint -eq $true)
        {
            Write-Output ("-Action " + $Database.Action + " -SourceServer " + $Server.ServerName + " -TargetServer " + $TargetServer + " -Database " + $Database.Name + " -NewDBName " + $Database.NewName)
        }
        else
        {
            .\CloneDB.ps1 -Action $Database.Action -SourceServer $Server.ServerName -TargetServer $TargetServer -Database $Database.Name -NewDBName $Database.NewName
        }
    }
}