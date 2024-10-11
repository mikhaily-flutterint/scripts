# Get all merge commits with format: hash, author, date, sybject, body
git log --first-parent --merges --pretty=format:"%h    %aN    %C(white)%<(5)%aI%Creset %C(red bold)%<(5)%d%Creset %s    %b" > C:\temp\gitlog.csv

# v2
git log --first-parent --merges --pretty=format:"%h ;%aN ;%C(white)%<(5)%aI ;%Creset%s ;%<(40,trunc)%b" > C:\temp\gitlog.csv

#v3 - extract required fields with Powershell 
$repo = "REPO_NAME"
$file = "FILE_PATH.csv"

(gh pr list --repo $repo --state merged --base master --json title,headRefName,mergedAt,mergeStateStatus,state	 -L 500 | ConvertFrom-Json) |
    ForEach-Object {
        [PsCustomObject]@{
            'title' = $_.title
            'headRefName' = $_.headRefName
            'mergedAt' = $_.mergedAt
			'mergeStateStatus' = $_.mergeStateStatus
			'state' = $_.state
        }
    } | Export-Csv -Path $file -NoTypeInformation -Force 
