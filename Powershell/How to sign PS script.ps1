#download and import the cert on the server
#check password in password state
Import-PfxCertificate -FilePath "\\iomfs01\group\Software Dev\BORIS\Migration\code signing certificate\corporate_it.pfx" -CertStoreLocation Cert:localmachine\trustedpublisher\ -Password (ConvertTo-SecureString -String '***' -AsPlainText -Force)


# sign : (example)e
$file = "C:\Code\FILENAME.ps1"
$cert=(dir cert:localmachine\trustedpublisher\ -CodeSigningCert)
Set-AuthenticodeSignature $file $cert