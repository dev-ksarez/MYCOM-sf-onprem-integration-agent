# Release 0.2.19

- Hotfix fuer Windows PowerShell: Die Rollen-Normalisierung in `install-agent-service.ps1` ist jetzt komplett pipeline-frei implementiert
- `update-existing-installation.cmd` und die anschliessende Dienstemigration koennen damit auch unter aelteren Windows-PowerShell-Versionen ohne Parserfehler laufen
- Die Trennung der Windows-Rollen sowie die bisherigen `nssm.exe`-Migrationsfixes bleiben enthalten
