param(
  [string]$AppRoot,
  [string]$LogFile,
  [string]$ServiceName = "SfOnpremIntegrationAgent",
  [string]$DisplayName = "SF OnPrem Integration Agent",
  [string]$Description = "Runs the Salesforce On-Prem Integration Agent",
  [string]$TaskName = "SfOnpremIntegrationAgent-Updater",
  [string]$ManifestUrl = "https://github.com/dev-ksarez/MYCOM-sf-onprem-integration-agent/releases/latest/download/update-manifest.json",
  [int]$WebUiPort = 8080,
  [int]$SchedulerIntervalMs = 60000,
  [int]$EveryMinutes = 15,
  [string]$UpdaterTaskUser = "SYSTEM",
  [string]$UpdaterTaskPassword,
  [int]$UpdateLogRetentionDays = 30,
  [switch]$InstallDependencies,
  [switch]$CopyEnvExample,
  [switch]$PromptForEnv,
  [switch]$SkipUpdater,
  [switch]$RunInitialSetup,
  [ValidateSet("SAGE100")]
  [string]$InitialSetupMode = "SAGE100",
  [switch]$ActivateInitialSchedules
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$script:InstallerLogFile = if ($LogFile -and $LogFile.Trim()) { $LogFile.Trim() } else { Join-Path $env:TEMP "sf-onprem-integration-agent-installer.log" }
$script:ScriptDirectory = Split-Path -Parent $PSCommandPath

function Initialize-InstallerLog {
  $directory = Split-Path -Parent $script:InstallerLogFile
  if ($directory -and -not (Test-Path -Path $directory)) {
    New-Item -Path $directory -ItemType Directory -Force | Out-Null
  }

  if (-not (Test-Path -Path $script:InstallerLogFile)) {
    New-Item -Path $script:InstallerLogFile -ItemType File -Force | Out-Null
  }
}

function Write-InstallerLog {
  param(
    [string]$Message,
    [ValidateSet("INFO", "WARN", "ERROR")]
    [string]$Level = "INFO"
  )

  try {
    Initialize-InstallerLog
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss.fff"
    Add-Content -Path $script:InstallerLogFile -Value "[$timestamp] [$Level] $Message"
  }
  catch {
  }
}

function Resolve-AppRoot {
  param([string]$InputPath)

  if ($InputPath -and $InputPath.Trim()) {
    $candidate = $InputPath.Trim()
    if (-not (Test-Path -Path $candidate)) {
      throw "AppRoot path not found: $candidate"
    }

    return (Resolve-Path -Path $candidate).Path
  }

  $scriptDir = $script:ScriptDirectory
  return (Resolve-Path -Path (Join-Path $scriptDir "..\..\")).Path
}

function Ask-YesNo {
  param(
    [string]$Prompt,
    [bool]$Default = $false
  )

  $suffix = if ($Default) { "[Y/n]" } else { "[y/N]" }
  $answer = Read-Host "$Prompt $suffix"

  if (-not $answer) {
    return $Default
  }

  return $answer.Trim().ToLowerInvariant().StartsWith("y")
}

function Test-IsBuiltInTaskAccount {
  param([string]$AccountName)

  if (-not $AccountName) {
    return $true
  }

  $normalized = $AccountName.Trim().ToUpperInvariant()
  return $normalized -in @(
    "SYSTEM",
    "NT AUTHORITY\SYSTEM",
    "LOCAL SERVICE",
    "NT AUTHORITY\LOCAL SERVICE",
    "NETWORK SERVICE",
    "NT AUTHORITY\NETWORK SERVICE"
  )
}

function Test-IsElevated {
  $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
  $principal = New-Object Security.Principal.WindowsPrincipal($identity)
  return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Convert-BoundParametersToArgumentList {
  param([hashtable]$BoundParameters)

  $arguments = @()
  foreach ($entry in $BoundParameters.GetEnumerator()) {
    $key = [string]$entry.Key
    $value = $entry.Value
    if ($value -is [switch]) {
      if ($value.IsPresent) {
        $arguments += "-$key"
      }
      continue
    }

    if ($null -eq $value -or [string]::IsNullOrWhiteSpace([string]$value)) {
      continue
    }

    $arguments += "-$key"
    $arguments += [string]$value
  }

  return ,$arguments
}

function Convert-ArgumentToProcessString {
  param([string]$Value)

  if ($null -eq $Value) {
    return '""'
  }

  return '"' + ($Value -replace '"', '""') + '"'
}

function Ensure-Elevated {
  param([hashtable]$BoundParameters)

  if (Test-IsElevated) {
    Write-InstallerLog -Message "Windows agent installer already running with administrator rights."
    return
  }

  $scriptPath = $PSCommandPath
  if (-not $scriptPath) {
    throw "Die automatische Rechteanhebung ist fehlgeschlagen, weil der Skriptpfad nicht aufgeloest werden konnte."
  }

  $argumentList = @(
    "-NoProfile",
    "-ExecutionPolicy",
    "Bypass",
    "-File",
    $scriptPath
  )
  $argumentList += Convert-BoundParametersToArgumentList -BoundParameters $BoundParameters
  $argumentString = ($argumentList | ForEach-Object { Convert-ArgumentToProcessString -Value $_ }) -join ' '

  Write-Host "Administratorrechte werden angefordert..." -ForegroundColor Yellow
  Write-InstallerLog -Message "Requesting administrator rights via UAC for Windows agent installer."
  try {
    Start-Process -FilePath "powershell.exe" -Verb RunAs -ArgumentList $argumentString -ErrorAction Stop | Out-Null
    Write-InstallerLog -Message "UAC relaunch for Windows agent installer started successfully."
  }
  catch {
    Write-InstallerLog -Level ERROR -Message ("UAC relaunch failed: " + $_.Exception.Message)
    throw
  }
  exit 0
}

function Resolve-CommandPath {
  param(
    [string[]]$Names,
    [string]$Purpose
  )

  foreach ($name in $Names) {
    $command = Get-Command $name -ErrorAction SilentlyContinue
    if ($command -and $command.Source) {
      return $command.Source
    }
  }

  throw "$Purpose was not found. Checked: $($Names -join ', ')"
}

function Invoke-ProcessChecked {
  param(
    [string]$FilePath,
    [string[]]$ArgumentList,
    [string]$WorkingDirectory,
    [string]$ErrorMessage
  )

  Push-Location $WorkingDirectory
  try {
    & $FilePath @ArgumentList
    if ($LASTEXITCODE -ne 0) {
      throw "$ErrorMessage (exit code $LASTEXITCODE)."
    }
  }
  finally {
    Pop-Location
  }
}

function Invoke-ProcessCapture {
  param(
    [string]$FilePath,
    [string[]]$ArgumentList,
    [string]$WorkingDirectory
  )

  Push-Location $WorkingDirectory
  try {
    $output = & $FilePath @ArgumentList 2>&1
    return [pscustomobject]@{
      ExitCode = $LASTEXITCODE
      Output = ($output | Out-String).Trim()
    }
  }
  finally {
    Pop-Location
  }
}

function Convert-SecureStringToPlainText {
  param([Security.SecureString]$SecureString)

  if (-not $SecureString) {
    return ""
  }

  $pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($SecureString)
  try {
    return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer)
  }
  finally {
    if ($pointer -ne [IntPtr]::Zero) {
      [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer)
    }
  }
}

function Get-EnvValueMap {
  param([string]$Path)

  $values = [ordered]@{}
  if (-not (Test-Path -Path $Path)) {
    return $values
  }

  foreach ($line in Get-Content -Path $Path) {
    if ($line -match '^\s*#' -or $line -notmatch '=') {
      continue
    }

    $parts = $line -split '=', 2
    $key = $parts[0].Trim()
    if (-not $key) {
      continue
    }

    $value = if ($parts.Count -gt 1) { $parts[1] } else { "" }
    $values[$key] = $value
  }

  return $values
}

function Set-OrAppendEnvValues {
  param(
    [string]$Path,
    [hashtable]$Updates
  )

  $lines = New-Object System.Collections.Generic.List[string]
  if (Test-Path -Path $Path) {
    foreach ($line in Get-Content -Path $Path) {
      $lines.Add($line)
    }
  }

  foreach ($entry in $Updates.GetEnumerator()) {
    $key = $entry.Key
    $value = [string]$entry.Value
    $replacement = "$key=$value"
    $pattern = '^\s*' + [regex]::Escape($key) + '\s*='
    $replaced = $false

    for ($index = 0; $index -lt $lines.Count; $index += 1) {
      if ($lines[$index] -match $pattern) {
        $lines[$index] = $replacement
        $replaced = $true
        break
      }
    }

    if (-not $replaced) {
      if ($lines.Count -gt 0 -and $lines[$lines.Count - 1] -ne "") {
        $lines.Add("")
      }
      $lines.Add($replacement)
    }
  }

  Set-Content -Path $Path -Value $lines -Encoding UTF8
}

function Read-ConfigValue {
  param(
    [string]$Prompt,
    [string]$DefaultValue = "",
    [switch]$Required,
    [switch]$Secret,
    [ValidateSet("text", "int", "url")]
    [string]$Validation = "text",
    [string]$HelpText = ""
  )

  while ($true) {
    $displayDefault = if ($DefaultValue) { " [$DefaultValue]" } else { "" }
    if ($HelpText) {
      Write-Host "  $HelpText" -ForegroundColor DarkGray
    }
    if ($Secret) {
      $secure = Read-Host "$Prompt$displayDefault" -AsSecureString
      $plain = Convert-SecureStringToPlainText -SecureString $secure
      if (-not $plain) {
        $plain = $DefaultValue
      }
      if ($Required -and -not $plain) {
        Write-Host "  Dieser Wert ist erforderlich." -ForegroundColor Yellow
        continue
      }
      if ($plain -and $Validation -eq "int" -and -not ($plain -match '^\d+$')) {
        Write-Host "  Bitte eine ganze Zahl eingeben." -ForegroundColor Yellow
        continue
      }
      if ($plain -and $Validation -eq "url" -and -not ($plain -match '^https?://')) {
        Write-Host "  Bitte eine URL mit http:// oder https:// eingeben." -ForegroundColor Yellow
        continue
      }
      return $plain
    }

    $answer = Read-Host "$Prompt$displayDefault"
    if (-not $answer) {
      $answer = $DefaultValue
    }
    if ($Required -and -not $answer) {
      Write-Host "  Dieser Wert ist erforderlich." -ForegroundColor Yellow
      continue
    }
    if ($answer -and $Validation -eq "int" -and -not ($answer -match '^\d+$')) {
      Write-Host "  Bitte eine ganze Zahl eingeben." -ForegroundColor Yellow
      continue
    }
    if ($answer -and $Validation -eq "url" -and -not ($answer -match '^https?://')) {
      Write-Host "  Bitte eine URL mit http:// oder https:// eingeben." -ForegroundColor Yellow
      continue
    }
    return $answer
  }
}

function Show-ConfigSection {
  param(
    [string]$Title,
    [string]$Description
  )

  Write-Host "" 
  Write-Host $Title -ForegroundColor Cyan
  Write-Host $Description -ForegroundColor DarkGray
}

function Configure-EnvInteractively {
  param(
    [string]$EnvPath,
    [string]$ExamplePath,
    [int]$DefaultWebUiPort,
    [int]$DefaultSchedulerIntervalMs
  )

  $values = if (Test-Path -Path $EnvPath) { Get-EnvValueMap -Path $EnvPath } else { Get-EnvValueMap -Path $ExamplePath }
  $updates = [ordered]@{}

  Write-Host "Interaktive Konfiguration der Datei .env" -ForegroundColor Cyan
  Write-Host "Die Werte werden als Klartext in $EnvPath gespeichert." -ForegroundColor Yellow

  $defaultAgentId = if ($values.Contains('AGENT_ID')) { [string]$values['AGENT_ID'] } else { [Environment]::MachineName }
  $defaultLogLevel = if ($values.Contains('LOG_LEVEL')) { [string]$values['LOG_LEVEL'] } else { 'info' }
  $defaultWebUiPortValue = if ($values.Contains('WEB_UI_PORT')) { [string]$values['WEB_UI_PORT'] } else { [string]$DefaultWebUiPort }
  $defaultSchedulerInterval = if ($values.Contains('SCHEDULER_INTERVAL_MS')) { [string]$values['SCHEDULER_INTERVAL_MS'] } else { [string]$DefaultSchedulerIntervalMs }

  Show-ConfigSection -Title 'Grundkonfiguration' -Description 'Diese Werte steuern den lokalen Agenten, das Logging und die Web-Oberflaeche.'
  $updates['AGENT_ID'] = Read-ConfigValue -Prompt 'Agentenkennung' -DefaultValue $defaultAgentId -Required -HelpText 'Eindeutiger Name des installierten Agenten, zum Beispiel der Servername.'
  $updates['LOG_LEVEL'] = Read-ConfigValue -Prompt 'Log-Level' -DefaultValue $defaultLogLevel -Required -HelpText 'Empfohlen ist info. Fuer Fehlersuche kann spaeter debug gesetzt werden.'
  $updates['WEB_UI_PORT'] = Read-ConfigValue -Prompt 'Port der Web-Oberflaeche' -DefaultValue $defaultWebUiPortValue -Required -Validation int -HelpText 'Standard ist 8080. Der Port muss auf dem Server frei sein.'
  $updates['SCHEDULER_INTERVAL_MS'] = Read-ConfigValue -Prompt 'Pruefintervall des Schedulers in Millisekunden' -DefaultValue $defaultSchedulerInterval -Required -Validation int -HelpText '60000 bedeutet: alle 60 Sekunden nach faelligen Jobs suchen.'

  if (Ask-YesNo -Prompt 'Soll der Salesforce-Zugang jetzt eingerichtet werden?' -Default $true) {
    Show-ConfigSection -Title 'Salesforce-Zugang' -Description 'Diese Zugangsdaten werden fuer die Verbindung zur Salesforce-Org benoetigt.'
    $defaultLoginUrl = if ($values.Contains('SF_LOGIN_URL')) { [string]$values['SF_LOGIN_URL'] } else { 'https://login.salesforce.com' }
    $defaultClientId = if ($values.Contains('SF_CLIENT_ID')) { [string]$values['SF_CLIENT_ID'] } else { '' }
    $defaultClientSecret = if ($values.Contains('SF_CLIENT_SECRET')) { [string]$values['SF_CLIENT_SECRET'] } else { '' }

    $updates['SF_LOGIN_URL'] = Read-ConfigValue -Prompt 'Salesforce Login-URL' -DefaultValue $defaultLoginUrl -Required -Validation url -HelpText 'Produktion meist https://login.salesforce.com, Sandbox meist https://test.salesforce.com.'
    $updates['SF_CLIENT_ID'] = Read-ConfigValue -Prompt 'Salesforce Client ID' -DefaultValue $defaultClientId -Required -HelpText 'Consumer Key der verbundenen Salesforce App.'
    $updates['SF_CLIENT_SECRET'] = Read-ConfigValue -Prompt 'Salesforce Client Secret' -DefaultValue $defaultClientSecret -Required -Secret -HelpText 'Consumer Secret der verbundenen Salesforce App.'
  }

  if (Ask-YesNo -Prompt 'Soll die SAGE100- bzw. MSSQL-Verbindung jetzt eingerichtet werden?' -Default $true) {
    Show-ConfigSection -Title 'SAGE100 / MSSQL-Verbindung' -Description 'Diese Werte werden fuer den Datenzugriff auf das lokale Sage- bzw. SQL-System verwendet.'
    $defaultServer = if ($values.Contains('SAGE100_SQL_SERVER')) { [string]$values['SAGE100_SQL_SERVER'] } else { '' }
    $defaultPort = if ($values.Contains('SAGE100_SQL_PORT')) { [string]$values['SAGE100_SQL_PORT'] } else { '1433' }
    $defaultDatabase = if ($values.Contains('SAGE100_SQL_DATABASE')) { [string]$values['SAGE100_SQL_DATABASE'] } else { '' }
    $defaultUser = if ($values.Contains('SAGE100_SQL_USER')) { [string]$values['SAGE100_SQL_USER'] } else { '' }
    $defaultPassword = if ($values.Contains('SAGE100_SQL_PASSWORD')) { [string]$values['SAGE100_SQL_PASSWORD'] } else { '' }
    $defaultAccountExternalId = if ($values.Contains('SAGE100_ACCOUNT_EXTERNAL_ID_FIELD')) { [string]$values['SAGE100_ACCOUNT_EXTERNAL_ID_FIELD'] } else { 'AccountNumber' }
    $defaultContactExternalId = if ($values.Contains('SAGE100_CONTACT_EXTERNAL_ID_FIELD')) { [string]$values['SAGE100_CONTACT_EXTERNAL_ID_FIELD'] } else { 'Email' }

    $updates['SAGE100_SQL_SERVER'] = Read-ConfigValue -Prompt 'SQL-Server / Hostname' -DefaultValue $defaultServer -Required -HelpText 'Beispiel: localhost, SQLSERVER01 oder 192.168.1.20.'
    $updates['SAGE100_SQL_PORT'] = Read-ConfigValue -Prompt 'SQL-Port' -DefaultValue $defaultPort -Required -Validation int -HelpText 'Standard fuer MSSQL ist 1433.'
    $updates['SAGE100_SQL_DATABASE'] = Read-ConfigValue -Prompt 'Datenbankname' -DefaultValue $defaultDatabase -Required -HelpText 'Name der SAGE100-Datenbank.'
    $updates['SAGE100_SQL_USER'] = Read-ConfigValue -Prompt 'SQL-Benutzername' -DefaultValue $defaultUser -Required -HelpText 'Datenbank-Benutzer mit Leserechten fuer die benoetigten Tabellen.'
    $updates['SAGE100_SQL_PASSWORD'] = Read-ConfigValue -Prompt 'SQL-Passwort' -DefaultValue $defaultPassword -Required -Secret -HelpText 'Das Passwort wird nicht angezeigt, aber als Klartext in .env gespeichert.'
    $updates['SAGE100_ACCOUNT_EXTERNAL_ID_FIELD'] = Read-ConfigValue -Prompt 'Externe ID fuer Accounts' -DefaultValue $defaultAccountExternalId -Required -HelpText 'Standard fuer SAGE100-Accounts ist AccountNumber.'
    $updates['SAGE100_CONTACT_EXTERNAL_ID_FIELD'] = Read-ConfigValue -Prompt 'Externe ID fuer Kontakte' -DefaultValue $defaultContactExternalId -Required -HelpText 'Standard fuer Kontakte ist meist Email.'
  }

  $defaultMssqlDevPassword = if ($values.Contains('MSSQL_DEV_PASSWORD')) {
    [string]$values['MSSQL_DEV_PASSWORD']
  }
  elseif ($updates.Contains('SAGE100_SQL_PASSWORD')) {
    [string]$updates['SAGE100_SQL_PASSWORD']
  }
  else {
    ''
  }

  if (Ask-YesNo -Prompt 'Soll zusaetzlich der bestehende Secret Key MSSQL_DEV_PASSWORD fuer vorhandene MSSQL-Connectoren gesetzt werden?' -Default $true) {
    Show-ConfigSection -Title 'Bestehende MSSQL-Connectoren' -Description 'Einige vorhandene Schedules verwenden den Secret Key MSSQL_DEV_PASSWORD. Ohne diesen Wert starten diese Jobs nicht.'
    $updates['MSSQL_DEV_PASSWORD'] = Read-ConfigValue -Prompt 'MSSQL_DEV_PASSWORD' -DefaultValue $defaultMssqlDevPassword -Required -Secret -HelpText 'Falls dieselbe Datenbank genutzt wird, kann hier meist dasselbe Passwort wie oben verwendet werden.'
  }

  Set-OrAppendEnvValues -Path $EnvPath -Updates $updates
  return $updates
}

Initialize-InstallerLog
Write-InstallerLog -Message ("Windows agent installer started. Log file: " + $script:InstallerLogFile)
Write-InstallerLog -Message ("Current process elevated: " + (Test-IsElevated))

try {
  $appRootResolved = Resolve-AppRoot -InputPath $AppRoot
  $entryPoint = Join-Path $appRootResolved "dist\main.js"
  $connectorSecretCheckScript = Join-Path $appRootResolved "scripts\windows\check-connector-secrets.js"
  $packageJsonPath = Join-Path $appRootResolved "package.json"
  $packageLockPath = Join-Path $appRootResolved "package-lock.json"
  $nodeModulesPath = Join-Path $appRootResolved "node_modules"
  $envPath = Join-Path $appRootResolved ".env"
  $envExamplePath = Join-Path $appRootResolved ".env.example"
  $installServiceScript = Join-Path $appRootResolved "scripts\windows\install-agent-service.ps1"
  $registerUpdaterScript = Join-Path $appRootResolved "scripts\windows\register-agent-updater-task.ps1"
  $effectiveWebUiPort = $WebUiPort
  $effectiveSchedulerIntervalMs = $SchedulerIntervalMs

  Ensure-Elevated -BoundParameters $PSBoundParameters
  Write-InstallerLog -Message ("Resolved AppRoot: " + $appRootResolved)

  if (-not (Test-Path -Path $packageJsonPath)) {
    Write-InstallerLog -Level ERROR -Message ("package.json missing: " + $packageJsonPath)
    throw "package.json not found: $packageJsonPath"
  }

  if (-not (Test-Path -Path $entryPoint)) {
    Write-InstallerLog -Level ERROR -Message ("Entry point missing: " + $entryPoint)
    throw "Entry point not found: $entryPoint. Build the project before running the installer."
  }

  if (-not (Test-Path -Path $installServiceScript)) {
    Write-InstallerLog -Level ERROR -Message ("Service installer missing: " + $installServiceScript)
    throw "Service installer script not found: $installServiceScript"
  }

  if (-not (Test-Path -Path $connectorSecretCheckScript)) {
    Write-InstallerLog -Level ERROR -Message ("Connector secret check script missing: " + $connectorSecretCheckScript)
    throw "Connector secret check script not found: $connectorSecretCheckScript"
  }

  if (-not (Test-Path -Path $registerUpdaterScript)) {
    Write-InstallerLog -Level ERROR -Message ("Updater registration script missing: " + $registerUpdaterScript)
    throw "Updater registration script not found: $registerUpdaterScript"
  }

  $nodeExe = Resolve-CommandPath -Names @("node", "node.exe") -Purpose "Node.js 22+"
  $npmExe = $null
  if ($InstallDependencies -or $RunInitialSetup) {
    $npmExe = Resolve-CommandPath -Names @("npm.cmd", "npm") -Purpose "npm"
  }
  Write-InstallerLog -Message ("Using node executable: " + $nodeExe)

  if (-not $SkipUpdater) {
    $UpdaterTaskUser = if ($UpdaterTaskUser -and $UpdaterTaskUser.Trim()) { $UpdaterTaskUser.Trim() } else { "SYSTEM" }
    if (-not $PSBoundParameters.ContainsKey('UpdaterTaskUser')) {
      Write-Host "Konfiguration Auto-Updater:" -ForegroundColor Cyan
      $UpdaterTaskUser = Read-ConfigValue -Prompt 'Benutzer fuer Updater-Task (SYSTEM oder DOMAIN\Benutzer)' -DefaultValue $UpdaterTaskUser -Required -HelpText 'SYSTEM benoetigt kein Passwort. Fuer einen eigenen Benutzer DOMAIN\Benutzer oder COMPUTER\Benutzer eingeben.'
    }

    if (-not (Test-IsBuiltInTaskAccount -AccountName $UpdaterTaskUser) -and -not $PSBoundParameters.ContainsKey('UpdaterTaskPassword')) {
      $UpdaterTaskPassword = Read-ConfigValue -Prompt 'Passwort fuer Updater-Task' -DefaultValue $UpdaterTaskPassword -Required -Secret -HelpText 'Erforderlich, wenn der Auto-Updater nicht als SYSTEM oder Dienstkonto laufen soll.'
    }
  }

  Write-Host "Installationskonfiguration:" -ForegroundColor Cyan
  Write-Host "  Zielordner           : $appRootResolved"
  Write-Host "  Dienstname           : $ServiceName"
  Write-Host "  Updater-Task         : $TaskName"
  if (-not $SkipUpdater) {
    Write-Host "  Updater-Task-User    : $UpdaterTaskUser"
    Write-Host "  Log-Bereinigung      : $UpdateLogRetentionDays Tage"
  }
  Write-Host "  Node.exe             : $nodeExe"
  Write-Host "  Einstiegspunkt       : $entryPoint"
  Write-Host "  Abhaengigkeiten      : $InstallDependencies"
  Write-Host "  .env aus Vorlage     : $CopyEnvExample"
  Write-Host "  .env interaktiv      : $PromptForEnv"
  Write-Host "  Initial-Setup        : $RunInitialSetup"
  Write-Host "  Updater ueberspringen: $SkipUpdater"
  Write-Host "  Log-Datei            : $script:InstallerLogFile"

  if (-not (Ask-YesNo -Prompt "Soll die Installation mit diesen Einstellungen fortgesetzt werden?" -Default $true)) {
    Write-InstallerLog -Level WARN -Message "Installation aborted by user at confirmation prompt."
    Write-Host "Installation abgebrochen."
    exit 1
  }

  if (-not (Test-Path -Path $nodeModulesPath)) {
    Write-InstallerLog -Level WARN -Message "node_modules folder is missing."
    if (-not (Test-Path -Path $packageLockPath)) {
      Write-InstallerLog -Level ERROR -Message ("package-lock.json missing while node_modules missing: " + $packageLockPath)
      throw "node_modules is missing and package-lock.json was not found. Use a full customer package or add package-lock.json."
    }

    $shouldInstallDependencies = $InstallDependencies
    if (-not $shouldInstallDependencies) {
      $shouldInstallDependencies = Ask-YesNo -Prompt "Der Ordner node_modules fehlt. Sollen die Produktionsabhaengigkeiten jetzt mit 'npm ci --omit=dev' installiert werden?" -Default $true
    }

    if (-not $shouldInstallDependencies) {
      Write-InstallerLog -Level ERROR -Message "Runtime dependencies are missing and installation was declined."
      throw "Installation cancelled because runtime dependencies are missing."
    }

    if (-not $npmExe) {
      $npmExe = Resolve-CommandPath -Names @("npm.cmd", "npm") -Purpose "npm"
    }

    Write-Host "Installiere Produktionsabhaengigkeiten ..." -ForegroundColor Cyan
    Write-InstallerLog -Message "Running npm ci --omit=dev."
    Invoke-ProcessChecked `
      -FilePath $npmExe `
      -ArgumentList @("ci", "--omit=dev") `
      -WorkingDirectory $appRootResolved `
      -ErrorMessage "npm ci failed"
  }

  if ((-not (Test-Path -Path $envPath)) -and (Test-Path -Path $envExamplePath)) {
  $shouldCopyEnv = $CopyEnvExample
  if (-not $shouldCopyEnv) {
    $shouldCopyEnv = Ask-YesNo -Prompt "Die Datei .env fehlt. Soll sie jetzt aus .env.example erzeugt werden?" -Default $true
  }

  if ($shouldCopyEnv) {
    Copy-Item -Path $envExamplePath -Destination $envPath -Force
    Write-InstallerLog -Message ".env created from .env.example."
    Write-Host "Die Datei .env wurde aus .env.example erzeugt." -ForegroundColor Yellow
  }
  }

  $shouldPromptEnv = $PromptForEnv
  if (-not $shouldPromptEnv -and (Test-Path -Path $envPath)) {
  $shouldPromptEnv = Ask-YesNo -Prompt "Soll die .env jetzt interaktiv bearbeitet werden?" -Default $true
  }
  elseif (-not $shouldPromptEnv -and -not (Test-Path -Path $envPath) -and (Test-Path -Path $envExamplePath)) {
  $shouldPromptEnv = Ask-YesNo -Prompt "Es wurde noch keine .env gefunden. Soll die Konfiguration jetzt interaktiv erfasst werden?" -Default $true
  }

  if ($shouldPromptEnv) {
  if (-not (Test-Path -Path $envPath) -and (Test-Path -Path $envExamplePath)) {
    Copy-Item -Path $envExamplePath -Destination $envPath -Force
  }

  if (-not (Test-Path -Path $envPath)) {
    New-Item -Path $envPath -ItemType File -Force | Out-Null
  }

  $envUpdates = Configure-EnvInteractively `
    -EnvPath $envPath `
    -ExamplePath $envExamplePath `
    -DefaultWebUiPort $effectiveWebUiPort `
    -DefaultSchedulerIntervalMs $effectiveSchedulerIntervalMs

  if ($envUpdates.Contains('WEB_UI_PORT')) {
    $effectiveWebUiPort = [int]$envUpdates['WEB_UI_PORT']
  }
  if ($envUpdates.Contains('SCHEDULER_INTERVAL_MS')) {
    $effectiveSchedulerIntervalMs = [int]$envUpdates['SCHEDULER_INTERVAL_MS']
  }
    Write-InstallerLog -Message ".env was updated interactively."
  }

  if ((-not (Test-Path -Path $envPath)) -and (-not (Ask-YesNo -Prompt "Die Datei .env fehlt weiterhin. Soll trotzdem fortgefahren werden?" -Default $false))) {
    Write-InstallerLog -Level ERROR -Message ".env is still missing and user declined continuation."
  throw "Installation cancelled until .env is provided."
  }

  if ($RunInitialSetup) {
  if (-not $npmExe) {
    $npmExe = Resolve-CommandPath -Names @("npm.cmd", "npm") -Purpose "npm"
  }

  $setupArguments = @("run", "init:installation", "--", "--mode", $InitialSetupMode)
  if ($ActivateInitialSchedules) {
    $setupArguments += "--activate"
  }

  Write-Host "Fuehre das Initial-Setup im Modus '$InitialSetupMode' aus ..." -ForegroundColor Cyan
  Write-InstallerLog -Message ("Running initial setup in mode: " + $InitialSetupMode)
  Invoke-ProcessChecked `
    -FilePath $npmExe `
    -ArgumentList $setupArguments `
    -WorkingDirectory $appRootResolved `
    -ErrorMessage "Initial setup failed"
  }

  Write-Host "Pruefe benoetigte Connector-Secret-Keys ..." -ForegroundColor Cyan
  Write-InstallerLog -Message "Running connector secret preflight check."
  $secretCheckResult = Invoke-ProcessCapture `
    -FilePath $nodeExe `
    -ArgumentList @($connectorSecretCheckScript, $appRootResolved) `
    -WorkingDirectory $appRootResolved

  if ($secretCheckResult.Output) {
    Write-InstallerLog -Message ("Connector secret preflight output: " + $secretCheckResult.Output)
  }

  if ($secretCheckResult.ExitCode -eq 2) {
    throw "Es fehlen benoetigte Secret-Keys fuer aktive Connectoren. Details siehe Installer-Log."
  }

  if ($secretCheckResult.ExitCode -ne 0) {
    throw "Der Connector-Secret-Preflight ist fehlgeschlagen: $($secretCheckResult.Output)"
  }

  Write-Host "Installiere den Windows-Dienst ..." -ForegroundColor Cyan
  Write-InstallerLog -Message "Invoking Windows service installer script."
  & $installServiceScript `
    -ServiceName $ServiceName `
    -DisplayName $DisplayName `
    -Description $Description `
    -AppRoot $appRootResolved `
    -WebUiPort $effectiveWebUiPort `
    -SchedulerIntervalMs $effectiveSchedulerIntervalMs

  if ($LASTEXITCODE -ne 0) {
    Write-InstallerLog -Level ERROR -Message ("Service installer exited with code " + $LASTEXITCODE)
    throw "Service installation failed (exit code $LASTEXITCODE)."
  }

  if (-not $SkipUpdater) {
  Write-Host "Registriere den Auto-Updater als geplante Aufgabe ..." -ForegroundColor Cyan
  Write-InstallerLog -Message "Invoking updater registration script."
  $updaterRegistrationParameters = @{
    TaskName = $TaskName
    ServiceName = $ServiceName
    ManifestUrl = $ManifestUrl
    EveryMinutes = $EveryMinutes
    UpdaterTaskUser = $UpdaterTaskUser
    LogRetentionDays = $UpdateLogRetentionDays
    AppRoot = $appRootResolved
  }
  if ($UpdaterTaskPassword) {
    $updaterRegistrationParameters['UpdaterTaskPassword'] = $UpdaterTaskPassword
  }
  & $registerUpdaterScript @updaterRegistrationParameters

    if ($LASTEXITCODE -ne 0) {
      Write-InstallerLog -Level ERROR -Message ("Updater registration exited with code " + $LASTEXITCODE)
      throw "Updater registration failed (exit code $LASTEXITCODE)."
    }
  }
  else {
  Write-Host "Die Registrierung des Auto-Updaters wurde uebersprungen." -ForegroundColor Yellow
    Write-InstallerLog -Message "Updater registration was skipped by request." -Level WARN
  }

  Write-InstallerLog -Message "Windows agent installer completed successfully."
  Write-Host "Die Installation wurde erfolgreich abgeschlossen." -ForegroundColor Green
  Write-Host "Empfohlene Pruefungen:" -ForegroundColor Cyan
  Write-Host "  Get-Service $ServiceName"
  if (-not $SkipUpdater) {
    Write-Host "  Get-ScheduledTask -TaskName '$TaskName'"
  }
  Write-Host "  Web UI: http://localhost:$effectiveWebUiPort" -ForegroundColor Cyan
  Write-Host "  Log-Datei: $script:InstallerLogFile" -ForegroundColor Cyan
}
catch {
  Write-InstallerLog -Level ERROR -Message ("Windows agent installer failed: " + $_.Exception.Message)
  if ($_.ScriptStackTrace) {
    Write-InstallerLog -Level ERROR -Message ("StackTrace: " + $_.ScriptStackTrace)
  }
  throw
}
finally {
  Write-InstallerLog -Message "Windows agent installer finished."
}