# WiX MSI

This project builds the Windows 11 x64 MSI. The Windows 10 release uses the separate NSIS package because that package carries the WebView2 bootstrapper.

Build on Windows with .NET 8 and WiX 7:

```powershell
dotnet build .\build\windows\wix\InfluxDesk.wixproj `
  -c Release `
  -p:ProductVersion=1.0.0 `
  -p:SourceDir="$PWD\build\bin"
```

The stable `UpgradeCode` and component GUID must never change. `ProductCode` is generated per build. The MSI is x64 and per-machine, keeps user data on uninstall, rejects downgrade and same-version replacement, and checks the machine-wide Evergreen WebView2 registration before file copy.
