Unicode true

####
## Please note: Template replacements don't work in this file. They are provided with default defines like
## mentioned underneath.
## If the keyword is not defined, "wails_tools.nsh" will populate them with the values from ProjectInfo.
## If they are defined here, "wails_tools.nsh" will not touch them. This allows to use this project.nsi manually
## from outside of Wails for debugging and development of the installer.
##
## For development first make a wails nsis build to populate the "wails_tools.nsh":
## > wails build --target windows/amd64 --nsis
## Then you can call makensis on this file with specifying the path to your binary:
## For a AMD64 only installer:
## > makensis -DARG_WAILS_AMD64_BINARY=..\..\bin\app.exe
## For a ARM64 only installer:
## > makensis -DARG_WAILS_ARM64_BINARY=..\..\bin\app.exe
## For a installer with both architectures:
## > makensis -DARG_WAILS_AMD64_BINARY=..\..\bin\app-amd64.exe -DARG_WAILS_ARM64_BINARY=..\..\bin\app-arm64.exe
####
## The following information is taken from the ProjectInfo file, but they can be overwritten here.
####
## !define INFO_PROJECTNAME    "MyProject" # Default "{{.Name}}"
## !define INFO_COMPANYNAME    "MyCompany" # Default "{{.Info.CompanyName}}"
## !define INFO_PRODUCTNAME    "MyProduct" # Default "{{.Info.ProductName}}"
## !define INFO_PRODUCTVERSION "1.0.0"     # Default "{{.Info.ProductVersion}}"
## !define INFO_COPYRIGHT      "Copyright" # Default "{{.Info.Copyright}}"
###
## !define PRODUCT_EXECUTABLE  "Application.exe"      # Default "${INFO_PROJECTNAME}.exe"
## !define UNINST_KEY_NAME     "UninstKeyInRegistry"  # Default "${INFO_COMPANYNAME}${INFO_PRODUCTNAME}"
####
## !define REQUEST_EXECUTION_LEVEL "admin"            # Default "admin"  see also https://nsis.sourceforge.io/Docs/Chapter4.html
####
## Include the wails tools
####
!define WAILS_WIN10_REQUIRED "InfluxDesk 需要 Windows 10 x64 或更高版本。"
!define WAILS_ARCHITECTURE_NOT_SUPPORTED "此安装包仅支持 x64（amd64）Windows。"
!define INFLUXDESK_MIN_WINDOWS_BUILD 16299
!define INFLUXDESK_WINDOWS_BUILD_REQUIRED "InfluxDesk 技术最低要求为 Windows 10 1709 x64（build 16299）；推荐使用 Windows 10 22H2（build 19045）。"
!define INFLUXDESK_MIN_WEBVIEW2_VERSION "94.0.992.31"
!define INFLUXDESK_WEBVIEW2_DETAIL "正在安装或更新 Microsoft Edge WebView2 Runtime"
!define INFLUXDESK_WEBVIEW2_FAILED "WebView2 Runtime 安装或更新失败。请检查网络和管理员策略后重试。"

!include "wails_tools.nsh"
!include "WordFunc.nsh"

!macro influxdesk.readWebView2Version output
    SetRegView 64
    ClearErrors
    ReadRegStr ${output} HKLM "SOFTWARE\WOW6432Node\Microsoft\EdgeUpdate\Clients\{F3017226-FE2A-4295-8BDF-00C3A9A7E4C5}" "pv"
    ${If} ${output} == ""
        ReadRegStr ${output} HKCU "Software\Microsoft\EdgeUpdate\Clients\{F3017226-FE2A-4295-8BDF-00C3A9A7E4C5}" "pv"
    ${EndIf}
!macroend

!macro influxdesk.webview2runtime
    !insertmacro influxdesk.readWebView2Version $0
    ${If} $0 != ""
        ${VersionCompare} "$0" "${INFLUXDESK_MIN_WEBVIEW2_VERSION}" $1
        ${If} $1 != 2
            Goto influxdesk_webview2_ready
        ${EndIf}
    ${EndIf}

    SetDetailsPrint both
    DetailPrint "${INFLUXDESK_WEBVIEW2_DETAIL}"
    SetDetailsPrint listonly
    InitPluginsDir
    CreateDirectory "$pluginsdir\webview2bootstrapper"
    SetOutPath "$pluginsdir\webview2bootstrapper"
    File "tmp\MicrosoftEdgeWebview2Setup.exe"
    ExecWait '"$pluginsdir\webview2bootstrapper\MicrosoftEdgeWebview2Setup.exe" /silent /install' $1
    ${If} $1 != 0
        IfSilent +2 0
        MessageBox MB_ICONSTOP|MB_OK "${INFLUXDESK_WEBVIEW2_FAILED}（错误码：$1）"
        SetErrorLevel 66
        Abort
    ${EndIf}

    !insertmacro influxdesk.readWebView2Version $0
    ${If} $0 != ""
        ${VersionCompare} "$0" "${INFLUXDESK_MIN_WEBVIEW2_VERSION}" $1
        ${If} $1 != 2
            Goto influxdesk_webview2_ready
        ${EndIf}
    ${EndIf}
    IfSilent +2 0
    MessageBox MB_ICONSTOP|MB_OK "${INFLUXDESK_WEBVIEW2_FAILED}"
    SetErrorLevel 66
    Abort

    influxdesk_webview2_ready:
!macroend

!macro influxdesk.checkMinimumWindowsBuild
    ${IfNot} ${AtLeastBuild} ${INFLUXDESK_MIN_WINDOWS_BUILD}
        IfSilent +2 0
        MessageBox MB_ICONSTOP|MB_OK "${INFLUXDESK_WINDOWS_BUILD_REQUIRED}"
        SetErrorLevel 64
        Abort
    ${EndIf}
!macroend

# The version information for this two must consist of 4 parts
VIProductVersion "${INFO_PRODUCTVERSION}.0"
VIFileVersion    "${INFO_PRODUCTVERSION}.0"

VIAddVersionKey "CompanyName"     "${INFO_COMPANYNAME}"
VIAddVersionKey "FileDescription" "${INFO_PRODUCTNAME} Installer"
VIAddVersionKey "ProductVersion"  "${INFO_PRODUCTVERSION}"
VIAddVersionKey "FileVersion"     "${INFO_PRODUCTVERSION}"
VIAddVersionKey "LegalCopyright"  "${INFO_COPYRIGHT}"
VIAddVersionKey "ProductName"     "${INFO_PRODUCTNAME}"

# Enable HiDPI support. https://nsis.sourceforge.io/Reference/ManifestDPIAware
ManifestDPIAware true

!include "MUI.nsh"

!define MUI_ICON "..\icon.ico"
!define MUI_UNICON "..\icon.ico"
# !define MUI_WELCOMEFINISHPAGE_BITMAP "resources\leftimage.bmp" #Include this to add a bitmap on the left side of the Welcome Page. Must be a size of 164x314
!define MUI_FINISHPAGE_NOAUTOCLOSE # Wait on the INSTFILES page so the user can take a look into the details of the installation steps
!define MUI_ABORTWARNING # This will warn the user if they exit from the installer.

!insertmacro MUI_PAGE_WELCOME # Welcome to the installer page.
# !insertmacro MUI_PAGE_LICENSE "resources\eula.txt" # Adds a EULA page to the installer
!insertmacro MUI_PAGE_DIRECTORY # In which folder install page.
!insertmacro MUI_PAGE_INSTFILES # Installing page.
!insertmacro MUI_PAGE_FINISH # Finished installation page.

!insertmacro MUI_UNPAGE_INSTFILES # Uinstalling page

!insertmacro MUI_LANGUAGE "SimpChinese" # Set the Language of the installer

## Production builds pass an absolute signing-script path. The same fail-closed
## verifier signs both the generated uninstaller and the final NSIS package.
!ifdef INFLUXDESK_SIGN_SCRIPT
!uninstfinalize 'powershell -NoProfile -ExecutionPolicy Bypass -File "${INFLUXDESK_SIGN_SCRIPT}" -Path "%1"'
!finalize 'powershell -NoProfile -ExecutionPolicy Bypass -File "${INFLUXDESK_SIGN_SCRIPT}" -Path "%1"'
!endif

Name "${INFO_PRODUCTNAME}"
OutFile "..\..\bin\${INFO_PROJECTNAME}-${INFO_PRODUCTVERSION}-win10-x64-installer.exe"
!ifdef WAILS_INSTALL_SCOPE
  !if "${WAILS_INSTALL_SCOPE}" == "user"
    InstallDir "$LOCALAPPDATA\Programs\${INFO_PRODUCTNAME}"
  !else
    InstallDir "$PROGRAMFILES64\${INFO_COMPANYNAME}\${INFO_PRODUCTNAME}"
  !endif
!else
  InstallDir "$PROGRAMFILES64\${INFO_COMPANYNAME}\${INFO_PRODUCTNAME}"
!endif # Default installing folder ($PROGRAMFILES is Program Files folder).
ShowInstDetails show # This will always show the installation details.

Function .onInit
    !insertmacro wails.checkArchitecture
    !insertmacro influxdesk.checkMinimumWindowsBuild
FunctionEnd

Section
    !insertmacro wails.setShellContext

    !insertmacro influxdesk.webview2runtime

    SetOutPath $INSTDIR

    !insertmacro wails.files

    CreateShortcut "$SMPROGRAMS\${INFO_PRODUCTNAME}.lnk" "$INSTDIR\${PRODUCT_EXECUTABLE}"
    CreateShortCut "$DESKTOP\${INFO_PRODUCTNAME}.lnk" "$INSTDIR\${PRODUCT_EXECUTABLE}"

    !insertmacro wails.associateFiles
    !insertmacro wails.associateCustomProtocols

    !insertmacro wails.writeUninstaller
SectionEnd

Section "uninstall"
    !insertmacro wails.setShellContext

    RMDir /r "$AppData\${PRODUCT_EXECUTABLE}" # Remove the WebView2 DataPath

    RMDir /r $INSTDIR

    Delete "$SMPROGRAMS\${INFO_PRODUCTNAME}.lnk"
    Delete "$DESKTOP\${INFO_PRODUCTNAME}.lnk"

    !insertmacro wails.unassociateFiles
    !insertmacro wails.unassociateCustomProtocols

    !insertmacro wails.deleteUninstaller
SectionEnd
