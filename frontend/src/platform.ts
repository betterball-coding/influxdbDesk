export function isMacOSPlatform(platform = navigator.platform): boolean {
  return /Mac|iPhone|iPad|iPod/.test(platform)
}

export function primaryShortcutLabel(platform = navigator.platform): string {
  return isMacOSPlatform(platform) ? '⌘' : 'Ctrl'
}
