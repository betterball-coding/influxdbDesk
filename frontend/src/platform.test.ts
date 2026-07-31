import { describe, expect, it } from 'vitest'
import { isMacOSPlatform, primaryShortcutLabel } from './platform'

describe('desktop platform shortcuts', () => {
  it('uses Command for the MacIntel value reported on Intel and Apple Silicon', () => {
    expect(isMacOSPlatform('MacIntel')).toBe(true)
    expect(primaryShortcutLabel('MacIntel')).toBe('⌘')
  })

  it('keeps Control on Windows and Linux', () => {
    for (const platform of ['Win32', 'Linux x86_64']) {
      expect(isMacOSPlatform(platform)).toBe(false)
      expect(primaryShortcutLabel(platform)).toBe('Ctrl')
    }
  })
})
