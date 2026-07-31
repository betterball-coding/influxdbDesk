import { createRequire } from 'node:module'
import { readFile, writeFile } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const scriptDir = dirname(fileURLToPath(import.meta.url))
const projectDir = resolve(scriptDir, '..')
const require = createRequire(resolve(projectDir, 'frontend/package.json'))
const { chromium } = require('playwright')

const svgPath = resolve(projectDir, 'build/appicon.svg')
const pngPath = resolve(projectDir, 'build/appicon.png')
const icoPath = resolve(projectDir, 'build/windows/icon.ico')
const iconSizes = [256, 128, 64, 48, 32, 16]

await readFile(svgPath)

const browser = await chromium.launch({ headless: true })

try {
  const render = async (size, outputPath) => {
    const page = await browser.newPage({ viewport: { width: size, height: size } })
    await page.goto(pathToFileURL(svgPath).href)
    const png = await page.screenshot({ path: outputPath, omitBackground: true })
    await page.close()
    return png
  }

  await render(1024, pngPath)
  const images = []
  for (const size of iconSizes) {
    images.push(await render(size))
  }

  const headerSize = 6 + iconSizes.length * 16
  const header = Buffer.alloc(headerSize)
  header.writeUInt16LE(0, 0)
  header.writeUInt16LE(1, 2)
  header.writeUInt16LE(iconSizes.length, 4)

  let imageOffset = headerSize
  iconSizes.forEach((size, index) => {
    const entryOffset = 6 + index * 16
    const image = images[index]
    header.writeUInt8(size === 256 ? 0 : size, entryOffset)
    header.writeUInt8(size === 256 ? 0 : size, entryOffset + 1)
    header.writeUInt8(0, entryOffset + 2)
    header.writeUInt8(0, entryOffset + 3)
    header.writeUInt16LE(1, entryOffset + 4)
    header.writeUInt16LE(32, entryOffset + 6)
    header.writeUInt32LE(image.length, entryOffset + 8)
    header.writeUInt32LE(imageOffset, entryOffset + 12)
    imageOffset += image.length
  })

  await writeFile(icoPath, Buffer.concat([header, ...images]))
  console.log(`Generated ${pngPath}`)
  console.log(`Generated ${icoPath} (${iconSizes.join(', ')} px)`)
} finally {
  await browser.close()
}
