import { open, writeFile } from 'node:fs/promises'
import { join } from 'node:path'

const INPUT_FILENAME_SMALL = join('..','..','..','data','measurements_small.txt')
const INPUT_FILENAME = join('..','..','..','data','measurements.txt')
const getOutputFileName = (full: boolean) => join('..','..','..','data',`output_${full?'full':'small'}_a_${Date.now()}.txt`)

interface StationAggregates {
  count: number
  min: number
  max: number
  mean: number
}

const [_, __, inputType] = process.argv
if (inputType == null || inputType == '') throw new Error('Required argument for inputType')

;(async () => {
  const useFullInput = inputType === 'full'
  let stationAggregateMap: Record<string, StationAggregates> = {}
  const file = await open(useFullInput ? INPUT_FILENAME : INPUT_FILENAME_SMALL)
  for await (const line of file.readLines({ encoding: 'utf8' })) {
    const [station, rawMeasurement] = line.split(';')
    const measurement = Number(rawMeasurement)
    const { count, min, max, mean } = stationAggregateMap[station] ?? { count: 0, min: 100, max: -100, mean: 0 }
    stationAggregateMap[station] = {
      count: count + 1,
      min: Math.min(min, measurement),
      max: Math.max(max, measurement),
      mean: mean + (measurement - mean) / (count + 1),
    }
  }
  await writeFile(getOutputFileName(useFullInput), JSON.stringify(stationAggregateMap))
})()