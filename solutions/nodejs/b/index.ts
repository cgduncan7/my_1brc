import { FileHandle, open, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads'

const WORKER_CHUNK_SIZE = 1024 * 1024 * 32 
const ACTIVE_WORKERS = 16

const INPUT_FILENAME_SMALL = join('..','..','..','data','measurements_small.txt')
const INPUT_FILENAME = join('..','..','..','data','measurements.txt')
const getInputFileName = (full: boolean) => full ? INPUT_FILENAME : INPUT_FILENAME_SMALL
const getOutputFileName = (full: boolean) => join('..','..','..','data',`output_${full?'full':'small'}_a_${Date.now()}.txt`)

interface StationAggregates {
  count: number
  min: number
  max: number
  mean: number
}


;(async () => {
  if (isMainThread) {
    const [_, __, inputType] = process.argv
    if (inputType == null || inputType == '') throw new Error('Required argument for inputType')
      const useFullInput = inputType === 'full'
    const findChunkBoundary = async(file: FileHandle, fileSize: number, start: number) => {
      if (start + WORKER_CHUNK_SIZE > fileSize) return [start, undefined]
      let byte: Buffer
      let bytesRead: number
      let inc = 0
      while ({ bytesRead, buffer: byte } = await file.read(Buffer.alloc(1), 0, 1, start + WORKER_CHUNK_SIZE + inc)) {
        // if newLine return boundary
        if (byte.at(0) === 0x0a || bytesRead === 0) {
          return [start, start + WORKER_CHUNK_SIZE + inc]
        }
        inc += 1
      }
      return [start, undefined]
    }

    const applyStationAggregate = (root: Record<string, StationAggregates>, toApply: Record<string, StationAggregates>) => {
      const entries = Object.entries(toApply)
      for (const [key, value] of entries) {
        const rootAgg = root[key]
        if (rootAgg != null) {
          root[key] = {
            min: Math.min(rootAgg.min, value.min),
            max: Math.max(rootAgg.max, value.max),
            mean: ((rootAgg.mean * rootAgg.count) + (value.mean * value.count)) / (rootAgg.count + value.count),
            count: rootAgg.count + value.count,
          }
        } else {
          root[key] = value
        }
      }
    }

    const combineStationAggregates = (stationAggregates: Record<string, StationAggregates>[]) => {
      let root: Record<string, StationAggregates> = {}
      for (const sa of stationAggregates) {
        applyStationAggregate(root, sa)
      }
      return root
    }

    const file = await open(getInputFileName(useFullInput), 'r')
    const stat = await file.stat()
  
    const chunkStarts = []
    let start = 0
    while (start <= stat.size) {
      const chunkStart = await findChunkBoundary(file, stat.size, start)
      if (chunkStart[1]) {
        start = chunkStart[1] + 1
      } else {
        start = stat.size + 1
      }
      chunkStarts.push(chunkStart)
    }

    let results: Record<string, StationAggregates>[] = []
    let activeWorkers = 0

    const decreaseActiveWorkers = () => {
      activeWorkers -= 1
    }
    
    const workerFns = chunkStarts.map(([start, end], index) => {
      return () => {
        const worker = new Worker(__filename, { workerData: { start, end, index, useFullInput }})
        worker.on('message', r => results.push(r))
        worker.on('error', decreaseActiveWorkers)
        worker.on('exit', decreaseActiveWorkers)
      }
    })

    while (workerFns.length > 0 || activeWorkers > 0) {
      if (activeWorkers < ACTIVE_WORKERS && workerFns.length > 0) {
        workerFns.shift()!()
        activeWorkers += 1
      } else {
        await new Promise((res) => setTimeout(res, 100))
      }
    }
    const combinedResults = combineStationAggregates(results)
    await writeFile(getOutputFileName(useFullInput), JSON.stringify(combinedResults))
  } else {
    const { start, end, useFullInput } = workerData
    const file = await open(getInputFileName(useFullInput), 'r')
    let stationAggregateMap: Record<string, StationAggregates> = {}
    for await (const line of file.readLines({ start, end, encoding: 'utf8' })) {
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
    parentPort?.postMessage(stationAggregateMap)
  }
})()