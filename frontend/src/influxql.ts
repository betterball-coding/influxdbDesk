export function quoteInfluxIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('\\', '\\\\').replaceAll('"', '\\"')}"`
}

export function quoteInfluxString(value: string): string {
  return `'${value.replaceAll('\\', '\\\\').replaceAll("'", "\\'")}'`
}

export function defaultMeasurementQuery(measurement: string): string {
  return `SELECT * FROM ${quoteInfluxIdentifier(measurement)} WHERE time > now() - 5m`
}
