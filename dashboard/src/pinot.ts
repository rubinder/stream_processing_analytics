const PINOT_URL = import.meta.env.VITE_PINOT_URL || "http://localhost:8099/query/sql";

interface PinotResponse {
  resultTable?: { dataSchema: { columnNames: string[] }; rows: unknown[][] };
  exceptions?: { errorCode: number; message: string }[];
}

export async function pinotQuery<T extends Record<string, unknown>>(sql: string): Promise<T[]> {
  const r = await fetch(PINOT_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sql }),
  });
  if (!r.ok) throw new Error(`Pinot HTTP ${r.status}`);
  const json = (await r.json()) as PinotResponse;
  if (json.exceptions?.length) throw new Error(json.exceptions[0].message);
  const cols = json.resultTable?.dataSchema.columnNames ?? [];
  const rows = json.resultTable?.rows ?? [];
  return rows.map((row) => Object.fromEntries(cols.map((c, i) => [c, row[i]])) as T);
}
