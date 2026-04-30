import { useQuery } from "@tanstack/react-query";
import { pinotQuery } from "../pinot";

type Row = { client_id: string; gross: number };

export function TopExposed() {
  const { data = [] } = useQuery<Row[]>({
    queryKey: ["top-exposed"],
    queryFn: () =>
      pinotQuery<Row>(
        "SELECT client_id, SUM(ABS(notional)) AS gross FROM positions GROUP BY client_id ORDER BY gross DESC LIMIT 10"
      ),
    refetchInterval: 5_000,
  });

  const max = Math.max(...data.map((d) => Number(d.gross)), 1);

  return (
    <div className="bg-neutral-900 border border-neutral-800 rounded p-3">
      <h2 className="text-xs uppercase tracking-wide text-neutral-400 mb-2">
        Top-10 Exposed Clients
      </h2>
      <div className="space-y-1">
        {data.map((d) => (
          <div key={d.client_id} className="flex items-center gap-2 text-sm">
            <span className="w-16 text-neutral-500">{d.client_id}</span>
            <div className="flex-1 bg-neutral-800 h-4 relative">
              <div
                className="bg-blue-600 h-full"
                style={{ width: `${(Number(d.gross) / max) * 100}%` }}
              />
            </div>
            <span className="w-24 text-right">{Number(d.gross).toLocaleString()}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
