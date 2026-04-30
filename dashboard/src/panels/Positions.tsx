import { useQuery } from "@tanstack/react-query";
import { pinotQuery } from "../pinot";

type Row = { symbol: string; net_quantity: number; notional_double: number; sector: string };

export function Positions({ clientId }: { clientId: string | null }) {
  const where = clientId ? `WHERE client_id = '${clientId}'` : "";
  const { data = [] } = useQuery<Row[]>({
    queryKey: ["positions", clientId],
    queryFn: () =>
      pinotQuery<Row>(
        `SELECT symbol, net_quantity, notional_double, sector FROM positions ${where} ORDER BY ABS(notional_double) DESC LIMIT 100`
      ),
    refetchInterval: 2_000,
  });

  return (
    <div className="bg-neutral-900 border border-neutral-800 rounded p-3">
      <h2 className="text-xs uppercase tracking-wide text-neutral-400 mb-2">
        Positions {clientId ? `· ${clientId}` : "· All"}
      </h2>
      <table className="w-full text-sm">
        <thead className="text-neutral-500 text-xs uppercase">
          <tr>
            <th className="text-left">Symbol</th>
            <th className="text-right">Qty</th>
            <th className="text-right">Notional</th>
            <th className="text-left">Sector</th>
          </tr>
        </thead>
        <tbody>
          {data.map((r) => (
            <tr key={r.symbol} className="border-t border-neutral-800">
              <td>{r.symbol}</td>
              <td className="text-right">{Number(r.net_quantity).toLocaleString()}</td>
              <td className="text-right">{Number(r.notional_double).toLocaleString(undefined, { maximumFractionDigits: 2 })}</td>
              <td className="text-neutral-500">{r.sector}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
