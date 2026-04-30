import { useQuery } from "@tanstack/react-query";
import { pinotQuery } from "../pinot";

type Row = {
  client_id: string;
  breach_type: string;
  actual_value: number;
  limit_value: number;
  details: string;
  detected_ts: number;
};

const COLOURS: Record<string, string> = {
  GROSS_NOTIONAL: "border-l-red-500",
  SINGLE_ORDER_NOTIONAL: "border-l-amber-500",
  TRADE_VELOCITY: "border-l-blue-500",
};

export function BreachFeed() {
  const { data = [] } = useQuery<Row[]>({
    queryKey: ["breaches"],
    queryFn: () =>
      pinotQuery<Row>(
        "SELECT client_id, breach_type, actual_value, limit_value, details, detected_ts FROM breaches WHERE detected_ts > ago('PT5M') ORDER BY detected_ts DESC LIMIT 50"
      ),
    refetchInterval: 2_000,
  });

  return (
    <div className="bg-neutral-900 border border-neutral-800 rounded p-3 h-full overflow-y-auto">
      <h2 className="text-xs uppercase tracking-wide text-neutral-400 mb-2">
        Live Breach Feed (last 5m)
      </h2>
      <ul className="space-y-1">
        {data.map((b, i) => (
          <li
            key={i}
            className={`pl-2 border-l-4 ${COLOURS[b.breach_type] ?? "border-l-neutral-500"} text-sm`}
          >
            <div className="flex justify-between">
              <span>
                {b.client_id} <span className="text-neutral-500">{b.breach_type}</span>
              </span>
              <span className="text-neutral-600 text-xs">
                {new Date(Number(b.detected_ts)).toLocaleTimeString()}
              </span>
            </div>
            <div className="text-neutral-400 text-xs">{b.details}</div>
          </li>
        ))}
      </ul>
    </div>
  );
}
