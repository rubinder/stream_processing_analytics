import { useQuery } from "@tanstack/react-query";
import { Area, AreaChart, ResponsiveContainer, Tooltip, XAxis } from "recharts";
import { pinotQuery } from "../pinot";

type Row = { minute: number; cnt: number };

export function BreachesSparkline() {
  const { data = [] } = useQuery<Row[]>({
    queryKey: ["sparkline"],
    queryFn: () =>
      pinotQuery<Row>(
        "SELECT DATETRUNC('MINUTE', detected_ts) AS minute, COUNT(*) AS cnt " +
        "FROM breaches WHERE detected_ts > ago('PT1H') " +
        "GROUP BY minute ORDER BY minute"
      ),
    refetchInterval: 10_000,
  });

  return (
    <div className="bg-neutral-900 border border-neutral-800 rounded p-3">
      <h2 className="text-xs uppercase tracking-wide text-neutral-400 mb-1">
        Breaches per minute, last 1h
      </h2>
      <ResponsiveContainer width="100%" height={80}>
        <AreaChart data={data}>
          <XAxis dataKey="minute" hide />
          <Tooltip
            contentStyle={{ background: "#171717", border: "1px solid #404040" }}
            labelFormatter={(v) => new Date(v as number).toLocaleTimeString()}
          />
          <Area type="monotone" dataKey="cnt" stroke="#f87171" fill="#f87171" fillOpacity={0.3} />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
