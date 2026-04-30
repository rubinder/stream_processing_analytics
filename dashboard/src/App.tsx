import { QueryClient, QueryClientProvider, useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { pinotQuery } from "./pinot";
import { BreachesSparkline } from "./panels/BreachesSparkline";
import { TopExposed } from "./panels/TopExposed";
import { Positions } from "./panels/Positions";
import { BreachFeed } from "./panels/BreachFeed";

const qc = new QueryClient();

function ClientFilter({
  value,
  onChange,
}: {
  value: string | null;
  onChange: (v: string | null) => void;
}) {
  const { data = [] } = useQuery<{ client_id: string }[]>({
    queryKey: ["client-list"],
    queryFn: () =>
      pinotQuery<{ client_id: string }>("SELECT DISTINCT client_id FROM positions LIMIT 1000"),
    refetchInterval: 30_000,
  });
  return (
    <select
      className="bg-neutral-800 border border-neutral-700 px-2 py-1 text-sm"
      value={value ?? "ALL"}
      onChange={(e) => onChange(e.target.value === "ALL" ? null : e.target.value)}
    >
      <option value="ALL">All clients</option>
      {data.map((d) => (
        <option key={d.client_id} value={d.client_id}>
          {d.client_id}
        </option>
      ))}
    </select>
  );
}

function Shell() {
  const [client, setClient] = useState<string | null>(null);
  return (
    <div className="p-4 space-y-3 max-w-screen-2xl mx-auto">
      <header className="flex items-center justify-between">
        <h1 className="text-lg font-semibold">Streaming Risk Dashboard</h1>
        <ClientFilter value={client} onChange={setClient} />
      </header>
      <BreachesSparkline />
      <div className="grid grid-cols-2 gap-3">
        <div className="space-y-3">
          <TopExposed />
          <Positions clientId={client} />
        </div>
        <BreachFeed />
      </div>
    </div>
  );
}

export default function App() {
  return (
    <QueryClientProvider client={qc}>
      <Shell />
    </QueryClientProvider>
  );
}
