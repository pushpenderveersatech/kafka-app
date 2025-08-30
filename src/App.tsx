import  { useEffect, useMemo, useRef, useState } from "react";
import * as d3 from "d3";
import { Crown, Server, AlertTriangle, RefreshCw, Crosshair, Send, Plus, Minus, Target, Layers } from "lucide-react";

// ---------------------------------------------------------
// Types
// ---------------------------------------------------------

type Broker = { id: string; name: string; up: boolean };

type Partition = {
  id: number; // partition number
  topic: string;
  leader: string | null; // broker id
  replicas: string[]; // includes leader as first ideally
  offline?: boolean;
  endOffset: number; // last produced offset + 1 (log end offset)
};

type Topic = {
  name: string;
  partitions: Partition[];
  color: string; // hex for visuals
};

type Cluster = {
  brokers: Broker[];
  topics: Topic[];
};

// Consumer groups commit offsets per (topic, partition).
// Assignments map a partition to a particular consumer in the group.

type Consumer = { id: string };

type ConsumerGroup = {
  id: string;
  members: Consumer[];
  // partition key `${topic}:p${id}` -> consumerId
  assignments: Record<string, string>;
  // committed offsets per partition key
  offsets: Record<string, number>;
};

// ---------------------------------------------------------
// Utilities
// ---------------------------------------------------------

const PALETTE = [
  "#22c55e",
  "#3b82f6",
  "#eab308",
  "#ef4444",
  "#a855f7",
  "#06b6d4",
  "#f97316",
  "#84cc16",
];

function hashKeyToPartition(key: string, partitions: number) {
  let h = 0;
  for (let i = 0; i < key.length; i++) h = (h * 31 + key.charCodeAt(i)) | 0;
  return Math.abs(h) % partitions;
}

function buildCluster(
  brokerCount: number,
  topicsConfig: { name: string; partitions: number; replicationFactor: number }[]
): Cluster {
  const brokers: Broker[] = Array.from({ length: brokerCount }, (_, i) => ({
    id: `b${i}`,
    name: `broker-${i}`,
    up: true,
  }));

  const topics: Topic[] = topicsConfig.map((t, idx) => {
    const rf = Math.max(1, Math.min(t.replicationFactor, brokerCount));
    const parts: Partition[] = Array.from({ length: t.partitions }, (_, p) => {
      // Round-robin leader, then replicas wrap around
      const leaderIdx = p % brokerCount;
      const replicaIdxs = Array.from({ length: rf }, (_, k) => (leaderIdx + k) % brokerCount);
      const replicas = replicaIdxs.map((ri) => brokers[ri].id);
      return {
        id: p,
        topic: t.name,
        leader: replicas[0],
        replicas,
        endOffset: 0,
      } as Partition;
    });
    return { name: t.name, partitions: parts, color: PALETTE[idx % PALETTE.length] };
  });

  return { brokers, topics };
}

function cloneCluster(c: Cluster): Cluster {
  return {
    brokers: c.brokers.map((b) => ({ ...b })),
    topics: c.topics.map((t) => ({
      ...t,
      partitions: t.partitions.map((p) => ({ ...p, replicas: [...p.replicas] })),
    })),
  };
}

function electLeaders(c: Cluster) {
  // Recompute ISR and leaders when some brokers are down
  const upSet = new Set(c.brokers.filter((b) => b.up).map((b) => b.id));
  c.topics.forEach((t) =>
    t.partitions.forEach((p) => {
      const aliveReplicas = p.replicas.filter((r) => upSet.has(r));
      const hasLeaderAlive = p.leader && upSet.has(p.leader);
      if (!hasLeaderAlive) {
        p.leader = aliveReplicas[0] ?? null;
      }
      p.offline = !p.leader; // no leader -> offline
    })
  );
}

function partitionsOnBroker(cluster: Cluster, brokerId: string) {
  const res: Partition[] = [];
  cluster.topics.forEach((t) => {
    t.partitions.forEach((p) => {
      if (p.leader === brokerId || p.replicas.includes(brokerId)) {
        res.push(p);
      }
    });
  });
  return res;
}

// Arrange brokers in a grid (rows x columns) based on width
function brokerLayout(brokers: Broker[], width: number) {
  const minCard = 260; // px card width
  const cols = Math.max(1, Math.floor(width / minCard));
  const colWidth = width / cols;
  return brokers.map((b, i) => ({
    broker: b,
    x: (i % cols) * colWidth,
    y: Math.floor(i / cols) * 280,
    w: colWidth,
    h: 260,
  }));
}

// Assignment helpers
function partitionKey(t: string, p: number) {
  return `${t}:p${p}`;
}

function rebalanceGroup(g: ConsumerGroup, topics: Topic[]) {
  // simple RR across all partitions in all topics
  const allParts: string[] = [];
  topics.forEach((t) => t.partitions.forEach((p) => allParts.push(partitionKey(t.name, p.id))));
  if (g.members.length === 0) {
    g.assignments = {};
    return;
  }
  const asg: Record<string, string> = {};
  allParts.forEach((pk, i) => {
    asg[pk] = g.members[i % g.members.length].id;
  });
  g.assignments = asg;
}

function computeLagFor(g: ConsumerGroup, t: Topic, p: Partition) {
  const key = partitionKey(t.name, p.id);
  const committed = g.offsets[key] || 0;
  return p.endOffset - committed;
}

// ---------------------------------------------------------
// Component
// ---------------------------------------------------------

export default function KafkaInteractiveReact() {
  const [brokerCount, setBrokerCount] = useState(3);
  const [topicsSpec, setTopicsSpec] = useState([
    { name: "logs", partitions: 6, replicationFactor: 2 },
    { name: "purchases", partitions: 6, replicationFactor: 3 },
    { name: "trucks_gps", partitions: 6, replicationFactor: 2 },
  ]);

  const [cluster, setCluster] = useState<Cluster>(() => buildCluster(brokerCount, topicsSpec));

  // Consumer Groups state
  const [groups, setGroups] = useState<ConsumerGroup[]>([
    { id: "g1", members: [{ id: "c1" }, { id: "c2" }], assignments: {}, offsets: {} },
    { id: "g2", members: [{ id: "c3" }], assignments: {}, offsets: {} },
  ]);
  const [selectedGroup, setSelectedGroup] = useState<string | null>("g1");

  // Derived state for filtering/highlighting
  const [filterTopic, setFilterTopic] = useState<string | null>(null);
  const [selected, setSelected] = useState<{ kind: "broker" | "partition" | "topic"; id: string; extra?: any } | null>(
    null
  );

  // Build / rebuild cluster when controls change
  useEffect(() => {
    const next = buildCluster(brokerCount, topicsSpec);
    setCluster(next);
  }, [brokerCount, JSON.stringify(topicsSpec)]);

  // Rebalance groups whenever topology or group membership changes
  useEffect(() => {
    setGroups((prev) => {
      const copy = prev.map((g) => ({ ...g, members: [...g.members], assignments: { ...g.assignments }, offsets: { ...g.offsets } }));
      copy.forEach((g) => rebalanceGroup(g, cluster.topics));
      return copy;
    });
  }, [JSON.stringify(cluster.topics), JSON.stringify(groups.map((g) => ({ id: g.id, m: g.members.map((m) => m.id) })))]);

  // Zoom / Pan via D3
  const svgRef = useRef<SVGSVGElement | null>(null);
  const gRef = useRef<SVGGElement | null>(null);
  useEffect(() => {
    if (!svgRef.current || !gRef.current) return;
    const svg = d3.select(svgRef.current);
    const g = d3.select(gRef.current);
    const zoomed = (event: any) => g.attr("transform", event.transform);
    const zoom = d3.zoom<SVGSVGElement, unknown>().scaleExtent([0.5, 2.5]).on("zoom", zoomed);
    svg.call(zoom as any);
    return () => {
      svg.on("wheel.zoom", null).on("mousedown.zoom", null).on("touchstart.zoom", null);
    };
  }, []);

  // Re-election when brokers up/down change
  useEffect(() => {
    const next = cloneCluster(cluster);
    electLeaders(next);
    setCluster(next);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cluster.brokers.map((b) => `${b.id}:${b.up}`).join(",")]);

  const width = 1200; // canvas width
  const height = 820; // canvas height

  // Layout brokers
  const layout = useMemo(() => brokerLayout(cluster.brokers, width - 24), [cluster.brokers, width]);

  // Legend topics
  const topicLegend = cluster.topics.map((t) => ({ name: t.name, color: t.color }));

  function toggleBroker(id: string) {
    const next = cloneCluster(cluster);
    const b = next.brokers.find((x) => x.id === id)!;
    b.up = !b.up;
    electLeaders(next);
    setCluster(next);
    setSelected({ kind: "broker", id: id, extra: b });
  }

  function setAllUp(up: boolean) {
    const next = cloneCluster(cluster);
    next.brokers.forEach((b) => (b.up = up));
    electLeaders(next);
    setCluster(next);
  }

  function failRandomBroker() {
    const ups = cluster.brokers.filter((b) => b.up);
    if (ups.length === 0) return;
    toggleBroker(ups[Math.floor(Math.random() * ups.length)].id);
  }

  function onClickPartition(p: Partition) {
    setSelected({ kind: "partition", id: `${p.topic}:${p.id}`, extra: p });
  }

  function onClickTopic(name: string) {
    setFilterTopic((prev) => (prev === name ? null : name));
    setSelected({ kind: "topic", id: name });
  }

  function updateTopicSpec(idx: number, key: "name" | "partitions" | "replicationFactor", value: any) {
    setTopicsSpec((prev) => {
      const next = prev.map((t) => ({ ...t }));
      // prevent invalid values
      if (key === "partitions" || key === "replicationFactor") {
        value = Math.max(1, parseInt(value || 1));
      }
      // prevent duplicate names
      if (key === "name" && value) {
        const exists = next.some((t, i) => t.name === value && i !== idx);
        if (exists) value = `${value}-${idx}`;
      }
      (next[idx] as any)[key] = value;
      return next;
    });
  }

  function addTopic() {
    setTopicsSpec((prev) => [...prev, { name: `topic_${prev.length}`, partitions: 6, replicationFactor: 2 }]);
  }
  function removeTopic(idx: number) {
    setTopicsSpec((prev) => prev.filter((_, i) => i !== idx));
    if (filterTopic && topicsSpec[idx]?.name === filterTopic) setFilterTopic(null);
  }

  // --- PRODUCER: publish with or without a key ---
  const [produceTopic, setProduceTopic] = useState<string>("logs");
  const [produceKey, setProduceKey] = useState<string>("");

  function produce() {
    setCluster((prev) => {
      const next = cloneCluster(prev);
      const t = next.topics.find((x) => x.name === produceTopic);
      if (!t) return prev;
      const pCount = t.partitions.length;
      const pid = produceKey ? hashKeyToPartition(produceKey, pCount) : Math.floor(Math.random() * pCount);
      const part = t.partitions[pid];
      part.endOffset += 1; // append message -> increment log end offset
      return next;
    });
  }

  // --- CONSUMER side: consume for a group (advances committed offsets) ---
  function consumeOnce(groupId: string) {
    const g = groups.find((x) => x.id === groupId);
    if (!g) return;
    const nextGroups = groups.map((gg) => ({ ...gg, members: [...gg.members], assignments: { ...gg.assignments }, offsets: { ...gg.offsets } }));
    const target = nextGroups.find((x) => x.id === groupId)!;
    // iterate all partitions assigned to any member
    cluster.topics.forEach((t) => {
      t.partitions.forEach((p) => {
        const pk = partitionKey(t.name, p.id);
        const assigned = target.assignments[pk];
        if (!assigned) return; // no member -> no progress
        const committed = target.offsets[pk] || 0;
        if (committed < p.endOffset) {
          // consume 1 message (simulate)
          target.offsets[pk] = committed + 1;
        }
      });
    });
    setGroups(nextGroups);
  }

  // helpers for UI
  function groupLagSummary(g: ConsumerGroup) {
    let totalLag = 0, parts = 0;
    cluster.topics.forEach((t) => t.partitions.forEach((p) => { totalLag += computeLagFor(g, t, p); parts++; }));
    return { totalLag, parts };
  }

  // Partition tiles inside a broker card
  function renderBrokerTiles(brokerId: string, w: number) {
    const parts = partitionsOnBroker(cluster, brokerId).filter((p) => (filterTopic ? p.topic === filterTopic : true));
    // layout as grid
    const cols = Math.max(2, Math.floor((w - 24) / 140));
    const tileW = Math.min(180, (w - 24) / cols - 8);
    const tileH = 70;
    return (
      <g>
        {parts.map((p, idx) => {
          const col = idx % cols;
          const row = Math.floor(idx / cols);
          const x = 12 + col * (tileW + 8);
          const y = 64 + row * (tileH + 8);
          const isLeaderHere = p.leader === brokerId;
          const topic = cluster.topics.find((t) => t.name === p.topic)!;
          const color = topic?.color || "#64748b";
          const stroke = isLeaderHere ? "#fbbf24" : "#1f2937"; // amber for leader
          const opacity = p.offline ? 0.35 : 1;
          const gSel = selectedGroup ? groups.find((g) => g.id === selectedGroup) : null;
          const pk = partitionKey(p.topic, p.id);
          const committed = gSel ? gSel.offsets[pk] || 0 : 0;
          const lag = gSel ? p.endOffset - committed : 0;
          const owner = gSel ? gSel.assignments[pk] : undefined;
          return (
            <g key={`${p.topic}-${p.id}-${brokerId}`} onClick={() => onClickPartition(p)} style={{ cursor: "pointer" }}>
              <rect x={x} y={y} width={tileW} height={tileH} rx={10} ry={10} fill={color} opacity={opacity} stroke={stroke} strokeWidth={isLeaderHere ? 2.5 : 1.2} />
              <text x={x + 10} y={y + 18} fill="#0b1220" fontSize={12} fontWeight={700}>{`${p.topic} · p${p.id}`}</text>
              <text x={x + 10} y={y + 34} fill="#0b1220" fontSize={11}>
                {isLeaderHere ? "leader" : "follower"}{p.offline ? " · offline" : ""}
              </text>
              <text x={x + 10} y={y + 50} fill="#0b1220" fontSize={11}>
                <tspan>
                  end:{p.endOffset}
                  {gSel && ` · ${gSel.id} off:${committed} lag:${Math.max(0, lag)}`}
                </tspan>
                {owner && (
                  <tspan x={x + 10} dy={14}>
                    owns:{owner}
                  </tspan>
                )}
              </text>
              {isLeaderHere && <Crown color="#fbbf24" size={16} style={{ position: "absolute" }} />}
            </g>
          );
        })}
      </g>
    );
  }

  return (
    <div className="h-full w-full bg-[#0b1220] text-slate-200">
      {/* Controls */}
      <div className="flex flex-wrap items-center gap-3 border-b border-slate-800 p-3">
        <div className="font-semibold text-slate-100">Kafka Interactive Diagram (React)</div>
        <div className="ml-auto flex flex-wrap items-center gap-2">
          <label className="flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-900 px-2 py-1">
            <span className="text-xs opacity-80">Brokers</span>
            <input
              className="w-16 bg-transparent text-right outline-none"
              type="number"
              min={1}
              max={12}
              value={brokerCount}
              onChange={(e) => setBrokerCount(Math.max(1, Math.min(12, parseInt(e.target.value || "1"))))}
            />
          </label>
          <button
            className="inline-flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-900 px-3 py-1 hover:bg-slate-800"
            onClick={failRandomBroker}
            title="Fail a random broker"
          >
            <AlertTriangle size={16} /> Fail random broker
          </button>
          <button
            className="inline-flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-900 px-3 py-1 hover:bg-slate-800"
            onClick={() => setAllUp(true)}
            title="Bring all brokers up"
          >
            <RefreshCw size={16} /> Heal all
          </button>
          <div className="hidden md:flex h-6 w-px bg-slate-800" />
          <button
            className="inline-flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-900 px-3 py-1 hover:bg-slate-800"
            onClick={() => setFilterTopic(null)}
            title="Clear topic filter"
          >
            <Crosshair size={16} /> Clear filter
          </button>
        </div>
      </div>

      {/* Topic Config */}
      <div className="flex flex-wrap items-center gap-2 border-b border-slate-800 p-3">
        {topicsSpec.map((t, idx) => (
          <div key={idx} className="flex items-center gap-2 rounded-xl border border-slate-800 bg-slate-900/50 px-3 py-2">
            <button
              className="h-4 w-4 rounded-sm"
              style={{ background: PALETTE[idx % PALETTE.length] }}
              onClick={() => onClickTopic(t.name)}
              title="Filter by topic"
            />
            <input
              className="w-28 rounded-md border border-slate-700 bg-slate-950/60 px-2 py-1 outline-none"
              value={t.name}
              onChange={(e) => updateTopicSpec(idx, "name", e.target.value)}
            />
            <label className="text-xs opacity-80">parts</label>
            <input
              className="w-16 rounded-md border border-slate-700 bg-slate-950/60 px-2 py-1 text-right outline-none"
              type="number"
              min={1}
              value={t.partitions}
              onChange={(e) => updateTopicSpec(idx, "partitions", e.target.value)}
            />
            <label className="text-xs opacity-80">RF</label>
            <input
              className="w-14 rounded-md border border-slate-700 bg-slate-950/60 px-2 py-1 text-right outline-none"
              type="number"
              min={1}
              max={brokerCount}
              value={t.replicationFactor}
              onChange={(e) => updateTopicSpec(idx, "replicationFactor", Math.min(brokerCount, parseInt(e.target.value || "1")))}
            />
            <button
              className="rounded-md border border-slate-700 bg-slate-900 px-2 py-1 text-xs hover:bg-slate-800"
              onClick={() => removeTopic(idx)}
            >
              remove
            </button>
          </div>
        ))}
        <button className="rounded-lg border border-slate-700 bg-slate-900 px-3 py-2 hover:bg-slate-800" onClick={addTopic}>
          + add topic
        </button>

        {/* Producer controls */}
        <div className="ml-auto flex flex-wrap items-center gap-2 rounded-xl border border-slate-800 bg-slate-900/50 px-3 py-2">
          <span className="text-xs opacity-80 mr-1">producer</span>
          <select className="rounded-md border border-slate-700 bg-slate-950/60 px-2 py-1" value={produceTopic} onChange={(e) => setProduceTopic(e.target.value)}>
            {cluster.topics.map((t) => (
              <option key={t.name} value={t.name}>{t.name}</option>
            ))}
          </select>
          <input className="w-32 rounded-md border border-slate-700 bg-slate-950/60 px-2 py-1" placeholder="key (optional)" value={produceKey} onChange={(e) => setProduceKey(e.target.value)} />
          <button className="inline-flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-900 px-3 py-1 hover:bg-slate-800" onClick={produce}>
            <Send size={14}/> publish
          </button>
          <span className="text-xs opacity-70">no key ⇒ random partition • key ⇒ sticky partition</span>
        </div>
      </div>

      {/* Diagram Area */}
      <div className="grid grid-cols-1 md:grid-cols-[1fr_380px] gap-0">
        <div className="relative">
          <div style={{ marginTop: 40 }} /> {/* Add margin above the SVG */}
          <svg ref={svgRef} width={width} height={height} className="block">
            <defs>
              <pattern id="diag" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
                <rect width="6" height="6" fill="#0b1220" />
                <rect width="3" height="6" fill="#0f172a" />
              </pattern>
            </defs>
            <g ref={gRef}>
              {/* Cluster frame */}
              <rect x={8} y={8} width={width - 16} height={height - 16} rx={16} ry={16} fill="url(#diag)" stroke="#1f2937" />
              <text x={24} y={28} fontSize={12} fill="#94a3b8">Kafka Cluster</text>

              {/* Brokers */}
              {layout.map(({ broker, x, y, w, h }) => (
                <g key={broker.id} transform={`translate(${x + 12}, ${y + 20})`}>
                  <rect
                    width={w - 24}
                    height={h}
                    rx={16}
                    ry={16}
                    fill={broker.up ? "#0f172a" : "#1f2937"}
                    stroke={broker.up ? "#334155" : "#ef4444"}
                    strokeWidth={broker.up ? 1.2 : 2}
                  />
                  <g transform="translate(12, 14)">
                    <Server size={16} color={broker.up ? "#a3e635" : "#ef4444"} />
                  </g>
                  <text x={36} y={26} fontSize={13} fill="#e5e7eb" className="select-none">
                    {broker.name} {broker.up ? "" : "(down)"}
                  </text>
                  {/* Toggle button */}
                  <g onClick={() => toggleBroker(broker.id)} style={{ cursor: "pointer" }} transform={`translate(${w - 120}, 10)`}>
                    <rect width={96} height={28} rx={8} fill="#111827" stroke="#374151" />
                    <text x={10} y={19} fontSize={12} fill="#cbd5e1">
                      {broker.up ? "bring down" : "bring up"}
                    </text>
                  </g>

                  {/* tiles */}
                  {renderBrokerTiles(broker.id, w - 24)}
                </g>
              ))}
            </g>
          </svg>
        </div>

        {/* Inspector / Consumer Groups */}
        <div className="border-l border-slate-800 p-3 space-y-3">
          <div className="text-sm font-semibold text-slate-100">Inspector</div>
          {!selected && <div className="text-sm opacity-80">Click a broker or partition to see details.</div>}
          {selected?.kind === "broker" && (
            <div className="space-y-2 text-sm">
              <div className="font-semibold">{(selected.extra as Broker).name}</div>
              <div>Status: {(selected.extra as Broker).up ? "up" : "down"}</div>
              <div>Partitions hosted: {partitionsOnBroker(cluster, selected.extra.id).length}</div>
            </div>
          )}
          {selected?.kind === "partition" && (
            <div className="space-y-2 text-sm">
              {(() => {
                const p = selected.extra as Partition;
                const t = cluster.topics.find((x) => x.name === p.topic)!;
                const isr = p.replicas.filter((r) => cluster.brokers.find((b) => b.id === r)?.up);
                const gSel = selectedGroup ? groups.find((g) => g.id === selectedGroup) : null;
                const pk = partitionKey(p.topic, p.id);
                const committed = gSel ? gSel.offsets[pk] || 0 : 0;
                const lag = gSel ? Math.max(0, p.endOffset - committed) : 0;
                return (
                  <>
                    <div className="font-semibold">
                      {p.topic} / partition {p.id}
                    </div>
                    <div className="flex items-center gap-2">
                      <Crown size={16} color="#fbbf24" />
                      <span>Leader: {p.leader ?? "none (offline)"}</span>
                    </div>
                    <div>Replicas: {p.replicas.join(", ")}</div>
                    <div>ISR (in-sync replicas): {isr.join(", ") || "—"}</div>
                    <div>End Offset: {p.endOffset}</div>
                    {gSel && (
                      <>
                        <div>Group {gSel.id} Offset: {committed}</div>
                        <div>Lag: {lag}</div>
                        <div>Assigned to: {gSel.assignments[pk] || "—"}</div>
                      </>
                    )}
                    <div>
                      Status: {p.offline ? <span className="text-red-400">offline (no leader)</span> : <span className="text-emerald-400">online</span>}
                    </div>
                    <div className="h-2 w-full rounded bg-slate-800">
                      <div className="h-2 rounded" style={{ width: `${(isr.length / p.replicas.length) * 100}%`, background: t.color }} />
                    </div>
                  </>
                );
              })()}
            </div>
          )}
          {selected?.kind === "topic" && (
            <div className="space-y-2 text-sm">
              <div className="font-semibold">Topic: {selected.id}</div>
              <div>Filter active — only showing tiles for this topic.</div>
            </div>
          )}

          <div className="my-4 h-px w-full bg-slate-800" />

          {/* Consumer Groups Panel */}
          <div className="text-sm font-semibold text-slate-100 flex items-center gap-2"><Layers size={16}/> Consumer Groups</div>
          <div className="space-y-2">
            {groups.map((g) => {
              const { totalLag } = groupLagSummary(g);
              return (
                <div key={g.id} className={`rounded-xl border ${selectedGroup === g.id ? "border-amber-400" : "border-slate-800"} bg-slate-900/50 p-2`}>
                  <div className="flex items-center gap-2">
                    <button className={`rounded-md px-2 py-1 text-xs border ${selectedGroup === g.id ? "border-amber-400 bg-amber-400/10" : "border-slate-700 bg-slate-900"}`} onClick={() => setSelectedGroup(g.id)}>
                      <Target size={12}/> {g.id}
                    </button>
                    <div className="text-xs opacity-80">members: {g.members.length} • total lag: {totalLag}</div>
                    <div className="ml-auto flex items-center gap-2">
                      <button className="rounded-md border border-slate-700 bg-slate-900 px-2 py-1 text-xs" onClick={() => consumeOnce(g.id)}>consume 1/message</button>
                      <button className="rounded-md border border-slate-700 bg-slate-900 px-2 py-1 text-xs" onClick={() => setGroups(gs => gs.map(gg => gg.id===g.id?{...gg, offsets:{}}:gg))}>reset offsets</button>
                    </div>
                  </div>
                  <div className="mt-2 flex flex-wrap items-center gap-2">
                    {g.members.map((m) => (
                      <span key={m.id} className="rounded-full border border-slate-700 bg-slate-800 px-2 py-0.5 text-xs">{m.id}</span>
                    ))}
                    <button className="inline-flex items-center gap-1 rounded-md border border-slate-700 bg-slate-900 px-2 py-1 text-xs" onClick={() => setGroups(prev => prev.map(gg => gg.id===g.id?{...gg, members:[...gg.members,{id:`c${gg.members.length+1}` }]}:gg))}><Plus size={12}/> add consumer</button>
                    <button className="inline-flex items-center gap-1 rounded-md border border-slate-700 bg-slate-900 px-2 py-1 text-xs" onClick={() => setGroups(prev => prev.map(gg => gg.id===g.id?{...gg, members:gg.members.slice(0,-1)}:gg))}><Minus size={12}/> remove</button>
                  </div>
                  <div className="mt-2 text-[11px] leading-5">
                    {/* show few assignments */}
                    {cluster.topics.slice(0,2).map(t => (
                      <div key={t.name} className="opacity-80">{t.name}: {t.partitions.map(p => `${p.id}->${g.assignments[partitionKey(t.name,p.id)]||"-"}`).join("  ")}</div>
                    ))}
                  </div>
                </div>
              );
            })}
            <button className="rounded-lg border border-slate-700 bg-slate-900 px-3 py-2 text-xs hover:bg-slate-800" onClick={() => setGroups(prev => [...prev, { id: `g${prev.length+1}`, members: [{id:"c1"}], assignments: {}, offsets: {} }])}>
              + add group
            </button>
          </div>

          <div className="my-4 h-px w-full bg-slate-800" />
          <div className="mb-2 text-sm font-semibold text-slate-100">Topics</div>
          <div className="flex flex-wrap gap-2">
            {topicLegend.map((t) => (
              <button
                key={t.name}
                onClick={() => onClickTopic(t.name)}
                className={`inline-flex items-center gap-2 rounded-full border px-2 py-1 text-xs ${
                  filterTopic === t.name ? "border-amber-400 bg-amber-400/10" : "border-slate-700 bg-slate-900"
                }`}
                title="Filter by topic"
              >
                <span className="h-3 w-3 rounded" style={{ background: t.color }} />
                {t.name}
              </button>
            ))}
          </div>

          <div className="my-4 h-px w-full bg-slate-800" />
          <div className="text-xs opacity-70 space-y-1">
            <div>• Offsets advance as consumers in a group read; each group has its own offsets.</div>
            <div>• Lag = (endOffset − committedOffset) per partition.</div>
            <div>• Same groupId → partitions are shared (one consumer per partition at a time); different groupIds → each group reads the full stream.</div>
            <div>• Producer without key ⇒ random partition; with key ⇒ same partition (sticky by hash).</div>
          </div>
        </div>
      </div>
    </div>
  );
}
