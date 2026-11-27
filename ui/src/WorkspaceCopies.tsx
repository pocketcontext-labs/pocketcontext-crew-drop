import { useEffect, useRef, useState } from "react";
import { pb } from "./api";

type Copy = { id: string; created: string; bytes: number; remote: boolean; available: boolean };
type Inventory = { copies: Copy[]; funded: boolean };
const path = "/api/workspace/copies";

export default function WorkspaceCopies() {
  const [inventory, setInventory] = useState<Inventory>();
  const [selected, setSelected] = useState("");
  const [confirming, setConfirming] = useState(false);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const pendingId = useRef("");
  async function refresh() {
    const next = await pb.send<Inventory>(path, { method: "GET", cache: "no-store" });
    setInventory(next);
    setSelected(previous => next.copies.some(item => item.id === previous) ? previous : next.copies.find(item => item.available)?.id || "");
  }
  useEffect(() => { refresh().catch(() => setMessage("Could not load recovery copies.")); }, []);
  async function run(restore: boolean) {
    setBusy(true);setMessage("");
    try {
      if (restore) {
        await pb.send(path + "/restore", { method: "POST", body: { id: selected, confirm: selected } });
        setMessage("Restore accepted. This workspace is restarting. Sign in again at this address.");
        setConfirming(false);
      } else {
        pendingId.current ||= crypto.randomUUID().replaceAll("-", "");
        await pb.send(path, { method: "POST", body: { id: pendingId.current } });
        pendingId.current = "";
        await refresh();
        setMessage("Recovery copy saved and verified.");
      }
    } catch {
      setMessage(restore ? "Restore could not be confirmed. Reload and check this workspace before retrying." : "Copy could not be confirmed. Check storage and retry.");
    } finally {setBusy(false);}
  }
  return <section aria-labelledby="workspace-backups">
    <h2 id="workspace-backups">Backup and restore</h2>
    {inventory && <p>{inventory.funded ? "New copies include records and attachments on this volume and in funded remote storage." : "New copies stay on this volume. A local-only copy cannot recover a lost host."}</p>}
    <button type="button" disabled={busy || !inventory} onClick={() => run(false)}>Create verified copy</button>
    {inventory?.copies.length ? <>
      <label>Recovery copy <select value={selected} disabled={busy} onChange={e => { setSelected(e.target.value);setConfirming(false); }}>
        {inventory.copies.map(item => <option disabled={!item.available} key={item.id} value={item.id}>{new Date(item.created).toLocaleString()} · {!item.available ? "Unavailable here" : item.remote ? "Funded backup" : "Local only"} · {Math.ceil(item.bytes / 1024)} KiB</option>)}
      </select></label>
      {confirming ? <div>
        <p>Replace the records and attachments in this workspace with the selected copy? Changes made since that copy will be removed.</p>
        <button type="button" disabled={busy} onClick={() => run(true)}>Replace workspace with this copy</button>
        <button type="button" disabled={busy} onClick={() => setConfirming(false)}>Cancel</button>
      </div> : <button type="button" disabled={busy || !inventory.copies.find(item => item.id === selected)?.available} onClick={() => setConfirming(true)}>Review restore</button>}
    </> : null}
    <p role="status">{busy ? "Working…" : message}</p>
  </section>;
}
