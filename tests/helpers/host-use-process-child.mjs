import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { openHostStorageV1 } from 'pi-distill/host-storage';

const [mode, databasePath, sourceJson] = process.argv.slice(2);
if ((mode !== 'opening' && mode !== 'attached') || !databasePath || !sourceJson) {
  throw new Error('Expected mode, database path, and source reference.');
}

const source = JSON.parse(sourceJson);
const storage = openHostStorageV1(databasePath);
const reservation = storage.beginHostUse(source);
if (!reservation) throw new Error('Synthetic child could not reserve host use.');

const child = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore' });
await once(child, 'spawn');
const childIdentity = mode === 'attached' ? reservation.attachChild(child.pid) : null;
process.send?.({ id: reservation.id, childPid: child.pid, childIdentity });
setInterval(() => {}, 1000);
