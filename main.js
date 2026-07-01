const { app, BrowserWindow, ipcMain, desktopCapturer } = require('electron');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const { exec } = require('child_process');

// ─── HARDWARE FINGERPRINT ─────────────────────────────────────────────────────
function generateHardwareFingerprint() {
  const cpus = os.cpus();
  const raw = [
    os.platform(), 
    os.release(), 
    os.arch(), 
    cpus[0]?.model || 'unknown', 
    cpus.length.toString(), 
    os.totalmem().toString(), 
    os.hostname()
  ].join('|');
  return crypto.createHash('sha256').update(raw).digest('hex');
}

function getHardwareInfo() {
  const cpus = os.cpus();
  return {
    fingerprint: generateHardwareFingerprint(),
    platform: os.platform() + ' ' + os.release(),
    arch: os.arch(),
    cpuModel: cpus[0]?.model || 'Unknown CPU',
    cores: cpus.length,
    totalRamGB: Math.round(os.totalmem() / (1024 ** 3)),
  };
}

// ─── ROBOTJS / NUT-JS INPUT SIMULATION ───────────────────────────────────────
let robot = null;
try {
  robot = require('@nut-tree/nut-js');
} catch (_) {
  try {
    robot = require('robotjs');
    robot._isRobotJs = true;
  } catch (__) {
    console.warn('[Input] No input simulation library found. Fallback to PowerShell active.');
  }
}

// ─── PURE JS OS INPUT SIMULATION WORKAROUND ──────────────────────────────
// Coalesce/throttle high-frequency packets (mouse-move especially) so the
// PowerShell fallback never spawns more than ~1 exec() per interval. Without
// this, a touch-drag stream can fork dozens of processes/sec and stall the
// main process event loop — which freezes desktopCapturer + the WebRTC video
// pipeline along with it.
const MOVE_THROTTLE_MS = 33; // ~30/sec ceiling, well under what shell exec can keep up with
let lastMoveTime = 0;
let pendingMove = null;
let moveFlushTimer = null;

function flushPendingMove() {
  moveFlushTimer = null;
  if (!pendingMove) return;
  const pkt = pendingMove;
  pendingMove = null;
  lastMoveTime = Date.now();
  dispatchInput(pkt);
}

async function simulateInput(pkt) {
  if (!pkt) return;

  // Mouse-move packets get throttled/coalesced — only the latest position
  // within a window actually fires a native call.
  if (pkt.t === 'mm') {
    const now = Date.now();
    pendingMove = pkt;
    if (now - lastMoveTime >= MOVE_THROTTLE_MS) {
      flushPendingMove();
    } else if (!moveFlushTimer) {
      moveFlushTimer = setTimeout(flushPendingMove, MOVE_THROTTLE_MS - (now - lastMoveTime));
    }
    return;
  }

  // Everything else (clicks, key events) goes straight through — these are
  // low-frequency and shouldn't be delayed.
  dispatchInput(pkt);
}

async function dispatchInput(pkt) {
  try {
    // If native desktop control module loaded successfully, prioritize it
    if (robot) {
      if (robot._isRobotJs) {
        if (pkt.t === 'mm' && pkt.x !== undefined && pkt.y !== undefined) {
          robot.moveMouse(pkt.x, pkt.y);
          return;
        }
        if (pkt.t === 'click' || pkt.t === 'md') {
          robot.mouseClick();
          return;
        }
        if (pkt.t === 'kd' && pkt.k) {
          robot.keyTap(pkt.k.toLowerCase());
          return;
        }
      }
    }

    // OS Native PowerShell Failover Path
    switch (pkt.t) {
      case 'mm': // Mouse Move
        if (pkt.x !== undefined && pkt.y !== undefined) {
          const command = `powershell -Command "[void][System.Reflection.Assembly]::LoadWithPartialName('System.Windows.Forms'); [System.Windows.Forms.Cursor]::Position = New-Object System.Drawing.Point(${pkt.x}, ${pkt.y});"`;
          exec(command, (err) => { if (err) console.error('[Input Move Failure]:', err.message); });
        }
        break;

      case 'md': // Mouse Down / Click Down
      case 'click':
        const clickCommand = `powershell -Command "[void][System.Reflection.Assembly]::LoadWithPartialName('System.Windows.Forms'); $sim = New-Object -ComObject WScript.Shell; [System.Runtime.InteropServices.Marshal]::PrelinkAll([System.Windows.Forms.Control]); Add-Type -TypeDefinition 'using System; using System.Runtime.InteropServices; public class Mouse { [DllImport(\\\"user32.dll\\\")] public static extern void mouse_event(int flags, int dx, int dy, int data, int extra); }'; [Mouse]::mouse_event(0x0002 -or 0x0004, 0, 0, 0, 0);"`;
        exec(clickCommand, (err) => { if (err) console.error('[Input Click Failure]:', err.message); });
        break;

      case 'kd': // Key Down
        if (pkt.k) {
          let keystroke = pkt.k;
          const keyCommand = `powershell -Command "$wshell = New-Object -ComObject WScript.Shell; $wshell.SendKeys('${keystroke}');"`;
          exec(keyCommand, (err) => { if (err) console.error('[Input Key Failure]:', err.message); });
        }
        break;
        
      default:
        break;
    }
  } catch (err) {
    console.error('[Input Backup Engine Failure]:', err.message);
  }
}

// ─── WINDOW MANAGEMENT ─────────────────────────────────────────────────────────
function createWindow() {
  const win = new BrowserWindow({
    width: 1280,
    height: 820,
    minWidth: 1200,
    minHeight: 800,
    frame: false,
    titleBarStyle: 'hidden',
    backgroundColor: '#0a0a12',
    webPreferences: {
      nodeIntegration: true,
      contextIsolation: false,
      devTools: true,
    },
    icon: path.join(__dirname, 'assets', 'icon.png')
  });

  win.loadFile('index.html');
  win.setMenuBarVisibility(false);

  // Window controls handlers
  ipcMain.on('win-minimize', () => { if (win) win.minimize(); });
  ipcMain.on('win-maximize', () => { 
    if (!win) return;
    if (win.isMaximized()) win.unmaximize(); 
    else win.maximize(); 
  });
  ipcMain.on('win-close', () => { if (win) win.close(); });

  // Expose Hardware fingerprint structure payload to Renderer process
  ipcMain.handle('get-hardware-info', () => getHardwareInfo());

  // FIX: Secure Main Process Screen Capture Bridge for Renderer Lifecycle
  ipcMain.handle('get-screen-sources', async () => {
    try {
      return await desktopCapturer.getSources({ types: ['screen', 'window'] });
    } catch (err) {
      console.error('[IPC Main] Failed to fetch display streams:', err);
      return [];
    }
  });

  // Input simulation relay from DataChannel streams
  ipcMain.on('simulate-input', (_event, pkt) => { simulateInput(pkt); });

  win.on('close', (e) => {
    // Future Expansion Hook: Background Tray Service mapping routine goes here
  });
}

// ─── RUNTIME APP HANDLERS ─────────────────────────────────────────────────────
app.whenReady().then(createWindow);

app.on('window-all-closed', () => { 
  if (process.platform !== 'darwin') app.quit(); 
});

app.on('activate', () => { 
  if (BrowserWindow.getAllWindows().length === 0) createWindow(); 
});