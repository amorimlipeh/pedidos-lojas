import express from "express";
import path from "path";
import fs from "fs";
import jwt from "jsonwebtoken";
import bcrypt from "bcryptjs";
import multer from "multer";
import pdfParse from "pdf-parse";
import mammoth from "mammoth";
import xlsx from "xlsx";
import PDFDocument from "pdfkit";
import { fileURLToPath } from "url";

const app = express();
const PORT = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || "pedidos_lojas_secret";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DATA_DIR = path.join(__dirname, "data");
const UPLOAD_DIR = path.join(DATA_DIR, "uploads");
const DB_FILE = path.join(DATA_DIR, "db.json");

fs.mkdirSync(DATA_DIR, { recursive: true });
fs.mkdirSync(UPLOAD_DIR, { recursive: true });

const upload = multer({ dest: UPLOAD_DIR });
app.use(express.json({ limit: "15mb" }));
app.use(express.urlencoded({ extended: true }));
app.use(express.static(__dirname));

function nowIso() { return new Date().toISOString(); }
function num(v) {
  if (v === null || v === undefined || v === "") return 0;
  if (typeof v === "number") return Number.isFinite(v) ? v : 0;
  const s = String(v).trim().replace(/\.(?=\d{3}(\D|$))/g, "").replace(",", ".");
  const n = Number(s);
  return Number.isFinite(n) ? n : 0;
}
function codePrefix(code) { return String(code || "").trim().toUpperCase().slice(0, 3); }
function sanitizeUser(v) { return String(v || "").trim().toLowerCase(); }
function isQty(v){ return /^-?\d[\d.]*,\d+$/.test(String(v||"").trim()) || /^-?\d+(?:\.\d+)?$/.test(String(v||"").trim()); }
function normalizeLine(line){ return String(line || "").replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim(); }
function defaultStores(){ const out = {}; for(let i=1;i<=20;i++) out[String(i).padStart(2, "0")] = `Loja ${String(i).padStart(2, "0")}`; return out; }
function ensureStoreContainers(db, storeId){
  db.order_drafts_by_store ||= {};
  db.order_history_by_store ||= {};
  db.shortage_history_by_store ||= {};
  db.order_sequence_by_store ||= {};
  if(!db.order_drafts_by_store[storeId]) db.order_drafts_by_store[storeId] = [];
  if(!db.order_history_by_store[storeId]) db.order_history_by_store[storeId] = [];
  if(!db.shortage_history_by_store[storeId]) db.shortage_history_by_store[storeId] = [];
  if(!db.order_sequence_by_store[storeId]) db.order_sequence_by_store[storeId] = 0;
}
function seedUsers() {
  return [
    { username: "admin", password_hash: bcrypt.hashSync(process.env.ADMIN_PASSWORD || "admin", 10), role: "admin", created_at: nowIso() },
    { username: "funcionario", password_hash: bcrypt.hashSync(process.env.FUNC_PASSWORD || "func123", 10), role: "funcionario", created_at: nowIso() },
    { username: "separador", password_hash: bcrypt.hashSync(process.env.SEP_PASSWORD || "sep123", 10), role: "separador", created_at: nowIso() },
  ];
}
function ensureDb(){
  if(!fs.existsSync(DB_FILE)) {
    const db = { stores: defaultStores(), users: seedUsers(), products: {}, stock_imports: [], order_drafts_by_store: {}, order_history_by_store: {}, shortage_history_by_store: {}, order_sequence_by_store: {} };
    for (const storeId of Object.keys(db.stores)) ensureStoreContainers(db, storeId);
    fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));
    return;
  }
  const db = JSON.parse(fs.readFileSync(DB_FILE, "utf8"));
  db.stores ||= defaultStores();
  db.users ||= seedUsers();
  db.products ||= {};
  db.stock_imports ||= [];
  db.order_drafts_by_store ||= {};
  db.order_history_by_store ||= {};
  db.shortage_history_by_store ||= {};
  db.order_sequence_by_store ||= {};
  for (const storeId of Object.keys(db.stores)) ensureStoreContainers(db, storeId);
  if (!db.users.find(u => u.username === 'admin')) db.users.push({ username: "admin", password_hash: bcrypt.hashSync(process.env.ADMIN_PASSWORD || "admin", 10), role: "admin", created_at: nowIso() });
  if (!db.users.find(u => u.username === 'funcionario')) db.users.push({ username: "funcionario", password_hash: bcrypt.hashSync(process.env.FUNC_PASSWORD || "func123", 10), role: "funcionario", created_at: nowIso() });
  if (!db.users.find(u => u.username === 'separador')) db.users.push({ username: "separador", password_hash: bcrypt.hashSync(process.env.SEP_PASSWORD || "sep123", 10), role: "separador", created_at: nowIso() });
  fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));
}
function readDb(){ ensureDb(); return JSON.parse(fs.readFileSync(DB_FILE, "utf8")); }
function writeDb(db){ fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2)); }

function authRequired(req, res, next){
  try {
    const auth = req.headers.authorization || "";
    const [, token] = auth.split(" ");
    if (!token) return res.status(401).json({ error: "Token ausente" });
    req.user = jwt.verify(token, JWT_SECRET);
    next();
  } catch {
    return res.status(401).json({ error: "Token inválido" });
  }
}
function requireRole(...roles){ return (req, res, next) => (!req.user || !roles.includes(req.user.role)) ? res.status(403).json({ error: "Sem permissão" }) : next(); }

function upsertProduct(db, product){
  const code = String(product.code || "").trim().toUpperCase();
  if (!code) return;
  const prev = db.products[code] || { code, product: "", material: "", stock: 0, factor: 1, source: "" };
  db.products[code] = {
    code,
    product: String(product.product ?? prev.product ?? "").trim(),
    material: String(product.material ?? prev.material ?? "").trim(),
    stock: Number.isFinite(product.stock) ? product.stock : prev.stock,
    factor: Number.isFinite(product.factor) && product.factor > 0 ? product.factor : (prev.factor || 1),
    source: Array.from(new Set([prev.source, product.source].filter(Boolean))).join(" | "),
    updated_at: nowIso(),
  };
}
function mergeByCode(items){
  const map = {};
  for(const item of items){
    const code = String(item.code || "").trim().toUpperCase();
    if(!code) continue;
    const prev = map[code];
    if(!prev){ map[code] = { ...item, code, stock: num(item.stock), factor: num(item.factor) || 1 }; continue; }
    map[code] = { ...prev, product: item.product || prev.product, material: item.material || prev.material, stock: Math.max(num(prev.stock), num(item.stock)), factor: prev.factor || item.factor || 1, source: Array.from(new Set([prev.source, item.source].filter(Boolean))).join(" | ") };
  }
  return Object.values(map);
}
function parseSimplifiedPdf(text, source){
  const lines = String(text || "").replace(/\r/g,"").split("\n").map(normalizeLine).filter(Boolean);
  const products = [];
  for(let i=0;i<lines.length;i++){
    const line = lines[i];
    const m = line.match(/^\(([A-Z0-9]{4,10})\)$/i);
    if(!m) continue;
    const code = m[1].toUpperCase();
    let product = ""; let qty = 0; let j = i + 1;
    while(j < lines.length && !product){ const ln = lines[j]; if (/^(produto|matéria do produto|qtde\.? estoque|estoque simplificado|arquivo gerado)/i.test(ln)) { j++; continue; } if (ln.match(/^\([A-Z0-9]{4,10}\)$/i)) break; if (!isQty(ln)) product = ln; j++; }
    while(j < lines.length){ const ln = lines[j]; if (isQty(ln)) { qty = num(ln); break; } if (ln.match(/^\([A-Z0-9]{4,10}\)$/i)) break; j++; }
    if (product) products.push({ code, product, material: product, stock: qty, factor: 1, source });
  }
  return mergeByCode(products);
}
function parseOriginalPdf(text, source){
  const lines = String(text || "").replace(/\r/g,"").split("\n").map(normalizeLine).filter(Boolean);
  const products = [];
  for(const line of lines){
    if (/^(produto|grupo de estoque|peso|local:|grupo:|erp |pág:|1-jf comercio)/i.test(line)) continue;
    const m = line.match(/UN([A-Z0-9]{4,10})\s*-\s*(.+?)\s+1$/i);
    if (m) {
      const code = m[1].toUpperCase();
      const product = m[2].trim();
      const prefix = line.slice(0, m.index);
      const nums = (prefix.match(/\d[\d.,]*/g) || []).map(x => x.trim()).filter(Boolean);
      let stock = 0;
      if (nums.length >= 2) stock = num(nums[nums.length - 2]); else if (nums.length >= 1) stock = num(nums[nums.length - 1]);
      products.push({ code, product, material: product, stock, factor: 1, source });
    }
  }
  return mergeByCode(products);
}
function parseWorkbook(filePath, source){
  const wb = xlsx.readFile(filePath, { cellDates: false });
  const products = [];
  for (const sheet of wb.SheetNames) {
    const rows = xlsx.utils.sheet_to_json(wb.Sheets[sheet], { header: 1, defval: "" });
    for (const row of rows) {
      const values = row.map(v => String(v ?? '').trim()).filter(Boolean);
      const joined = values.join(' | ');
      let code = ''; let product = ''; let stock = 0;
      const m1 = joined.match(/\b([A-Z0-9]{4,10})\s*-\s*([^|]+)/i);
      if (m1) { code = m1[1].toUpperCase(); product = m1[2].trim(); }
      else {
        const c0 = String(row[0] || '').trim(); const c1 = String(row[1] || '').trim();
        if (/^[A-Z0-9]{4,10}$/i.test(c0)) { code = c0.toUpperCase(); product = c1 || c0; }
      }
      if (!code) continue;
      const numericCells = row.map(num).filter(v => Number.isFinite(v));
      if (row.length >= 3) stock = num(row[2]);
      if (!stock && numericCells.length) stock = numericCells[numericCells.length - 1];
      products.push({ code, product, material: product, stock, factor: 1, source });
    }
  }
  return mergeByCode(products);
}
async function parseFile(file){
  const ext = path.extname(file.originalname || "").toLowerCase();
  const source = file.originalname || "arquivo";
  if(ext === ".pdf"){
    const data = await pdfParse(fs.readFileSync(file.path));
    const text = data.text || "";
    if (/Estoque Simplificado/i.test(text) || /\(\w{4,10}\)\s*\n/i.test(text)) return { imported_type: "pdf-simplificado", products: parseSimplifiedPdf(text, source) };
    return { imported_type: "pdf-original", products: parseOriginalPdf(text, source) };
  }
  if(ext === ".xlsx" || ext === ".xls") return { imported_type: "excel", products: parseWorkbook(file.path, source) };
  if(ext === ".docx"){ const result = await mammoth.extractRawText({ path: file.path }); return { imported_type: "word", products: parseSimplifiedPdf(result.value, source) }; }
  if(ext === ".csv" || ext === ".txt"){ const txt = fs.readFileSync(file.path, "utf8"); return { imported_type: ext.slice(1), products: parseSimplifiedPdf(txt, source) }; }
  throw new Error(`Formato não suportado: ${ext || 'desconhecido'}`);
}

function sanitizeOrderItem(x){
  const factor = Math.max(1, num(x.factor) || 1);
  const boxes = Math.max(0, num(x.boxes));
  const units = x.units === undefined || x.units === null || x.units === "" ? boxes * factor : Math.max(0, num(x.units));
  const stock = Math.max(0, num(x.stock));
  return { code: String(x.code || "").trim().toUpperCase(), product: String(x.product || "").trim(), factor, boxes, units, stock, insufficient: units > stock };
}
function toShortageItem(orderItem){
  const requested_boxes = Math.max(0, num(orderItem.boxes));
  const factor = Math.max(1, num(orderItem.factor) || 1);
  const requested_units = Math.max(0, num(orderItem.units || (requested_boxes * factor)));
  return { code: String(orderItem.code || '').trim().toUpperCase(), product: String(orderItem.product || '').trim(), factor, requested_boxes, requested_units, not_sent: false, left_boxes: 0, left_units: 0, sent_boxes: requested_boxes, sent_units: requested_units };
}
function normalizeShortageItem(x){
  const factor = Math.max(1, num(x.factor) || 1);
  const requested_boxes = Math.max(0, num(x.requested_boxes));
  const not_sent = Boolean(x.not_sent);
  const left_boxes = Math.max(0, num(x.left_boxes));
  const sent_boxes = not_sent ? 0 : Math.max(0, requested_boxes - left_boxes);
  return { code: String(x.code || '').trim().toUpperCase(), product: String(x.product || '').trim(), factor, requested_boxes, requested_units: requested_boxes * factor, not_sent, left_boxes, left_units: left_boxes * factor, sent_boxes, sent_units: sent_boxes * factor };
}
function summarizeOrder(order){
  return { order_no: order.order_no, created_at: order.created_at, item_count: order.items.length, total_boxes: order.items.reduce((s,x)=>s+num(x.boxes),0), total_units: order.items.reduce((s,x)=>s+num(x.units),0), status: order.status || 'aberta' };
}
function summarizeShortage(rec){
  return { order_no: rec.order_no, created_at: rec.created_at, item_count: rec.items.length, total_left_boxes: rec.items.reduce((s,x)=>s+num(x.left_boxes),0), total_sent_boxes: rec.items.reduce((s,x)=>s+num(x.sent_boxes),0) };
}
function buildExportRowsForOrder(order){ return order.items.map((p, i) => ({ Item: i + 1, Código: p.code, Produto: p.product, Caixas: p.boxes, Unidades: p.units, Fator: p.factor, Estoque: p.stock, Status: p.insufficient ? 'Estoque insuficiente' : 'OK' })); }
function buildExportRowsForShortage(rec){ return rec.items.map((p, i) => ({ Item: i + 1, Código: p.code, Produto: p.product, Caixa: p.requested_boxes, 'Não foi': p.not_sent ? 'Sim' : 'Não', 'Permaneceu no estoque (caixas)': p.left_boxes, 'Enviados (caixas)': p.sent_boxes, Fator: p.factor, 'Enviados (unidades)': p.sent_units })); }
function sendPdfFromRows(res, filename, title, subtitle, rows){
  res.setHeader("Content-Type", "application/pdf");
  res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
  const pdf = new PDFDocument({ margin: 36, size: 'A4' });
  pdf.pipe(res); pdf.fontSize(16).text(title, { align: 'center' }); pdf.moveDown(0.4).fontSize(10).text(subtitle); pdf.moveDown(0.8).fontSize(9);
  if (!rows.length) pdf.text('Sem dados.');
  rows.forEach((row, idx) => { pdf.text(`${idx + 1}. ${Object.entries(row).map(([k,v]) => `${k}: ${v}`).join(' | ')}`); pdf.moveDown(0.25); });
  pdf.end();
}
function sendExcel(res, filename, sheetName, rows){ const wb = xlsx.utils.book_new(); const ws = xlsx.utils.json_to_sheet(rows.length ? rows : [{ mensagem: 'Sem dados' }]); xlsx.utils.book_append_sheet(wb, ws, sheetName); const buf = xlsx.write(wb, { bookType: 'xlsx', type: 'buffer' }); res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'); res.setHeader('Content-Disposition', `attachment; filename="${filename}"`); res.end(buf); }
function sendWord(res, filename, title, subtitle, rows){ const html = `<!DOCTYPE html><html><meta charset="utf-8"><body><h1>${title}</h1><p>${subtitle}</p><table border="1" cellspacing="0" cellpadding="4"><thead><tr>${Object.keys(rows[0] || { mensagem: 'Sem dados' }).map(k=>`<th>${k}</th>`).join('')}</tr></thead><tbody>${(rows.length ? rows : [{ mensagem: 'Sem dados' }]).map(r=>`<tr>${Object.values(r).map(v=>`<td>${v}</td>`).join('')}</tr>`).join('')}</tbody></table></body></html>`; res.setHeader('Content-Type', 'application/msword'); res.setHeader('Content-Disposition', `attachment; filename="${filename}"`); res.end(html); }
function sendPrintHtml(res, title, subtitle, rows){ const headers = Object.keys(rows[0] || { mensagem: 'Sem dados' }); const html = `<!DOCTYPE html><html><meta charset="utf-8"><title>${title}</title><style>body{font-family:Arial;padding:20px}table{border-collapse:collapse;width:100%}th,td{border:1px solid #ccc;padding:6px;text-align:left}h1{margin:0 0 8px}p{margin:0 0 16px}@media print{button{display:none}}</style><body><button onclick="window.print()">Imprimir</button><h1>${title}</h1><p>${subtitle}</p><table><thead><tr>${headers.map(h=>`<th>${h}</th>`).join('')}</tr></thead><tbody>${(rows.length ? rows : [{ mensagem: 'Sem dados' }]).map(r=>`<tr>${headers.map(h=>`<td>${r[h] ?? ''}</td>`).join('')}</tr>`).join('')}</tbody></table></body></html>`; res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.end(html); }
function nextOrderNo(db, storeId){ ensureStoreContainers(db, storeId); db.order_sequence_by_store[storeId] += 1; return `${storeId}-${String(db.order_sequence_by_store[storeId]).padStart(4,'0')}`; }
function findOrder(db, storeId, orderNo){ ensureStoreContainers(db, storeId); return db.order_history_by_store[storeId].find(o => o.order_no === orderNo); }
function findShortage(db, storeId, orderNo){ ensureStoreContainers(db, storeId); return db.shortage_history_by_store[storeId].find(o => o.order_no === orderNo); }

app.post('/api/auth/login', (req, res) => {
  const username = sanitizeUser(req.body.username); const password = String(req.body.password || ''); const db = readDb(); const user = db.users.find(u => u.username === username); const okLegacyAdmin = username === 'admin' && (password === 'admin123' || password === 'admin');
  if ((!user || !bcrypt.compareSync(password, user.password_hash)) && !okLegacyAdmin) return res.status(401).json({ error: 'Usuário ou senha inválidos' });
  const role = user?.role || 'admin'; const token = jwt.sign({ username, role }, JWT_SECRET, { expiresIn: '7d' }); res.json({ token, username, role });
});

app.get('/api/stores', authRequired, (req, res) => { const db = readDb(); res.json({ stores: db.stores || defaultStores() }); });
app.get('/api/ping', (_req, res) => res.json({ status: 'ok', time: nowIso() }));

app.get('/api/products', authRequired, (req, res) => {
  const db = readDb(); let items = Object.values(db.products || {}); const qRaw = String(req.query.q || '').trim();
  if (qRaw) { const q = qRaw.toUpperCase(); const pattern = q.includes('%') ? new RegExp(q.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&').replace(/%/g, '.*'), 'i') : null; items = items.filter(p => { const text = [p.code, p.product, p.material, p.source].join(' '); return pattern ? pattern.test(text) : text.toUpperCase().includes(q); }); }
  items.sort((a,b) => String(a.code).localeCompare(String(b.code))); res.json({ products: items });
});
app.put('/api/products/:code', authRequired, requireRole('admin', 'separador'), (req, res) => {
  const db = readDb(); const code = String(req.params.code || '').trim().toUpperCase(); const existing = db.products[code]; if (!existing) return res.status(404).json({ error: 'Produto não encontrado' });
  const applyGroup = Boolean(req.body.apply_group) || String(req.body.apply_group || '') === 'true'; const prefix = codePrefix(code);
  for (const [productCode, product] of Object.entries(db.products)) {
    if (productCode !== code && !(applyGroup && codePrefix(productCode) === prefix)) continue;
    upsertProduct(db, { code: productCode, product: productCode === code ? (req.body.product ?? product.product) : product.product, material: productCode === code ? (req.body.material ?? product.material) : product.material, stock: productCode === code && req.body.stock !== undefined ? num(req.body.stock) : product.stock, factor: req.body.factor !== undefined ? Math.max(1, num(req.body.factor) || 1) : product.factor, source: product.source });
  }
  writeDb(db); res.json({ ok: true, product: db.products[code], factor_prefix_applied: applyGroup ? prefix : null });
});

async function handleStockImport(req, res) {
  if (!req.files?.length) return res.status(400).json({ error: 'Arquivos não enviados' });
  const db = readDb(); const summary = []; const beforeBase = Object.keys(db.products || {}).length; let totalImported = 0;
  try {
    for (const file of req.files) {
      const parsed = await parseFile(file); const beforeFileBase = Object.keys(db.products || {}).length; for (const p of parsed.products) upsertProduct(db, p); const afterFileBase = Object.keys(db.products || {}).length; const mergedExisting = Math.max(0, parsed.products.length - (afterFileBase - beforeFileBase));
      db.stock_imports.unshift({ file: file.originalname, imported_type: parsed.imported_type, imported_at: nowIso(), count: parsed.products.length, added_to_base: afterFileBase - beforeFileBase, merged_existing: mergedExisting });
      totalImported += parsed.products.length; summary.push({ file: file.originalname, imported_type: parsed.imported_type, count: parsed.products.length, added_to_base: afterFileBase - beforeFileBase, merged_existing: mergedExisting });
    }
    writeDb(db); const afterBase = Object.keys(db.products || {}).length;
    res.json({ ok: true, total_files: req.files.length, total_imported: totalImported, total_added_to_base: afterBase - beforeBase, total_merged_existing: Math.max(0, totalImported - (afterBase - beforeBase)), total_in_base: afterBase, imports: summary });
  } catch (e) { res.status(400).json({ error: e.message || 'Falha ao importar arquivos' }); }
  finally { for (const file of req.files) if (file?.path) fs.unlink(file.path, () => {}); }
}
app.post('/api/stock/import-many', authRequired, requireRole('admin'), upload.array('files', 30), handleStockImport);
app.post('/api/stock/import', authRequired, requireRole('admin'), upload.array('files', 30), handleStockImport);

app.get('/api/orders/:storeId', authRequired, (req, res) => { const db = readDb(); const storeId = String(req.params.storeId || '01').padStart(2, '0'); ensureStoreContainers(db, storeId); res.json({ items: db.order_drafts_by_store[storeId] }); });
app.put('/api/orders/:storeId', authRequired, requireRole('admin', 'funcionario', 'separador'), (req, res) => { const db = readDb(); const storeId = String(req.params.storeId || '01').padStart(2, '0'); ensureStoreContainers(db, storeId); const items = (Array.isArray(req.body.items) ? req.body.items : []).map(sanitizeOrderItem).filter(x => x.code); db.order_drafts_by_store[storeId] = items; writeDb(db); res.json({ ok: true, items }); });
app.post('/api/orders/:storeId/finalize', authRequired, requireRole('admin', 'funcionario'), (req, res) => {
  const db = readDb(); const storeId = String(req.params.storeId || '01').padStart(2, '0'); ensureStoreContainers(db, storeId); const items = (Array.isArray(req.body.items) ? req.body.items : db.order_drafts_by_store[storeId]).map(sanitizeOrderItem).filter(x => x.code); if (!items.length) return res.status(400).json({ error: 'Pedido vazio' });
  const order_no = nextOrderNo(db, storeId); const order = { order_no, created_at: nowIso(), created_by: req.user.username, status: 'aberta', items };
  db.order_history_by_store[storeId].unshift(order); db.order_drafts_by_store[storeId] = []; writeDb(db); res.json({ ok: true, order_no, order: summarizeOrder(order) });
});

app.get('/api/orders-history/:storeId', authRequired, (req, res) => { const db = readDb(); const storeId = String(req.params.storeId || '01').padStart(2, '0'); ensureStoreContainers(db, storeId); res.json({ orders: db.order_history_by_store[storeId].map(summarizeOrder) }); });
app.get('/api/orders-history/:storeId/:orderNo', authRequired, (req, res) => { const db = readDb(); const storeId = String(req.params.storeId || '01').padStart(2, '0'); const order = findOrder(db, storeId, req.params.orderNo); if (!order) return res.status(404).json({ error: 'Ordem não encontrada' }); res.json({ order }); });
app.put('/api/orders-history/:storeId/:orderNo', authRequired, requireRole('admin', 'funcionario'), (req, res) => { const db = readDb(); const storeId = String(req.params.storeId || '01').padStart(2, '0'); const order = findOrder(db, storeId, req.params.orderNo); if (!order) return res.status(404).json({ error: 'Ordem não encontrada' }); order.items = (Array.isArray(req.body.items) ? req.body.items : []).map(sanitizeOrderItem).filter(x => x.code); order.updated_at = nowIso(); writeDb(db); res.json({ ok: true, order }); });
app.delete('/api/orders-history/:storeId/:orderNo', authRequired, requireRole('admin', 'funcionario'), (req, res) => { const db = readDb(); const storeId = String(req.params.storeId || '01').padStart(2, '0'); const before = db.order_history_by_store[storeId].length; db.order_history_by_store[storeId] = db.order_history_by_store[storeId].filter(o => o.order_no !== req.params.orderNo); db.shortage_history_by_store[storeId] = db.shortage_history_by_store[storeId].filter(o => o.order_no !== req.params.orderNo); writeDb(db); res.json({ ok: true, removed: before - db.order_history_by_store[storeId].length }); });
app.get('/api/orders-history/:storeId/:orderNo/export/:format', authRequired, (req, res) => { const db = readDb(); const storeId = String(req.params.storeId || '01').padStart(2, '0'); const order = findOrder(db, storeId, req.params.orderNo); if (!order) return res.status(404).json({ error: 'Ordem não encontrada' }); const rows = buildExportRowsForOrder(order); const title = `Ordem ${order.order_no}`; const subtitle = `Loja ${storeId} | ${new Date(order.created_at).toLocaleString('pt-BR')}`; const f = String(req.params.format || '').toLowerCase(); if (f === 'pdf') return sendPdfFromRows(res, `ordem-${order.order_no}.pdf`, title, subtitle, rows); if (f === 'excel') return sendExcel(res, `ordem-${order.order_no}.xlsx`, 'Ordem', rows); if (f === 'word') return sendWord(res, `ordem-${order.order_no}.doc`, title, subtitle, rows); if (f === 'print') return sendPrintHtml(res, title, subtitle, rows); return res.status(400).json({ error: 'Formato inválido' }); });

app.get('/api/shortages-history/:storeId', authRequired, (req, res) => { const db = readDb(); const storeId = String(req.params.storeId || '01').padStart(2, '0'); ensureStoreContainers(db, storeId); res.json({ shortages: db.shortage_history_by_store[storeId].map(summarizeShortage) }); });
app.get('/api/shortages-history/:storeId/:orderNo', authRequired, (req, res) => {
  const db = readDb(); const storeId = String(req.params.storeId || '01').padStart(2, '0'); const orderNo = req.params.orderNo; let record = findShortage(db, storeId, orderNo);
  if (record) return res.json({ order_no: orderNo, created_at: record.created_at, items: record.items });
  const order = findOrder(db, storeId, orderNo); if (!order) return res.status(404).json({ error: 'Ordem não encontrada' });
  return res.json({ order_no: orderNo, created_at: order.created_at, items: order.items.map(toShortageItem) });
});
app.put('/api/shortages-history/:storeId/:orderNo', authRequired, requireRole('admin', 'separador'), (req, res) => {
  const db = readDb(); const storeId = String(req.params.storeId || '01').padStart(2, '0'); const orderNo = req.params.orderNo; const order = findOrder(db, storeId, orderNo); if (!order) return res.status(404).json({ error: 'Ordem não encontrada' });
  const items = (Array.isArray(req.body.items) ? req.body.items : []).map(normalizeShortageItem).filter(x => x.code); const existing = findShortage(db, storeId, orderNo);
  if (existing) { existing.items = items; existing.updated_at = nowIso(); }
  else db.shortage_history_by_store[storeId].unshift({ order_no: orderNo, created_at: nowIso(), created_by: req.user.username, items });
  order.status = 'concluida'; order.updated_at = nowIso();
  for (const item of items) {
    const product = db.products[item.code]; if (!product) continue; product.stock = Math.max(0, num(product.stock) - item.sent_units); product.updated_at = nowIso();
  }
  writeDb(db); res.json({ ok: true, order_no: orderNo, items });
});
app.get('/api/shortages-history/:storeId/:orderNo/export/:format', authRequired, (req, res) => { const db = readDb(); const storeId = String(req.params.storeId || '01').padStart(2, '0'); const record = findShortage(db, storeId, req.params.orderNo); if (!record) return res.status(404).json({ error: 'Histórico de faltas não encontrado' }); const rows = buildExportRowsForShortage(record); const title = `Faltas ${record.order_no}`; const subtitle = `Loja ${storeId} | ${new Date(record.created_at).toLocaleString('pt-BR')}`; const f = String(req.params.format || '').toLowerCase(); if (f === 'pdf') return sendPdfFromRows(res, `faltas-${record.order_no}.pdf`, title, subtitle, rows); if (f === 'excel') return sendExcel(res, `faltas-${record.order_no}.xlsx`, 'Faltas', rows); if (f === 'word') return sendWord(res, `faltas-${record.order_no}.doc`, title, subtitle, rows); if (f === 'print') return sendPrintHtml(res, title, subtitle, rows); return res.status(400).json({ error: 'Formato inválido' }); });

app.get('*', (req, res) => { if (req.path.startsWith('/api/')) return res.status(404).json({ error: 'Rota da API não encontrada' }); res.sendFile(path.join(__dirname, 'index.html')); });
app.listen(PORT, () => console.log(`Servidor rodando na porta ${PORT}`));
