require('dotenv').config();
const express = require('express');
const cors = require('cors');
const path = require('path');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
const multer = require('multer');
const { parse } = require('csv-parse/sync');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || 'gov-system-secret-2024';

// ── DB Init ──────────────────────────────────────
const Database = require('better-sqlite3');
const DB_PATH = process.env.DB_PATH || path.join(__dirname, '..', 'data.db');
const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');

// ── Auto-seed from seed.sql if empty ─────────────
const isNewDb = db.prepare("SELECT count(*) as c FROM sqlite_master WHERE type='table' AND name='mat_archive'").get().c === 0;
if (isNewDb && fs.existsSync(path.join(__dirname, '..', 'seed.sql'))) {
  console.log('🌱 Found seed.sql, importing initial data...');
  const seed = fs.readFileSync(path.join(__dirname, '..', 'seed.sql'), 'utf8');
  db.exec(seed);
}

db.exec(`
  CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
  );
  CREATE TABLE IF NOT EXISTS mat_raw (
    id TEXT PRIMARY KEY,
    code TEXT,
    name TEXT,
    spec TEXT,
    new_code TEXT,
    new_name TEXT,
    status TEXT DEFAULT '未治理',
    source TEXT,
    created_at INTEGER
  );
  CREATE TABLE IF NOT EXISTS mat_archive (
    id TEXT PRIMARY KEY,
    code TEXT UNIQUE,
    name TEXT,
    synonyms TEXT,
    category TEXT,
    raw_count INTEGER DEFAULT 0,
    created_at INTEGER
  );
  CREATE TABLE IF NOT EXISTS categories (
    id TEXT PRIMARY KEY,
    code TEXT,
    l1 TEXT, l2 TEXT, l3 TEXT, l4 TEXT,
    key_attrs TEXT DEFAULT '',
    status TEXT DEFAULT '正常',
    created_at INTEGER
  );
  CREATE TABLE IF NOT EXISTS goods_raw (
    id TEXT PRIMARY KEY,
    code TEXT,
    name TEXT,
    std_code TEXT,
    std_name TEXT,
    status TEXT DEFAULT '未归一',
    source TEXT,
    created_at INTEGER
  );
  CREATE TABLE IF NOT EXISTS goods_std (
    id TEXT PRIMARY KEY,
    code TEXT UNIQUE,
    name TEXT,
    category TEXT,
    attrs TEXT,
    raw_count INTEGER DEFAULT 0,
    mat_code TEXT,
    created_at INTEGER
  );
  CREATE TABLE IF NOT EXISTS mapping (
    id TEXT PRIMARY KEY,
    goods_code TEXT,
    goods_name TEXT,
    mat_code TEXT,
    mat_name TEXT,
    type TEXT,
    method TEXT,
    created_at INTEGER
  );
  CREATE TABLE IF NOT EXISTS mat_gov_tasks (
    id TEXT PRIMARY KEY,
    filename TEXT,
    total_rows INTEGER,
    success_count INTEGER,
    fail_count INTEGER,
    new_arch_count INTEGER,
    results_json TEXT,
    created_at INTEGER
  );
  CREATE TABLE IF NOT EXISTS brands (
    id TEXT PRIMARY KEY,
    name TEXT UNIQUE,
    created_at INTEGER
  );
`);

// Migrations
try { db.exec("ALTER TABLE categories ADD COLUMN key_attrs TEXT DEFAULT ''"); } catch(e) {}
const newCols = [
  'orig_code', 'orig_name', 'spec', 'brand', 'model', 'core_word',
  'category_code', 'category_name', 'missing_attrs', 'std_name_proposed', 'match_status'
];
for (const col of newCols) {
  try { db.exec(`ALTER TABLE goods_raw ADD COLUMN ${col} TEXT`); } catch(e) {}
}

// Init default settings
const initSettings = () => {
  const rows = [
    ['username', 'admin'],
    ['password', bcrypt.hashSync('12345a', 10)],
    ['api_type', 'deepseek'],
    ['api_key', ''],
    ['api_base', ''],
    ['api_model', ''],
    ['mat_code_prefix', 'MAT'],
    ['mat_code_digits', '6'],
    ['goods_code_prefix', 'SP'],
    ['goods_code_digits', '6'],
    ['mat_extract_prompt', '保留品类级核心词，去掉功能级核心词，去掉规格/型号/品牌/材质/工艺等修饰属性'],
    ['mat_similarity_threshold', '0.3'],
    ['cat_attr_prompt', '你是品类数据治理专家。根据给定的品类层级路径（一级>二级>三级>四级），提取该类目下“SKU关键属性”（即必要属性），最多不超过5个，只返回属性名称，用逗号分隔，不要任何其他内容。\n【SKU属性定义】\nSKU关键属性必须满足以下至少2条：\n1. 会直接影响商品价格\n2. 会影响库存管理（不同属性值需要独立库存）\n3. 用户在购买时必须选择\n4. 不同属性值不能作为同一个商品售卖\n\n【排除规则（非常重要）】\n以下属性不能作为SKU属性：\n- 外观类：颜色（非功能性）、图案、外形设计\n- 描述类：适用人群、使用场景\n- 营销类：卖点、风格、品牌溢价\n- 弱影响属性：不影响价格或库存的属性\n【要求】\n- 不要遗漏核心属性\n- 不要输出与该类目无关的属性\n- 输出要专业、简洁、结构化'],
  ];
  const stmt = db.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)');
  rows.forEach(r => stmt.run(r[0], r[1]));
};
initSettings();

// ── Helpers ──────────────────────────────────────
const genId = () => Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
const getSetting = (key) => { const r = db.prepare('SELECT value FROM settings WHERE key=?').get(key); return r ? r.value : null; };
const setSetting = (key, val) => db.prepare('INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)').run(key, String(val));

const genMatCode = () => {
  const prefix = getSetting('mat_code_prefix') || 'ML';
  const digits = parseInt(getSetting('mat_code_digits')) || 6;
  const count = db.prepare('SELECT COUNT(*) as c FROM mat_archive').get().c;
  return prefix + String(count + 1).padStart(digits, '0');
};
const genGoodsCode = () => {
  const prefix = getSetting('goods_code_prefix') || 'SP';
  const digits = parseInt(getSetting('goods_code_digits')) || 6;
  const count = db.prepare('SELECT COUNT(*) as c FROM goods_std').get().c;
  return prefix + String(count + 1).padStart(digits, '0');
};

// ── Middleware ────────────────────────────────────
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.static(path.join(__dirname, '..', 'public')));

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 20 * 1024 * 1024 } });

const auth = (req, res, next) => {
  const token = req.headers.authorization?.replace('Bearer ', '');
  if (!token) return res.status(401).json({ error: '未登录' });
  try {
    req.user = jwt.verify(token, JWT_SECRET);
    next();
  } catch { res.status(401).json({ error: 'token无效' }); }
};

// ── AI Proxy ──────────────────────────────────────
app.post('/api/ai/chat', auth, async (req, res) => {
  const { systemPrompt, userPrompt } = req.body;
  const apiKey = getSetting('api_key');
  const apiType = getSetting('api_type') || 'deepseek';
  const apiBase = getSetting('api_base');
  const apiModel = getSetting('api_model');

  if (!apiKey) return res.status(400).json({ error: '请先在系统设置中配置AI API密钥' });

  const endpoints = {
    openai: 'https://api.openai.com/v1/chat/completions',
    deepseek: 'https://api.deepseek.com/v1/chat/completions',
    qianwen: 'https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions',
    claude: 'https://api.anthropic.com/v1/messages',
  };
  const models = { openai: 'gpt-4o-mini', deepseek: 'deepseek-chat', qianwen: 'qwen-plus', claude: 'claude-haiku-4-5-20251001' };

  const url = apiBase || endpoints[apiType] || endpoints.deepseek;
  const model = apiModel || models[apiType] || 'deepseek-chat';

  try {
    const https = require('https');
    const http = require('http');
    const urlObj = new URL(url);
    const bodyStr = JSON.stringify({ model, messages: [{ role: 'system', content: systemPrompt }, { role: 'user', content: userPrompt }], max_tokens: 4000, temperature: 0.3 });
    console.log(`[AI] Requesting ${apiType} model: ${model} (Prompt length: ${userPrompt.length})`);
    
    const options = {
      hostname: urlObj.hostname, port: urlObj.port || (urlObj.protocol === 'https:' ? 443 : 80),
      path: urlObj.pathname + urlObj.search, method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${apiKey}`, 'Content-Length': Buffer.byteLength(bodyStr) },
      timeout: 60000 // 60s timeout
    };
    const proto = urlObj.protocol === 'https:' ? https : http;
    const apiReq = proto.request(options, (apiRes) => {
      let data = '';
      apiRes.on('data', chunk => data += chunk);
      apiRes.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          if (parsed.error) {
            console.error(`[AI] API Error: ${JSON.stringify(parsed.error)}`);
            return res.status(400).json({ error: parsed.error.message || JSON.stringify(parsed.error) });
          }
          const content = parsed.choices?.[0]?.message?.content || parsed.content?.[0]?.text || '';
          console.log(`[AI] Success (Response length: ${content.length})`);
          res.json({ content });
        } catch (e) { 
          console.error(`[AI] Parse Error: ${e.message}`);
          res.status(500).json({ error: 'AI响应解析失败: ' + data.slice(0, 200) }); 
        }
      });
    });
    apiReq.on('timeout', () => {
      console.error('[AI] Request Timeout (60s)');
      apiReq.destroy();
      res.status(504).json({ error: 'AI请求超时，请重试' });
    });
    apiReq.on('error', e => {
      console.error(`[AI] Network Error: ${e.message}`);
      res.status(500).json({ error: 'AI请求失败: ' + e.message });
    });
    apiReq.write(bodyStr);
    apiReq.end();
  } catch (e) { 
    console.error(`[AI] Unexpected Error: ${e.message}`);
    res.status(500).json({ error: e.message }); 
  }
});

// ── Auth Routes ───────────────────────────────────
app.post('/api/login', (req, res) => {
  const { username, password } = req.body;
  const storedUser = getSetting('username');
  const storedHash = getSetting('password');
  if (username !== storedUser || !bcrypt.compareSync(password, storedHash)) {
    return res.status(401).json({ error: '用户名或密码错误' });
  }
  const token = jwt.sign({ username }, JWT_SECRET, { expiresIn: '7d' });
  res.json({ token, username });
});

// ── Settings Routes ───────────────────────────────
// --- Brands API ---
app.get('/api/brands', auth, (req, res) => {
  const { q = '', page = 1, limit = 20 } = req.query;
  const offset = (page - 1) * limit;
  try {
    const list = db.prepare(`SELECT * FROM brands WHERE name LIKE ? ORDER BY created_at DESC LIMIT ? OFFSET ?`)
      .all(`%${q}%`, limit, offset);
    const total = db.prepare(`SELECT count(*) as c FROM brands WHERE name LIKE ?`)
      .get(`%${q}%`).c;
    res.json({ list, total });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/brands', auth, (req, res) => {
  const { name } = req.body;
  if (!name) return res.status(400).json({ error: '品牌名称为必填' });
  try {
    const stmt = db.prepare('INSERT INTO brands (id, name, created_at) VALUES (?, ?, ?)');
    stmt.run(genId(), name, Date.now());
    res.json({ success: true });
  } catch (err) {
    if (err.message.includes('UNIQUE constraint failed')) {
      res.status(400).json({ error: '品牌已存在' });
    } else {
      res.status(500).json({ error: err.message });
    }
  }
});

app.put('/api/brands/:id', auth, (req, res) => {
  const { name } = req.body;
  if (!name) return res.status(400).json({ error: '品牌名称为必填' });
  try {
    const stmt = db.prepare('UPDATE brands SET name=? WHERE id=?');
    stmt.run(name, req.params.id);
    res.json({ success: true });
  } catch (err) {
    if (err.message.includes('UNIQUE constraint failed')) {
      res.status(400).json({ error: '品牌名称已被使用' });
    } else {
      res.status(500).json({ error: err.message });
    }
  }
});

app.delete('/api/brands/:id', auth, (req, res) => {
  try {
    db.prepare('DELETE FROM brands WHERE id=?').run(req.params.id);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/settings', auth, (req, res) => {
  const keys = ['api_type', 'api_key', 'api_base', 'api_model', 'mat_code_prefix', 'mat_code_digits', 'goods_code_prefix', 'goods_code_digits', 'mat_extract_prompt', 'mat_similarity_threshold', 'cat_attr_prompt', 'username'];
  const result = {};
  keys.forEach(k => { result[k] = getSetting(k) || ''; });
  if (result.api_key) result.api_key = result.api_key.slice(0, 4) + '****' + result.api_key.slice(-4);
  res.json(result);
});

app.post('/api/settings', auth, (req, res) => {
  const { api_type, api_key, api_base, api_model, mat_code_prefix, mat_code_digits, goods_code_prefix, goods_code_digits, mat_extract_prompt, mat_similarity_threshold, cat_attr_prompt } = req.body;
  if (api_type) setSetting('api_type', api_type);
  if (api_key && !api_key.includes('****')) setSetting('api_key', api_key);
  if (api_base !== undefined) setSetting('api_base', api_base);
  if (api_model !== undefined) setSetting('api_model', api_model);
  if (mat_code_prefix) setSetting('mat_code_prefix', mat_code_prefix);
  if (mat_code_digits) setSetting('mat_code_digits', mat_code_digits);
  if (goods_code_prefix) setSetting('goods_code_prefix', goods_code_prefix);
  if (goods_code_digits) setSetting('goods_code_digits', goods_code_digits);
  if (mat_extract_prompt) setSetting('mat_extract_prompt', mat_extract_prompt);
  if (mat_similarity_threshold !== undefined) setSetting('mat_similarity_threshold', mat_similarity_threshold);
  if (cat_attr_prompt !== undefined) setSetting('cat_attr_prompt', cat_attr_prompt);
  res.json({ ok: true });
});

app.post('/api/settings/password', auth, (req, res) => {
  const { currentPassword, newPassword, newUsername } = req.body;
  const storedHash = getSetting('password');
  if (!bcrypt.compareSync(currentPassword, storedHash)) return res.status(400).json({ error: '当前密码不正确' });
  if (newPassword) setSetting('password', bcrypt.hashSync(newPassword, 10));
  if (newUsername) setSetting('username', newUsername);
  res.json({ ok: true });
});

// ── Generic CRUD factory ──────────────────────────
function crudRoutes(app, prefix, table, fields, searchFields, options = {}) {
  // List
  app.get(`/api/${prefix}`, auth, (req, res) => {
    const { q, filter, filterField, page, limit } = req.query;
    let sql = `FROM ${table}`;
    const params = [];
    const conds = [];
    if (q && searchFields.length) {
      conds.push('(' + searchFields.map(f => `${f} LIKE ?`).join(' OR ') + ')');
      searchFields.forEach(() => params.push(`%${q}%`));
    }
    if (filter && filterField && filter !== '全部') { conds.push(`${filterField} = ?`); params.push(filter); }
    
    const where = conds.length ? ' WHERE ' + conds.join(' AND ') : '';
    const total = db.prepare(`SELECT COUNT(*) as c ${sql} ${where}`).get(...params).c;
    
    let finalSql = `SELECT * ${sql} ${where} ORDER BY ${options.orderBy || 'created_at DESC'}`;
    if (page && limit) {
      finalSql += ` LIMIT ? OFFSET ?`;
      params.push(parseInt(limit), (parseInt(page) - 1) * parseInt(limit));
    }
    let rows = db.prepare(finalSql).all(...params);
    if (options.postProcessList) rows = options.postProcessList(rows);
    res.json(page ? { data: rows, total } : rows);
  });

  // Create
  app.post(`/api/${prefix}`, auth, (req, res) => {
    const id = genId();
    const data = { id, ...req.body, created_at: Date.now() };
    const cols = ['id', ...fields, 'created_at'].filter(f => data[f] !== undefined);
    const sql = `INSERT INTO ${table} (${cols.join(',')}) VALUES (${cols.map(() => '?').join(',')})`;
    try {
      db.prepare(sql).run(cols.map(c => data[c] ?? null));
      res.json({ id, ...data });
    } catch (e) { res.status(400).json({ error: e.message }); }
  });

  // Update
  app.put(`/api/${prefix}/:id`, auth, (req, res) => {
    const updates = fields.filter(f => req.body[f] !== undefined).map(f => `${f}=?`);
    if (!updates.length) return res.json({ ok: true });
    const vals = fields.filter(f => req.body[f] !== undefined).map(f => req.body[f]);
    db.prepare(`UPDATE ${table} SET ${updates.join(',')} WHERE id=?`).run(...vals, req.params.id);
    res.json({ ok: true });
  });

  // Delete
  app.delete(`/api/${prefix}/:id`, auth, (req, res) => {
    const id = req.params.id;
    if (table === 'mat_archive') {
      const arch = db.prepare('SELECT code FROM mat_archive WHERE id=?').get(id);
      if (arch) db.prepare("UPDATE mat_raw SET new_code=NULL, new_name=NULL, status='未治理' WHERE new_code=?").run(arch.code);
    }
    if (table === 'mat_raw') {
      const raw = db.prepare('SELECT new_code FROM mat_raw WHERE id=?').get(id);
      if (raw && raw.new_code) {
        db.prepare('UPDATE mat_archive SET raw_count = raw_count - 1 WHERE code=?').run(raw.new_code);
      }
    }
    db.prepare(`DELETE FROM ${table} WHERE id=?`).run(id);
    res.json({ ok: true });
  });

  // Bulk Delete
  app.post(`/api/${prefix}/bulk-delete`, auth, (req, res) => {
    const { ids } = req.body;
    if (!ids || !ids.length) return res.json({ ok: true });
    const deleteBatch = db.transaction(() => {
      ids.forEach(id => {
        if (table === 'mat_archive') {
          const arch = db.prepare('SELECT code FROM mat_archive WHERE id=?').get(id);
          if (arch) db.prepare("UPDATE mat_raw SET new_code=NULL, new_name=NULL, status='未治理' WHERE new_code=?").run(arch.code);
        }
        if (table === 'mat_raw') {
          const raw = db.prepare('SELECT new_code FROM mat_raw WHERE id=?').get(id);
          if (raw && raw.new_code) {
            db.prepare('UPDATE mat_archive SET raw_count = raw_count - 1 WHERE code=?').run(raw.new_code);
          }
        }
        db.prepare(`DELETE FROM ${table} WHERE id=?`).run(id);
      });
    });
    deleteBatch();
    res.json({ ok: true });
  });

  // Clear All
  app.post(`/api/${prefix}/clear-all`, auth, (req, res) => {
    if (table === 'mat_archive') db.prepare("UPDATE mat_raw SET new_code=NULL, new_name=NULL, status='未治理'").run();
    if (table === 'mat_raw') db.prepare("UPDATE mat_archive SET raw_count = 0").run();
    db.prepare(`DELETE FROM ${table}`).run();
    res.json({ ok: true });
  });
}

crudRoutes(app, 'mat-raw', 'mat_raw', ['code','name','spec','new_code','new_name','status','source'], ['code','name']);
crudRoutes(app, 'mat-archive', 'mat_archive', ['code','name','synonyms','category','raw_count'], ['code','name','synonyms'], {
  postProcessList: (rows) => {
    const threshold = parseFloat(getSetting('mat_similarity_threshold')) || 0.8;
    const all = db.prepare('SELECT id, code, name FROM mat_archive').all();
    return rows.map(row => {
      let similar = [];
      all.forEach(a => {
        if (a.id !== row.id && getDice(row.name, a.name) >= threshold) {
          similar.push(`${a.code} | ${a.name}`);
        }
      });
      return { ...row, similar_count: similar.length, similar_text: similar.join('\n') };
    });
  }
});
crudRoutes(app, 'categories', 'categories', ['code','l1','l2','l3','l4','key_attrs','status'], ['code','l1','l2','l3','l4','key_attrs'], { orderBy: 'code ASC' });

// ── Categories: batch update attrs ───────────────
app.post('/api/categories/batch-update-attrs', auth, (req, res) => {
  const { updates } = req.body; // [{id, key_attrs}]
  if (!updates || !updates.length) return res.json({ ok: true });
  const stmt = db.prepare('UPDATE categories SET key_attrs=? WHERE id=?');
  const batch = db.transaction(() => {
    updates.forEach(u => stmt.run(u.key_attrs || '', u.id));
  });
  try { batch(); res.json({ ok: true }); } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Categories: export ───────────────────────────
app.get('/api/categories/export', auth, (req, res) => {
  const rows = db.prepare('SELECT * FROM categories ORDER BY code ASC').all();
  res.json(rows);
});
crudRoutes(app, 'goods-raw', 'goods_raw', ['code','name','std_code','std_name','status','source','orig_code','orig_name','spec','brand','model','core_word','category_code','category_name','missing_attrs','std_name_proposed','match_status'], ['code','name','orig_code','orig_name','brand','model','core_word']);

// ── Goods Unify Pipeline Steps ────────────────────

// Step 2: Import & Merge
app.post('/api/goods-unify/import', auth, (req, res) => {
  const { rows } = req.body;
  if (!rows || !rows.length) return res.status(400).json({ error: '数据为空' });

  const results = { total: rows.length, success: 0, duplicate: 0, skipped: [] };
  const insertStmt = db.prepare(`
    INSERT INTO goods_raw (id, code, name, orig_code, orig_name, spec, brand, status, source, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const tx = db.transaction(() => {
    rows.forEach(r => {
      const origCode = r['MRO商品编号'] || r['编码'] || '';
      const origName = r['MRO商品名称'] || r['名称'] || '';
      const spec = r['规格'] || r['规格型号'] || '';
      const brand = r['品牌'] || '';
      
      if (!origCode) { results.skipped.push(origName); return; }

      const existing = db.prepare('SELECT id FROM goods_raw WHERE orig_code = ?').get(origCode);
      if (existing) {
        results.duplicate++;
        return;
      }

      // 合并名称、规格、品牌 
      const mergedName = [origName, spec, brand].filter(Boolean).join(' ');
      const id = genId();
      insertStmt.run(id, origCode, mergedName, origCode, origName, spec, brand, '未归一', 'Excel导入', Date.now());
      results.success++;
    });
  });

  try {
    tx();
    res.json(results);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});
crudRoutes(app, 'goods-std', 'goods_std', ['code','name','category','attrs','raw_count','mat_code'], ['code','name']);
crudRoutes(app, 'mapping', 'mapping', ['goods_code','goods_name','mat_code','mat_name','type','method'], ['goods_code','goods_name','mat_code','mat_name']);

// ── Special: Code generators ──────────────────────
app.get('/api/gen-mat-code', auth, (req, res) => res.json({ code: genMatCode() }));
app.get('/api/gen-goods-code', auth, (req, res) => res.json({ code: genGoodsCode() }));

// ── Special: Bulk save gov results ───────────────
app.post('/api/mat-gov/save-results', auth, (req, res) => {
  const { archive, rawMats } = req.body;
  const insertArch = db.prepare('INSERT OR REPLACE INTO mat_archive (id,code,name,synonyms,category,raw_count,created_at) VALUES (?,?,?,?,?,?,?)');
  const insertRaw = db.prepare('INSERT OR REPLACE INTO mat_raw (id,code,name,spec,new_code,new_name,status,source,created_at) VALUES (?,?,?,?,?,?,?,?,?)');
  const updateArch = db.prepare('UPDATE mat_archive SET synonyms=?, raw_count=? WHERE id=?');

  const saveAll = db.transaction(() => {
    archive.forEach(a => {
      const existing = db.prepare('SELECT id FROM mat_archive WHERE name=?').get(a.name);
      if (existing) {
        updateArch.run(a.synonyms || '', a.raw_count || 0, existing.id);
      } else {
        insertArch.run(a.id || genId(), a.code, a.name, a.synonyms || '', a.category || '', a.raw_count || 0, a.created_at || Date.now());
      }
    });
    rawMats.forEach(r => insertRaw.run(r.id || genId(), r.code, r.name, r.spec || '', r.new_code || '', r.new_name || '', r.status || '已治理', r.source || 'Excel导入', r.created_at || Date.now()));
  });

  const saveTask = () => {
    const { filename, total, success, fail, newArch, results } = req.body;
    db.prepare('INSERT INTO mat_gov_tasks (id,filename,total_rows,success_count,fail_count,new_arch_count,results_json,created_at) VALUES (?,?,?,?,?,?,?,?)').run(genId(), filename || '未知文件', total || 0, success || 0, fail || 0, newArch || 0, JSON.stringify(results || {}), Date.now());
  };

  try {
    saveAll();
    if (req.body.total !== undefined) saveTask();
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/mat-gov/tasks', auth, (req, res) => {
  res.json(db.prepare('SELECT id, filename, total_rows, success_count, fail_count, new_arch_count, created_at FROM mat_gov_tasks ORDER BY created_at DESC').all());
});

app.get('/api/mat-gov/tasks/:id', auth, (req, res) => {
  const task = db.prepare('SELECT * FROM mat_gov_tasks WHERE id=?').get(req.params.id);
  if (!task) return res.status(404).json({ error: '任务不存在' });
  task.results = JSON.parse(task.results_json);
  res.json(task);
});

app.delete('/api/mat-gov/tasks/:id', auth, (req, res) => {
  db.prepare('DELETE FROM mat_gov_tasks WHERE id=?').run(req.params.id);
  res.json({ ok: true });
});

app.post('/api/mat-gov/tasks/clear-all', auth, (req, res) => {
  db.prepare('DELETE FROM mat_gov_tasks').run();
  res.json({ ok: true });
});

// Similarity & Merge
const getDice = (s1, s2) => {
  if (!s1 || !s2) return 0;
  const getBigrams = s => {
    const b = new Set();
    for (let i = 0; i < s.length - 1; i++) b.add(s.substring(i, i + 2));
    return b;
  };
  const b1 = getBigrams(s1 + ''), b2 = getBigrams(s2 + '');
  let intersect = 0;
  for (const x of b1) if (b2.has(x)) intersect++;
  return (2.0 * intersect) / (b1.size + b2.size) || 0;
};

app.get('/api/mat-archive/:id/similar', auth, (req, res) => {
  const threshold = parseFloat(getSetting('mat_similarity_threshold')) || 0.8;
  const target = db.prepare('SELECT id, name FROM mat_archive WHERE id=?').get(req.params.id);
  if (!target) return res.status(404).json({ error: '档案不存在' });
  const all = db.prepare('SELECT id, code, name FROM mat_archive WHERE id != ?').all(req.params.id);
  const similar = all.map(a => ({ ...a, score: getDice(target.name, a.name) })).filter(a => a.score >= threshold).sort((a, b) => b.score - a.score);
  res.json(similar);
});

app.post('/api/mat-archive/merge', auth, (req, res) => {
  const { targetId, sourceIds } = req.body;
  if (!targetId || !sourceIds || !sourceIds.length) return res.status(400).json({ error: '缺少参数' });
  const target = db.prepare('SELECT * FROM mat_archive WHERE id=?').get(targetId);
  const sources = db.prepare(`SELECT * FROM mat_archive WHERE id IN (${sourceIds.map(() => '?').join(',')})`).all(...sourceIds);
  if (!target) return res.status(404).json({ error: '目标档案不存在' });

  const mergeBatch = db.transaction(() => {
    const synonyms = new Set((target.synonyms || '').split(',').filter(Boolean));
    let extraCount = 0;
    sources.forEach(s => {
      synonyms.add(s.name);
      (s.synonyms || '').split(',').forEach(sn => sn && synonyms.add(sn));
      extraCount += s.raw_count;
      db.prepare("UPDATE mat_raw SET new_code=?, new_name=? WHERE new_code=?").run(target.code, target.name, s.code);
      db.prepare('DELETE FROM mat_archive WHERE id=?').run(s.id);
    });
    db.prepare('UPDATE mat_archive SET synonyms=?, raw_count = raw_count + ? WHERE id=?').run([...synonyms].join(','), extraCount, targetId);
  });

  try {
    mergeBatch();
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Step 3: Match Category
app.post('/api/goods-unify/match-category', auth, async (req, res) => {
  const { ids } = req.body;
  if (!ids || !ids.length) return res.status(400).json({ error: '请选择商品' });

  const cats = db.prepare("SELECT code, l1, l2, l3, l4, key_attrs FROM categories WHERE l4 IS NOT NULL").all();
  const rawGoods = db.prepare(`SELECT id, name FROM goods_raw WHERE id IN (${ids.map(() => '?').join(',')})`).all(...ids);
  
  const results = [];
  for (const item of rawGoods) {
    // Recall Top 5 by Dice
    const top5 = cats.map(c => ({ ...c, score: getDice(item.name, c.l4) }))
      .sort((a, b) => b.score - a.score).slice(0, 5);
    
    // LLM Decide
    const prompt = `请从以下5个分类中选择一个最适合商品“${item.name}”的末级分类（L4）。只需直接返回分类编码和分类名称，用“|”分隔，如“100101|笔记本”。如果都不匹配，请返回“未匹配”。\n候选项：\n${top5.map(c => `${c.code}|${c.l1}>${c.l2}>${c.l3}>${c.l4}`).join('\n')}`;
    
    try {
      const resp = await POST('/api/ai/chat', { systemPrompt: '你是一个专业的商品分类专家。', userPrompt: prompt }, { internal: true });
      const content = resp.content.trim();
      if (content === '未匹配' || !content.includes('|')) {
        db.prepare('UPDATE goods_raw SET category_code=?, category_name=?, match_status=? WHERE id=?')
          .run(null, null, '分类未匹配', item.id);
        results.push({ id: item.id, status: 'fail' });
      } else {
        const [code, name] = content.split('|');
        db.prepare('UPDATE goods_raw SET category_code=?, category_name=?, match_status=? WHERE id=?')
          .run(code, name, '分类已匹配', item.id);
        results.push({ id: item.id, status: 'success', category: name });
      }
    } catch (e) {
      results.push({ id: item.id, status: 'error', error: e.message });
    }
  }
  res.json(results);
});

// Step 4: Extract Features
app.post('/api/goods-unify/extract-features', auth, async (req, res) => {
  const { ids } = req.body;
  const list = db.prepare(`SELECT id, name, category_code, category_name FROM goods_raw WHERE id IN (${ids.map(() => '?').join(',')})`).all(...ids);
  
  const results = [];
  for (const item of list) {
    if (!item.category_code) { results.push({ id: item.id, status: 'skip', reason: '无分类' }); continue; }
    const cat = db.prepare('SELECT key_attrs, l4 FROM categories WHERE code = ?').get(item.category_code);
    const keyAttrs = cat?.key_attrs || '';
    
    const systemPrompt = `你是一个商品属性提取专家。给定商品名称和所属分类及其关键属性，请提取该商品的：品牌、核心词、型号、以及分类的关键属性。
返回JSON格式：{"brand":"品牌","core_word":"核心词","model":"型号","attrs":{"属性名":"值"}}。
如果某个属性在名称中未找到，请对应的值设为"unknown"。品牌请优先从名称中提取。`;
    const userPrompt = `分类：${item.category_name}\n关键属性：${keyAttrs}\n商品名称：${item.name}`;

    try {
      const resp = await POST('/api/ai/chat', { systemPrompt, userPrompt }, { internal: true });
      const data = JSON.parse(resp.content.replace(/```json?|```/g, '').trim());
      
      // Sink brand
      if (data.brand && data.brand !== 'unknown') {
        db.prepare('INSERT OR IGNORE INTO brands (id, name, created_at) VALUES (?, ?, ?)').run(genId(), data.brand, Date.now());
      }
      
      // Propose Std Name: 品牌+核心词+型号+属性值
      const attrVals = Object.values(data.attrs).filter(v => v && v !== 'unknown');
      const stdName = [data.brand, data.core_word, data.model, ...attrVals].filter(v => v && v !== 'unknown').join(' ');
      
      const missing = [];
      if (data.brand === 'unknown') missing.push('品牌');
      if (data.core_word === 'unknown') missing.push('核心词');
      Object.entries(data.attrs).forEach(([k, v]) => { if (v === 'unknown') missing.push(k); });
      
      db.prepare(`UPDATE goods_raw SET brand=?, core_word=?, model=?, spec=?, missing_attrs=?, std_name_proposed=?, match_status=? WHERE id=?`)
        .run(data.brand, data.core_word, data.model, JSON.stringify(data.attrs), missing.join(','), stdName, missing.length ? '信息不全' : '属性已提取', item.id);
      
      results.push({ id: item.id, status: 'success', stdName, missing });
    } catch (e) {
      results.push({ id: item.id, status: 'error', error: e.message });
    }
  }
  res.json(results);
});

// Step 5: Match Standard
app.post('/api/goods-unify/match-standard', auth, async (req, res) => {
  const { ids } = req.body;
  const list = db.prepare(`SELECT * FROM goods_raw WHERE id IN (${ids.map(() => '?').join(',')})`).all(...ids);
  
  const results = [];
  for (const item of list) {
    if (!item.std_name_proposed) continue;
    
    const stds = db.prepare("SELECT code, name FROM goods_std").all();
    const top5 = stds.map(s => ({ ...s, score: getDice(item.std_name_proposed, s.name) }))
      .sort((a, b) => b.score - a.score).slice(0, 5);
    
    const prompt = `请判定商品“${item.std_name_proposed}”是否与以下标准商品中的某一个属于同一款。只需直接返回匹配到的标准商品编码，如“SP000101”。如果不匹配，请返回“不匹配”。\n候选项：\n${top5.map(s => `${s.code}|${s.name}`).join('\n')}`;
    
    try {
      const resp = await POST('/api/ai/chat', { systemPrompt: '你是一个商品比对专家。', userPrompt: prompt }, { internal: true });
      const content = resp.content.trim();
      
      let stdCode = '';
      let stdName = '';
      
      if (content !== '不匹配' && content.startsWith('SP')) {
        stdCode = content;
        const s = db.prepare('SELECT name FROM goods_std WHERE code=?').get(stdCode);
        stdName = s ? s.name : item.std_name_proposed;
        db.prepare('UPDATE goods_std SET raw_count = raw_count + 1 WHERE code=?').run(stdCode);
      } else {
        // Create new standard product
        const prefix = item.category_code || 'G';
        const count = db.prepare('SELECT count(*) as c FROM goods_std WHERE code LIKE ?').get(`${prefix}%`).c;
        stdCode = prefix + String(count + 1).padStart(4, '0');
        stdName = item.std_name_proposed;
        db.prepare('INSERT INTO goods_std (id, code, name, category, attrs, raw_count, created_at) VALUES (?,?,?,?,?,?,?)')
          .run(genId(), stdCode, stdName, item.category_name, item.spec, 1, Date.now());
      }
      
      db.prepare('UPDATE goods_raw SET std_code=?, std_name=?, status=?, match_status=? WHERE id=?')
        .run(stdCode, stdName, '已归一', '已完成', item.id);
      
      results.push({ id: item.id, status: 'success', stdCode });
    } catch (e) {
      results.push({ id: item.id, status: 'error', error: e.message });
    }
  }
  res.json(results);
});

// Step 6: Match Material
app.post('/api/goods-unify/match-material', auth, async (req, res) => {
  const { ids } = req.body; // std goods ids
  const list = db.prepare(`SELECT id, code, name FROM goods_std WHERE id IN (${ids.map(() => '?').join(',')})`).all(...ids);
  const mats = db.prepare('SELECT code, name FROM mat_archive').all();
  
  const results = [];
  for (const item of list) {
    const top5 = mats.map(m => ({ ...m, score: getDice(item.name, m.name) }))
      .sort((a,b) => b.score - a.score).slice(0, 5);
    
    const prompt = `为商品“${item.name}”匹配最合适的底层物料档案。只需返回物料编码及名称，用“|”分隔，如“MAT000001|笔记本”。如果不匹配请回“不匹配”。\n候选项：\n${top5.map(m => `${m.code}|${m.name}`).join('\n')}`;
    
    try {
      const resp = await POST('/api/ai/chat', { systemPrompt: '物料匹配专家', userPrompt: prompt }, { internal: true });
      const content = resp.content.trim();
      if (content !== '不匹配' && content.includes('|')) {
        const [mCode, mName] = content.split('|');
        db.prepare('UPDATE goods_std SET mat_code=? WHERE id=?').run(mCode, item.id);
        
        // Save to mapping table
        db.prepare('INSERT OR REPLACE INTO mapping (id, goods_code, goods_name, mat_code, mat_name, method, created_at) VALUES (?,?,?,?,?,?,?)')
          .run(genId(), item.code, item.name, mCode, mName, 'AI自动匹配', Date.now());
        
        results.push({ id: item.id, status: 'success', matCode: mCode });
      } else {
        results.push({ id: item.id, status: 'none' });
      }
    } catch (e) {
      results.push({ id: item.id, status: 'error', error: e.message });
    }
  }
  res.json(results);
});

// ── File Upload & Parse ───────────────────────────
app.post('/api/upload/csv', auth, upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: '未收到文件' });
  try {
    const text = req.file.buffer.toString('utf-8');
    const rows = parse(text, { columns: true, skip_empty_lines: true, trim: true, bom: true });
    if (rows.length > 10000) return res.status(400).json({ error: '文件行数超过10000行限制' });
    res.json({ rows, count: rows.length, columns: rows.length > 0 ? Object.keys(rows[0]) : [] });
  } catch (e) { res.status(400).json({ error: 'CSV解析失败: ' + e.message }); }
});

// ── Stats ─────────────────────────────────────────
app.get('/api/stats', auth, (req, res) => {
  res.json({
    matArchive: db.prepare('SELECT COUNT(*) as c FROM mat_archive').get().c,
    matRaw: db.prepare('SELECT COUNT(*) as c FROM mat_raw').get().c,
    matUntreated: db.prepare("SELECT COUNT(*) as c FROM mat_raw WHERE status='未治理'").get().c,
    goodsStd: db.prepare('SELECT COUNT(*) as c FROM goods_std').get().c,
    goodsRaw: db.prepare('SELECT COUNT(*) as c FROM goods_raw').get().c,
    goodsUnified: db.prepare("SELECT COUNT(*) as c FROM goods_raw WHERE status='已归一'").get().c,
    categories: db.prepare('SELECT COUNT(*) as c FROM categories').get().c,
    mapping: db.prepare('SELECT COUNT(*) as c FROM mapping').get().c,
  });
});

// ── Serve frontend ────────────────────────────────
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'index.html'));
});

app.listen(PORT, () => {
  console.log(`\n✅ 物料及商品数据治理系统已启动`);
  console.log(`🌐 本地访问地址: http://localhost:${PORT}`);
  console.log(`👤 默认账号: admin / 12345a\n`);
});
