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
`);

// Migrations
try { db.exec("ALTER TABLE categories ADD COLUMN key_attrs TEXT DEFAULT ''"); } catch(e) { /* column already exists */ }

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
    ['mat_extract_prompt', '提取物料名称的核心词：只保留最基础的品类级核心词（如\'灯管\'、\'扳手\'、\'螺栓\'），去掉所有修饰属性，包括规格、型号、品牌、材质、工艺，以及功能性修饰语（如\'LED\'、\'一体\'、\'紫外线\'、\'活口\'等）。核心词必须是具体的物料类别，不能仅有宽泛的泛指词（如只提取出\'配件\'、\'零件\'、\'材料\'等则视为失败）。如果治理失败，请返回格式：\'FAIL:失败原因\'，否则只返回品类级核心词。'],
    ['mat_similarity_threshold', '0.8'],
    ['cat_attr_prompt', '你是品类数据治理专家。根据给定的品类层级路径（一级>二级>三级>四级），为该末级品类生成4-5个关键SKU属性和2个补充属性。关键属性是决定SKU唯一性的核心维度（如规格、型号、材质、功率等），补充属性为辅助描述。只返回属性名称，用逗号分隔，例如：规格,型号,材质,功率,电压,品牌,产地。不要任何其他内容。'],
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
crudRoutes(app, 'goods-raw', 'goods_raw', ['code','name','std_code','std_name','status','source'], ['code','name']);
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

// ── Special: Bulk save unify results ─────────────
app.post('/api/goods-unify/save-results', auth, (req, res) => {
  const { stdGoods, rawGoods } = req.body;
  const insertStd = db.prepare('INSERT OR REPLACE INTO goods_std (id,code,name,category,attrs,raw_count,mat_code,created_at) VALUES (?,?,?,?,?,?,?,?)');
  const insertRaw = db.prepare('INSERT OR IGNORE INTO goods_raw (id,code,name,std_code,std_name,status,source,created_at) VALUES (?,?,?,?,?,?,?,?)');
  const updateRaw = db.prepare('UPDATE goods_raw SET std_code=?, std_name=?, status=? WHERE name=?');
  const saveAll = db.transaction(() => {
    stdGoods.forEach(s => insertStd.run(s.id || genId(), s.code, s.name, s.category || '', s.attrs || '', s.raw_count || 0, s.mat_code || '', s.created_at || Date.now()));
    rawGoods.forEach(r => {
      const ex = db.prepare('SELECT id FROM goods_raw WHERE name=?').get(r.name);
      if (ex) updateRaw.run(r.std_code, r.std_name, '已归一', r.name);
      else insertRaw.run(r.id || genId(), r.code, r.name, r.std_code, r.std_name, '已归一', r.source || 'CSV导入', r.created_at || Date.now());
    });
  });
  try { saveAll(); res.json({ ok: true }); } catch (e) { res.status(500).json({ error: e.message }); }
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
