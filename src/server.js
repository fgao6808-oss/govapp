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
    vector_text TEXT,
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
    mat_candidates TEXT,
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
    code TEXT,
    name TEXT UNIQUE,
    logo_urls TEXT DEFAULT '',
    created_at INTEGER
  );
`);

// Migrations
try { db.exec("ALTER TABLE categories ADD COLUMN key_attrs TEXT DEFAULT ''"); } catch(e) {}
try { db.exec("ALTER TABLE brands ADD COLUMN code TEXT"); } catch(e) {}
try { db.exec("ALTER TABLE brands ADD COLUMN logo_urls TEXT DEFAULT ''"); } catch(e) {}
try { db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_brands_code_unique ON brands(code) WHERE code IS NOT NULL AND code <> ''"); } catch(e) {}
try { db.exec('ALTER TABLE mat_archive ADD COLUMN vector_text TEXT'); } catch(e) {}
const newCols = [
  'orig_code', 'orig_name', 'orig_brand', 'spec', 'brand', 'model', 'core_word',
  'category_code', 'category_name', 'missing_attrs', 'std_name_proposed', 'match_status',
  'orig_spec', 'attrs_json', 'review_candidates', 'review_reason', 'review_score',
  'attr_pairs', 'attrs_vector', 'category_candidates', 'same_items', 'similar_items',
  'same_count', 'similar_count'
];
for (const col of newCols) {
  try { db.exec(`ALTER TABLE goods_raw ADD COLUMN ${col} TEXT`); } catch(e) {}
}
try { db.exec('ALTER TABLE goods_std ADD COLUMN category_code TEXT'); } catch(e) {}
try { db.exec('ALTER TABLE categories ADD COLUMN vector_text TEXT'); } catch(e) {}
try { db.exec('ALTER TABLE goods_std ADD COLUMN attrs_vector TEXT'); } catch(e) {}
try { db.exec('ALTER TABLE goods_std ADD COLUMN mat_candidates TEXT'); } catch(e) {}
try {
  db.prepare(`
    UPDATE goods_raw
    SET orig_spec = spec
    WHERE (orig_spec IS NULL OR orig_spec = '')
      AND spec IS NOT NULL
      AND TRIM(spec) != ''
      AND TRIM(spec) NOT LIKE '{%'
  `).run();
} catch (e) {}
try {
  db.prepare(`
    UPDATE goods_raw
    SET attrs_json = spec
    WHERE (attrs_json IS NULL OR attrs_json = '')
      AND spec IS NOT NULL
      AND TRIM(spec) LIKE '{%'
  `).run();
} catch (e) {}
try {
  db.prepare(`
    UPDATE goods_raw
    SET orig_brand = brand
    WHERE (orig_brand IS NULL OR orig_brand = '')
      AND brand IS NOT NULL
      AND TRIM(brand) <> ''
  `).run();
} catch (e) {}

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
    ['brand_code_prefix', 'BR'],
    ['brand_code_digits', '6'],
    ['goods_similarity_threshold', '0.45'],
    ['goods_core_word_prompt', '提取商品的关键品名，去掉品牌、规格、型号、包装、单位等修饰属性'],
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

const normalizeText = (value) => String(value || '')
  .toLowerCase()
  .replace(/[\s\-_/\\|,，。.、；;:：()（）【】\[\]<>《》"'`~!@#$%^&*+=?？]+/g, ' ')
  .replace(/\s+/g, ' ')
  .trim();

const vectorTokens = (value) => {
  const text = normalizeText(value);
  if (!text) return [];
  const chars = text.replace(/\s+/g, '');
  const tokens = text.split(' ').filter(Boolean);
  if (chars.length < 2) return tokens.length ? tokens : [chars];
  for (let i = 0; i < chars.length - 1; i++) tokens.push(chars.slice(i, i + 2));
  return tokens;
};

const vectorizeText = (value) => {
  const tokens = vectorTokens(value);
  if (!tokens.length) return {};
  const vec = {};
  tokens.forEach((token) => {
    let hash = 0;
    for (let i = 0; i < token.length; i++) hash = (hash * 31 + token.charCodeAt(i)) >>> 0;
    const idx = hash % 512;
    vec[idx] = (vec[idx] || 0) + 1;
  });
  let norm = 0;
  Object.values(vec).forEach(v => { norm += v * v; });
  norm = Math.sqrt(norm) || 1;
  Object.keys(vec).forEach((key) => { vec[key] = Number((vec[key] / norm).toFixed(6)); });
  return vec;
};

const vectorToText = (value) => JSON.stringify(vectorizeText(value));
const parseVectorText = (value) => {
  if (!value) return {};
  if (typeof value === 'object' && !Array.isArray(value)) return value;
  try {
    const parsed = JSON.parse(String(value || '{}'));
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
  } catch (e) {
    return {};
  }
};
const cosineFromVectors = (left, right) => {
  const a = parseVectorText(left);
  const b = parseVectorText(right);
  const keys = Object.keys(a);
  if (!keys.length || !Object.keys(b).length) return 0;
  let sum = 0;
  keys.forEach((key) => { if (b[key] != null) sum += Number(a[key]) * Number(b[key]); });
  return Number(sum.toFixed(6));
};

const attrsObjectToText = (attrs) => {
  const obj = parseAttrs(attrs);
  const keys = Object.keys(obj).sort();
  return keys.map(key => `${key}:${obj[key]}`).join(' ');
};

const genSequentialCode = (table, column, prefix, digits) => {
  const rows = db.prepare(`SELECT ${column} as code FROM ${table} WHERE ${column} LIKE ?`).all(`${prefix}%`);
  let maxSuffix = 0;

  for (const row of rows) {
    const code = String(row.code || '');
    if (!code.startsWith(prefix)) continue;
    const suffix = code.slice(prefix.length);
    if (!/^\d+$/.test(suffix)) continue;
    maxSuffix = Math.max(maxSuffix, parseInt(suffix, 10));
  }

  let next = maxSuffix + 1;
  while (true) {
    const candidate = prefix + String(next).padStart(digits, '0');
    const exists = db.prepare(`SELECT 1 FROM ${table} WHERE ${column}=?`).get(candidate);
    if (!exists) return candidate;
    next++;
  }
};

const stripCodeFence = (value) => String(value || '').replace(/```json?|```/g, '').trim();
const parseUploadedCsv = (text) => {
  const baseOptions = {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    bom: true,
    relax_quotes: true,
    relax_column_count: true,
  };

  try {
    return parse(text, baseOptions);
  } catch (err) {
    return parse(text, {
      ...baseOptions,
      quote: false,
      escape: '\\',
    });
  }
};

const filterMeaningfulCsvRows = (rows) => rows.filter(row => {
  const values = Object.values(row || {}).map(value => String(value || '').trim());
  return values.some(value => value !== '');
});
const mergeImportedGoodsRows = (rows) => {
  const merged = new Map();

  const chooseLonger = (left, right) => {
    const a = sanitizeImportedCell(left);
    const b = sanitizeImportedCell(right);
    if (!a) return b;
    if (!b) return a;
    return b.length > a.length ? b : a;
  };
  const chooseBetterName = (left, right) => {
    const score = (value) => {
      const text = sanitizeImportedCell(value);
      if (!text) return -1;
      let points = text.length;
      points -= (text.match(/"/g) || []).length * 5;
      if (/^[A-Za-z0-9/（）()]/.test(text)) points += 2;
      return points;
    };
    return score(right) > score(left) ? sanitizeImportedCell(right) : sanitizeImportedCell(left);
  };
  const mergeSpec = (left, right) => {
    const parts = [left, right]
      .flatMap(value => sanitizeImportedCell(value).split(/\s*\|\s*/))
      .map(value => value.trim())
      .filter(Boolean);
    return [...new Set(parts)].join(' | ');
  };

  rows.forEach(row => {
    const origCode = String(row['MRO商品编号'] || row['编码'] || '').trim();
    if (!origCode) return;

    if (!merged.has(origCode)) {
      merged.set(origCode, {
        ...row,
        'MRO商品名称': sanitizeImportedCell(row['MRO商品名称'] || row['名称']),
        '名称': sanitizeImportedCell(row['MRO商品名称'] || row['名称']),
        '规格': sanitizeImportedCell(row['规格'] || row['规格型号']),
        '规格型号': sanitizeImportedCell(row['规格'] || row['规格型号']),
        '品牌': sanitizeImportedCell(row['品牌']),
      });
      return;
    }

    const prev = merged.get(origCode);
    prev['MRO商品名称'] = chooseBetterName(prev['MRO商品名称'] || prev['名称'], row['MRO商品名称'] || row['名称']);
    prev['名称'] = prev['MRO商品名称'];
    prev['规格'] = mergeSpec(prev['规格'] || prev['规格型号'], row['规格'] || row['规格型号']);
    prev['规格型号'] = prev['规格'];
    prev['品牌'] = chooseLonger(prev['品牌'], row['品牌']);
  });

  return [...merged.values()];
};

const sanitizeImportedCell = (value) => {
  let text = String(value || '');
  if (!text) return '';

  text = text
    .normalize('NFKC')
    .replace(/\uFEFF/g, '')
    .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, ' ')
    .replace(/[\u200B-\u200D\u2060]/g, '')
    .replace(/\u00A0/g, ' ')
    .replace(/[“”]/g, '"')
    .replace(/[‘’]/g, "'")
    .replace(/，/g, ',')
    .replace(/；/g, ';')
    .replace(/：/g, ':')
    .replace(/（/g, '(')
    .replace(/）/g, ')')
    .replace(/【/g, '[')
    .replace(/】/g, ']')
    .replace(/\r?\n+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  if (text.startsWith('"') && text.endsWith('"') && text.length >= 2) {
    text = text.slice(1, -1).trim();
  }
  text = text
    .replace(/""/g, '"')
    .replace(/\s*([,;:])\s*/g, '$1 ')
    .replace(/\s{2,}/g, ' ')
    .replace(/\s+\)/g, ')')
    .replace(/\(\s+/g, '(')
    .trim();

  return text;
};

const cleanGoodsField = (value, field) => {
  let text = sanitizeImportedCell(value);

  if (field === 'name') {
    text = text
      .replace(/(^|[^A-Za-z0-9])1""([^A-Za-z0-9]|$)/g, '$11"$2')
      .replace(/""/g, '"')
      .replace(/\s*\/\s*/g, '/')
      .replace(/\s*-\s*/g, '-')
      .trim();
  }

  if (field === 'spec') {
    text = text
      .replace(/\s*\|\s*/g, ' | ')
      .replace(/,\s*$/g, '')
      .replace(/\s*:\s*/g, ':')
      .replace(/\s*,\s*/g, ', ')
      .trim();
  }

  if (field === 'brand') {
    text = text
      .replace(/\s*\/\s*/g, '/')
      .replace(/\s*\(\s*/g, '(')
      .replace(/\s*\)\s*/g, ')')
      .trim();
  }

  return text;
};

const extractPackagingSpec = (...values) => {
  const text = values
    .map(value => sanitizeImportedCell(value || ''))
    .filter(Boolean)
    .join(' ');
  if (!text) return '';

  const normalized = text
    .replace(/[，,;；]/g, ' ')
    .replace(/[xX×]/g, '*')
    .replace(/\s+/g, ' ')
    .trim();

  const patterns = [
    /\b\d+(?:\.\d+)?\s*(?:支|只|个|片|包|袋|盒|箱|瓶|桶|套|卷|条|块|张|板|扎|捆|罐|听|根|枚|本|把|副|对|台|组|粒|排|管)\s*[/／]\s*(?:盒|箱|包|袋|瓶|桶|件|套|卷|板|扎|捆|托|盘|排|管)\b/ig,
    /\b\d+(?:\.\d+)?\s*(?:入|装)\s*[/／]?\s*(?:盒|箱|包|袋|瓶|桶|件|套|卷|板|扎|捆|托|盘)\b/ig,
    /\b\d+(?:\.\d+)?\s*(?:克|g|kg|千克|公斤|斤|毫克|mg|毫升|ml|l|升)\s*\*\s*\d+(?:\.\d+)?\s*(?:袋|包|盒|箱|瓶|桶|支|只|个|片|罐|听|条|卷|块|张|板|套|根)\b/ig,
    /\b\d+(?:\.\d+)?\s*(?:克|g|kg|千克|公斤|斤|毫克|mg|毫升|ml|l|升)\b/ig,
  ];

  const matches = [];
  patterns.forEach((pattern) => {
    const found = normalized.match(pattern) || [];
    found.forEach((item) => {
      const cleaned = item.replace(/\s+/g, '');
      if (cleaned) matches.push(cleaned);
    });
  });

  return [...new Set(matches)][0] || '';
};

const normalizeBrandFormat = (brand) => {
  const raw = sanitizeImportedCell(brand);
  if (!raw) return '';

  const tokens = new Set();
  const pushToken = (value) => {
    const text = sanitizeImportedCell(value);
    if (!text || /^unknown$/i.test(text)) return;
    if (text) tokens.add(text);
  };

  raw.split(/[\/｜|]/).forEach(pushToken);
  const bracket = raw.match(/^(.+?)[(（]([^)）]+)[)）]$/);
  if (bracket) {
    pushToken(bracket[1]);
    pushToken(bracket[2]);
  }

  const zh = [];
  const en = [];
  const usedZh = new Set();
  const usedEn = new Set();
  [...tokens].forEach(token => {
    const cleaned = token.replace(/[()（）]/g, '').trim();
    if (!cleaned || /^unknown$/i.test(cleaned)) return;
    const hasZh = /[\u4e00-\u9fff]/.test(cleaned);
    const hasEn = /[A-Za-z]/.test(cleaned);
    const pushZh = (value) => {
      const text = sanitizeImportedCell(value).replace(/[()（）]/g, '').trim();
      if (!text || /^unknown$/i.test(text)) return;
      const key = normalizeText(text).replace(/\s+/g, '');
      if (!key || usedZh.has(key)) return;
      usedZh.add(key);
      zh.push(text);
    };
    const pushEn = (value) => {
      const text = sanitizeImportedCell(value).replace(/[()（）]/g, '').trim();
      if (!text || /^unknown$/i.test(text)) return;
      const key = normalizeText(text).replace(/\s+/g, '');
      if (!key || usedEn.has(key)) return;
      usedEn.add(key);
      en.push(text);
    };
    if (hasZh && hasEn) {
      const zhParts = cleaned.match(/[\u4e00-\u9fff]+/g) || [];
      const enParts = cleaned.match(/[A-Za-z][A-Za-z0-9.&-]*/g) || [];
      zhParts.forEach(pushZh);
      enParts.forEach(pushEn);
      return;
    }
    if (hasZh) pushZh(cleaned);
    else if (hasEn) pushEn(cleaned);
  });

  const zhVal = zh.sort((a, b) => b.length - a.length)[0] || '';
  const enVal = en.sort((a, b) => b.length - a.length)[0] || '';

  if (zhVal && enVal) return `${zhVal}｜${enVal}`;
  return zhVal || enVal || '';
};

const extractEnglishBrandHintFromName = (name) => {
  const text = sanitizeImportedCell(name || '');
  if (!text) return '';

  const patterns = [
    /([A-Za-z][A-Za-z0-9.&-]{1,30})\s*[\/｜|]\s*[\u4e00-\u9fff]{1,20}/g,
    /[\u4e00-\u9fff]{1,20}\s*[\/｜|]\s*([A-Za-z][A-Za-z0-9.&-]{1,30})/g,
    /([A-Za-z][A-Za-z0-9.&-]{1,30})\s*[（(]\s*[\u4e00-\u9fff]{1,20}\s*[)）]/g,
    /[\u4e00-\u9fff]{1,20}\s*[（(]\s*([A-Za-z][A-Za-z0-9.&-]{1,30})\s*[)）]/g,
  ];
  const candidates = [];
  patterns.forEach((reg) => {
    let match;
    // eslint-disable-next-line no-cond-assign
    while ((match = reg.exec(text)) !== null) {
      const hit = sanitizeImportedCell(match[1] || '').replace(/[^A-Za-z0-9.&-]/g, '');
      if (hit && /[A-Za-z]{2,}/.test(hit)) candidates.push(hit);
    }
  });
  if (!candidates.length) return '';
  return candidates.sort((a, b) => b.length - a.length)[0];
};

const normalizeCoreWordText = (value) => sanitizeImportedCell(value || '')
  .replace(/[｜|]/g, ' ')
  .replace(/\s+/g, ' ')
  .trim();

const mergeCoreWords = (categoryCoreWord, functionCoreWord, fallbackCoreWord) => {
  const categoryWord = normalizeCoreWordText(categoryCoreWord);
  const functionWord = normalizeCoreWordText(functionCoreWord);
  const fallbackWord = normalizeCoreWordText(fallbackCoreWord);
  const unknown = 'unknown';

  if (categoryWord && categoryWord !== unknown && functionWord && functionWord !== unknown) {
    if (functionWord.includes(categoryWord)) return functionWord;
    if (categoryWord.includes(functionWord)) return categoryWord;

    const hasZh = /[\u4e00-\u9fff]/.test(`${functionWord}${categoryWord}`);
    return hasZh ? `${functionWord}${categoryWord}` : `${functionWord} ${categoryWord}`;
  }

  const one = functionWord || categoryWord || fallbackWord;
  if (!one || one === unknown) return unknown;

  // 兼容历史“插座+五孔”格式，优先转成“功能词+品类词”。
  if (one.includes('+')) {
    const parts = one.split('+').map(v => sanitizeImportedCell(v)).filter(Boolean);
    if (parts.length === 2) {
      const [a, b] = parts;
      if (/[0-9一二三四五六七八九十单双多]/.test(b)) return `${b}${a}`;
      return `${a}${b}`;
    }
    return parts.join('');
  }
  return one.replace(/\+/g, '').trim() || unknown;
};

const resetGoodsRawUnifyStateSql = `
  UPDATE goods_raw
  SET std_code=NULL,
      std_name=NULL,
      brand=NULL,
      model=NULL,
      core_word=NULL,
      attrs_json=NULL,
      attr_pairs=NULL,
      attrs_vector=NULL,
      missing_attrs=NULL,
      std_name_proposed=NULL,
      category_candidates=NULL,
      same_items=NULL,
      similar_items=NULL,
      same_count=NULL,
      similar_count=NULL,
      status='未归一',
      review_candidates=NULL,
      review_reason=NULL,
      review_score=NULL,
      match_status = CASE
        WHEN category_code IS NOT NULL AND TRIM(category_code) <> '' THEN '分类已匹配'
        ELSE '待分类'
      END
`;
const parseAttrs = (value) => {
  const text = String(value || '').trim();
  if (!text || !text.startsWith('{')) return {};
  try {
    const parsed = JSON.parse(text);
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
  } catch (e) {
    return {};
  }
};

const buildCategoryVectorSource = (category) => (
  [category.l1, category.l2, category.l3, category.l4, category.key_attrs]
    .filter(Boolean)
    .join(' ')
);

const buildMatVectorSource = (mat) => (
  [mat.name, mat.synonyms]
    .filter(Boolean)
    .join(' ')
);

const splitAliasTokens = (value) => String(value || '')
  .split(/[\n,，;；、/｜|]+/)
  .map(v => sanitizeImportedCell(v))
  .filter(Boolean);

const buildCoreWordSynonymText = (coreWord = '') => {
  const terms = new Set();
  splitAliasTokens(coreWord).forEach(token => terms.add(token));
  if (!terms.size) return '';

  const mats = db.prepare('SELECT name, synonyms FROM mat_archive').all();
  [...terms].forEach((term) => {
    const normTerm = normalizeText(term);
    mats.forEach((mat) => {
      const aliases = [mat.name, ...splitAliasTokens(mat.synonyms)];
      const normalizedAliases = aliases.map(alias => normalizeText(alias)).filter(Boolean);
      if (normalizedAliases.some(alias => alias === normTerm || alias.includes(normTerm) || normTerm.includes(alias))) {
        aliases.forEach(alias => alias && terms.add(alias));
      }
    });
  });

  return [...terms].join(' ');
};

const buildGoodsCoreWordMaterialSource = (stdCode, stdName = '', attrs = '') => {
  const rawRows = db.prepare(`
    SELECT core_word
    FROM goods_raw
    WHERE std_code = ?
      AND core_word IS NOT NULL
      AND TRIM(core_word) <> ''
      AND TRIM(core_word) <> 'unknown'
  `).all(stdCode);

  const terms = new Set();
  rawRows.forEach((row) => {
    splitAliasTokens(row.core_word).forEach(token => terms.add(token));
  });

  if (!terms.size) {
    splitAliasTokens(stdName).forEach(token => terms.add(token));
  }

  const synonymText = buildCoreWordSynonymText([...terms].join(' '));
  splitAliasTokens(synonymText).forEach(token => terms.add(token));

  const attrsText = attrsObjectToText(attrs);
  return [[...terms].join(' '), attrsText].filter(Boolean).join(' ');
};

const filterMatCandidates = (candidates, currentMatCode = '') => {
  const current = String(currentMatCode || '').trim();
  const list = Array.isArray(candidates) ? candidates : [];
  return list.filter((item) => item && item.code && item.code !== current);
};

const filterCategoryCandidates = (candidates, currentCategoryCode = '') => {
  const current = String(currentCategoryCode || '').trim();
  const list = Array.isArray(candidates) ? candidates : [];
  return list.filter((item) => item && item.code && item.code !== current);
};

const ensureCategoryVectors = () => {
  try {
    const cats = db.prepare('SELECT id, l1, l2, l3, l4, key_attrs, vector_text FROM categories').all();
    const update = db.prepare('UPDATE categories SET vector_text=? WHERE id=?');
    const tx = db.transaction((rows) => {
      rows.forEach((cat) => {
        const source = buildCategoryVectorSource(cat);
        const nextVec = vectorToText(source);
        if (!cat.vector_text || cat.vector_text !== nextVec) update.run(nextVec, cat.id);
      });
    });
    tx(cats);
  } catch (e) {}
};

const ensureMatVectors = () => {
  try {
    const mats = db.prepare('SELECT id, name, synonyms, category, vector_text FROM mat_archive').all();
    const update = db.prepare('UPDATE mat_archive SET vector_text=? WHERE id=?');
    const tx = db.transaction((rows) => {
      rows.forEach((mat) => {
        const source = buildMatVectorSource(mat);
        const nextVec = vectorToText(source);
        if (!mat.vector_text || mat.vector_text !== nextVec) update.run(nextVec, mat.id);
      });
    });
    tx(mats);
  } catch (e) {}
};

const getOrderedAttrValues = (attrs, keyAttrs) => {
  if (!attrs || typeof attrs !== 'object') return [];

  const orderedKeys = String(keyAttrs || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
  const used = new Set();
  const values = [];

  orderedKeys.forEach(key => {
    const value = attrs[key];
    if (value && value !== 'unknown') {
      values.push(value);
      used.add(key);
    }
  });

  Object.keys(attrs).sort().forEach(key => {
    const value = attrs[key];
    if (!used.has(key) && value && value !== 'unknown') values.push(value);
  });

  return values;
};

const getOrderedAttrEntries = (attrs, keyAttrs) => {
  if (!attrs || typeof attrs !== 'object') return [];

  const orderedKeys = String(keyAttrs || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
  const used = new Set();
  const pairs = [];

  orderedKeys.forEach(key => {
    const value = attrs[key];
    if (value && value !== 'unknown') {
      pairs.push([key, value]);
      used.add(key);
    }
  });

  Object.keys(attrs).sort().forEach(key => {
    const value = attrs[key];
    if (!used.has(key) && value && value !== 'unknown') pairs.push([key, value]);
  });

  return pairs;
};

const normalizeStdNameToken = (value) => normalizeText(sanitizeImportedCell(value || '')).replace(/\s+/g, '');

const isDuplicateStdNamePart = (value, references = []) => {
  const current = normalizeStdNameToken(value);
  if (!current) return false;
  return references.some((item) => {
    const ref = normalizeStdNameToken(item);
    if (!ref) return false;
    return current === ref || current.includes(ref) || ref.includes(current);
  });
};

const buildStdName = ({ brand, coreWord, model, pack, attrs, keyAttrs }) => {
  const headParts = [brand, coreWord, model, pack]
    .map(value => sanitizeImportedCell(value || ''))
    .filter(value => value && value !== 'unknown');

  const metaKeyPattern = /(品牌|核心词|品类核心词|功能核心词|型号|包装|包装规格)/;
  const filteredAttrValues = getOrderedAttrEntries(attrs, keyAttrs)
    .filter(([key, value]) => {
      const safeKey = sanitizeImportedCell(key || '');
      const safeValue = sanitizeImportedCell(value || '');
      if (!safeValue || safeValue === 'unknown') return false;
      if (metaKeyPattern.test(safeKey)) return false;
      return !isDuplicateStdNamePart(safeValue, headParts);
    })
    .map(([key, value]) => {
      const safeKey = sanitizeImportedCell(key || '');
      const safeValue = sanitizeImportedCell(value || '');
      return safeValue ? `${safeKey}:${safeValue}` : '';
    })
    .filter(Boolean);

  return [...headParts, ...filteredAttrValues].join(' ').trim();
};

const extractCandidateCode = (content, candidates) => {
  const text = String(content || '').trim();
  if (!text) return '';
  const codes = new Set(candidates.map(item => item.code));

  if (codes.has(text)) return text;
  for (const candidate of candidates) {
    if (text.startsWith(candidate.code) || text.includes(`${candidate.code}|`)) return candidate.code;
  }

  const matched = text.match(/[A-Z]{1,6}\d{2,}/);
  return matched && codes.has(matched[0]) ? matched[0] : '';
};

const extractCandidateIds = (content, candidates) => {
  const text = String(content || '').trim();
  const ids = new Set(candidates.map(item => item.id));
  if (!text) return [];
  try {
    const parsed = JSON.parse(stripCodeFence(text));
    if (Array.isArray(parsed)) return parsed.filter(id => ids.has(String(id))).map(String);
  } catch (e) {}
  const result = [];
  ids.forEach((id) => {
    if (text.includes(id)) result.push(id);
  });
  return [...new Set(result)];
};

const getAttrOverlapScore = (left, right) => {
  const entries = Object.entries(left || {}).filter(([, value]) => value && value !== 'unknown');
  if (!entries.length) return 0;

  let matched = 0;
  entries.forEach(([key, value]) => {
    const target = right?.[key];
    if (target && normalizeText(target) === normalizeText(value)) matched++;
  });
  return matched / entries.length;
};

const scoreStdCandidate = (item, candidate) => {
  const candidateName = normalizeText(candidate.name);
  const itemAttrs = parseAttrs(item.attrs_json || item.spec);
  const candidateAttrs = parseAttrs(candidate.attrs);

  let score = getDice(item.std_name_proposed, candidate.name) * 0.7;
  if (item.brand && item.brand !== 'unknown' && candidateName.includes(normalizeText(item.brand))) score += 0.12;
  if (item.core_word && item.core_word !== 'unknown' && candidateName.includes(normalizeText(item.core_word))) score += 0.1;
  if (item.model && item.model !== 'unknown' && candidateName.includes(normalizeText(item.model))) score += 0.08;
  score += getAttrOverlapScore(itemAttrs, candidateAttrs) * 0.2;

  return Math.min(score, 1);
};

const getReviewThreshold = (autoMergeThreshold) => Math.max(0.18, autoMergeThreshold * 0.5);

const buildReviewCandidates = (candidates) => candidates.map(candidate => ({
  code: candidate.code,
  name: candidate.name,
  category: candidate.category,
  category_code: candidate.category_code || '',
  score: Number((candidate.score || 0).toFixed(4)),
}));

const savePendingReview = (item, candidates, reason, topScore) => {
  db.prepare(`
    UPDATE goods_raw
    SET std_code=NULL,
        std_name=NULL,
        status=?,
        match_status=?,
        review_candidates=?,
        review_reason=?,
        review_score=?
    WHERE id=?
  `).run(
    '待审核',
    '待审核',
    JSON.stringify(buildReviewCandidates(candidates)),
    reason,
    topScore == null ? null : String(Number(topScore.toFixed(4))),
    item.id
  );
};

const linkRawToExistingStd = (item, stdCode) => {
  const std = db.prepare('SELECT code, name FROM goods_std WHERE code=?').get(stdCode);
  if (!std) throw new Error('标准商品不存在');

  db.prepare('UPDATE goods_std SET raw_count = raw_count + 1 WHERE code=?').run(stdCode);
  db.prepare(`
    UPDATE goods_raw
    SET std_code=?, std_name=?, status=?, match_status=?,
        review_candidates=NULL, review_reason=NULL, review_score=NULL
    WHERE id=?
  `).run(std.code, std.name, '已归一', '已完成', item.id);

  return { stdCode: std.code, stdName: std.name };
};

const genCategoryStdCode = (categoryCode) => {
  const prefix = String(categoryCode || '').trim();
  if (!prefix) throw new Error('缺少四级类目编码，无法生成标准商品编码');
  const rows = db.prepare('SELECT code FROM goods_std WHERE code LIKE ?').all(`${prefix}%`);
  let maxSerial = 0;
  rows.forEach((row) => {
    const code = String(row.code || '');
    if (!code.startsWith(prefix)) return;
    const suffix = code.slice(prefix.length);
    if (!/^\d{4}$/.test(suffix)) return;
    maxSerial = Math.max(maxSerial, parseInt(suffix, 10));
  });
  return `${prefix}${String(maxSerial + 1).padStart(4, '0')}`;
};

const buildStdNameFromRaw = (item) => {
  const attrs = parseAttrs(item.attr_pairs || item.attrs_json || item.spec);
  const keyAttrs = item.category_code
    ? db.prepare('SELECT key_attrs FROM categories WHERE code=?').get(item.category_code)?.key_attrs || ''
    : '';
  return buildStdName({
    brand: normalizeBrandFormat(item.brand),
    coreWord: item.core_word,
    model: item.model,
    pack: extractPackagingSpec(item.orig_spec, item.spec, item.orig_name, item.name),
    attrs,
    keyAttrs,
  });
};

const createStdFromRaw = (item) => {
  const stdCode = genCategoryStdCode(item.category_code);
  const stdName = buildStdNameFromRaw(item) || item.std_name_proposed || item.name;
  const attrPairs = item.attr_pairs || item.attrs_json || item.spec || '{}';
  const attrsVector = item.attrs_vector || vectorToText(attrsObjectToText(attrPairs));
  db.prepare(`
    INSERT INTO goods_std (id, code, name, category, category_code, attrs, attrs_vector, raw_count, created_at)
    VALUES (?,?,?,?,?,?,?,?,?)
  `).run(
    genId(),
    stdCode,
    stdName,
    item.category_name,
    item.category_code || null,
    attrPairs,
    attrsVector,
    1,
    Date.now()
  );
  db.prepare(`
    UPDATE goods_raw
    SET std_code=?, std_name=?, status=?, match_status=?,
        review_candidates=NULL, review_reason=NULL, review_score=NULL
    WHERE id=?
  `).run(stdCode, stdName, '已归一', '已完成', item.id);

  return { stdCode, stdName };
};

const genMatCode = () => {
  const prefix = getSetting('mat_code_prefix') || 'ML';
  const digits = parseInt(getSetting('mat_code_digits')) || 6;
  return genSequentialCode('mat_archive', 'code', prefix, digits);
};
const genGoodsCode = () => {
  const prefix = getSetting('goods_code_prefix') || 'SP';
  const digits = parseInt(getSetting('goods_code_digits')) || 6;
  return genSequentialCode('goods_std', 'code', prefix, digits);
};
const genBrandCode = () => {
  const prefix = getSetting('brand_code_prefix') || 'BR';
  const digits = parseInt(getSetting('brand_code_digits')) || 6;
  return genSequentialCode('brands', 'code', prefix, digits);
};
const ensureBrandExists = (name, logoUrls = '') => {
  const brandName = sanitizeImportedCell(name);
  if (!brandName || /^unknown$/i.test(brandName)) return null;

  const existing = db.prepare('SELECT id, code FROM brands WHERE name=?').get(brandName);
  if (existing) return existing;

  const code = genBrandCode();
  db.prepare('INSERT INTO brands (id, code, name, logo_urls, created_at) VALUES (?, ?, ?, ?, ?)')
    .run(genId(), code, brandName, String(logoUrls || '').trim(), Date.now());
  return { code };
};

// Backfill code for historical brands once on startup.
try {
  const missings = db.prepare("SELECT id FROM brands WHERE code IS NULL OR TRIM(code)='' ORDER BY created_at ASC, id ASC").all();
  if (missings.length) {
    const updateStmt = db.prepare('UPDATE brands SET code=? WHERE id=?');
    const tx = db.transaction((rows) => {
      rows.forEach((row) => updateStmt.run(genBrandCode(), row.id));
    });
    tx(missings);
  }
} catch (e) {}

ensureCategoryVectors();

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
const wait = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const isTransientAIError = (message = '') => {
  const text = String(message || '').toLowerCase();
  return [
    'timeout',
    'timed out',
    'socket hang up',
    'econnreset',
    'etimedout',
    'eai_again',
    '502',
    '503',
    '504',
    'rate limit',
    'temporarily unavailable',
    'overloaded',
    'bad gateway',
    'service unavailable',
    'gateway timeout',
    'ai请求失败',
    'ai请求超时',
    'ai响应解析失败',
  ].some(keyword => text.includes(keyword));
};

const requestAIChatOnce = ({ systemPrompt, userPrompt }) => {
  const apiKey = getSetting('api_key');
  const apiType = getSetting('api_type') || 'deepseek';
  const apiBase = getSetting('api_base');
  const apiModel = getSetting('api_model');

  if (!apiKey) throw new Error('请先在系统设置中配置AI API密钥');

  const endpoints = {
    openai: 'https://api.openai.com/v1/chat/completions',
    deepseek: 'https://api.deepseek.com/v1/chat/completions',
    qianwen: 'https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions',
    claude: 'https://api.anthropic.com/v1/messages',
  };
  const models = { openai: 'gpt-4o-mini', deepseek: 'deepseek-chat', qianwen: 'qwen-plus', claude: 'claude-haiku-4-5-20251001' };

  const url = apiBase || endpoints[apiType] || endpoints.deepseek;
  const model = apiModel || models[apiType] || 'deepseek-chat';

  return new Promise((resolve, reject) => {
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
        timeout: 60000
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
              const codeText = parsed.error.code ? ` (${parsed.error.code})` : '';
              return reject(new Error((parsed.error.message || JSON.stringify(parsed.error)) + codeText));
            }
            const content = parsed.choices?.[0]?.message?.content || parsed.content?.[0]?.text || '';
            console.log(`[AI] Success (Response length: ${content.length})`);
            resolve({ content });
          } catch (e) {
            console.error(`[AI] Parse Error: ${e.message}`);
            reject(new Error('AI响应解析失败: ' + data.slice(0, 200)));
          }
        });
      });
      apiReq.on('timeout', () => {
        console.error('[AI] Request Timeout (60s)');
        apiReq.destroy();
        reject(new Error('AI请求超时，请重试'));
      });
      apiReq.on('error', e => {
        console.error(`[AI] Network Error: ${e.message}`);
        reject(new Error('AI请求失败: ' + e.message));
      });
      apiReq.write(bodyStr);
      apiReq.end();
    } catch (e) {
      console.error(`[AI] Unexpected Error: ${e.message}`);
      reject(e);
    }
  });
};

const requestAIChat = async ({ systemPrompt, userPrompt }, options = {}) => {
  const maxRetries = Math.max(0, Number(options.maxRetries ?? 2));
  let attempt = 0;
  let lastError;

  while (attempt <= maxRetries) {
    try {
      if (attempt > 0) console.log(`[AI] Retry attempt ${attempt}/${maxRetries}`);
      return await requestAIChatOnce({ systemPrompt, userPrompt });
    } catch (e) {
      lastError = e;
      const shouldRetry = attempt < maxRetries && isTransientAIError(e.message);
      if (!shouldRetry) throw e;
      const delayMs = 1200 * (attempt + 1);
      console.warn(`[AI] Transient failure, retrying in ${delayMs}ms: ${e.message}`);
      await wait(delayMs);
      attempt += 1;
    }
  }

  throw lastError || new Error('AI请求失败');
};

app.post('/api/ai/chat', auth, async (req, res) => {
  const { systemPrompt, userPrompt } = req.body;
  try {
    const result = await requestAIChat({ systemPrompt, userPrompt });
    res.json(result);
  } catch (e) {
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
  const safeLimit = Math.min(Math.max(parseInt(limit, 10) || 20, 1), 100);
  const safePage = Math.max(parseInt(page, 10) || 1, 1);
  const offset = (safePage - 1) * safeLimit;
  try {
    const keyword = `%${q}%`;
    const list = db.prepare(`
      SELECT * FROM brands
      WHERE name LIKE ? OR IFNULL(code, '') LIKE ?
      ORDER BY created_at DESC
      LIMIT ? OFFSET ?
    `).all(keyword, keyword, safeLimit, offset);
    const total = db.prepare(`
      SELECT count(*) as c FROM brands
      WHERE name LIKE ? OR IFNULL(code, '') LIKE ?
    `).get(keyword, keyword).c;
    res.json({ data: list, list, total, page: safePage, limit: safeLimit });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/brands', auth, (req, res) => {
  const { name, logo_urls } = req.body;
  if (!name) return res.status(400).json({ error: '品牌名称为必填' });
  try {
    const code = genBrandCode();
    const stmt = db.prepare('INSERT INTO brands (id, code, name, logo_urls, created_at) VALUES (?, ?, ?, ?, ?)');
    stmt.run(genId(), code, name.trim(), (logo_urls || '').trim(), Date.now());
    res.json({ success: true, code });
  } catch (err) {
    if (err.message.includes('UNIQUE constraint failed')) {
      res.status(400).json({ error: err.message.includes('brands.code') ? '品牌编码已存在' : '品牌已存在' });
    } else {
      res.status(500).json({ error: err.message });
    }
  }
});

app.put('/api/brands/:id', auth, (req, res) => {
  const { code, name, logo_urls } = req.body;
  if (!name) return res.status(400).json({ error: '品牌名称为必填' });
  try {
    const existing = db.prepare('SELECT code FROM brands WHERE id=?').get(req.params.id);
    if (!existing) return res.status(404).json({ error: '品牌不存在' });
    const nextCode = code === undefined ? existing.code : ((code || '').trim() || null);
    const stmt = db.prepare('UPDATE brands SET code=?, name=?, logo_urls=? WHERE id=?');
    stmt.run(nextCode, name.trim(), (logo_urls || '').trim(), req.params.id);
    res.json({ success: true });
  } catch (err) {
    if (err.message.includes('UNIQUE constraint failed')) {
      res.status(400).json({ error: err.message.includes('brands.code') ? '品牌编码已被使用' : '品牌名称已被使用' });
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

app.post('/api/brands/bulk-delete', auth, (req, res) => {
  const { ids } = req.body;
  if (!ids || !ids.length) return res.json({ ok: true });
  try {
    const stmt = db.prepare('DELETE FROM brands WHERE id=?');
    const tx = db.transaction((list) => {
      list.forEach((id) => stmt.run(id));
    });
    tx(ids);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/brands/clear-all', auth, (req, res) => {
  try {
    db.prepare('DELETE FROM brands').run();
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/settings', auth, (req, res) => {
  const keys = ['api_type', 'api_key', 'api_base', 'api_model', 'mat_code_prefix', 'mat_code_digits', 'goods_code_prefix', 'goods_code_digits', 'goods_similarity_threshold', 'goods_core_word_prompt', 'mat_extract_prompt', 'mat_similarity_threshold', 'cat_attr_prompt', 'username'];
  const result = {};
  keys.forEach(k => { result[k] = getSetting(k) || ''; });
  if (result.api_key) result.api_key = result.api_key.slice(0, 4) + '****' + result.api_key.slice(-4);
  res.json(result);
});

app.post('/api/settings', auth, (req, res) => {
  const { api_type, api_key, api_base, api_model, mat_code_prefix, mat_code_digits, goods_code_prefix, goods_code_digits, goods_similarity_threshold, goods_core_word_prompt, mat_extract_prompt, mat_similarity_threshold, cat_attr_prompt } = req.body;
  if (api_type) setSetting('api_type', api_type);
  if (api_key && !api_key.includes('****')) setSetting('api_key', api_key);
  if (api_base !== undefined) setSetting('api_base', api_base);
  if (api_model !== undefined) setSetting('api_model', api_model);
  if (mat_code_prefix) setSetting('mat_code_prefix', mat_code_prefix);
  if (mat_code_digits) setSetting('mat_code_digits', mat_code_digits);
  if (goods_code_prefix) setSetting('goods_code_prefix', goods_code_prefix);
  if (goods_code_digits) setSetting('goods_code_digits', goods_code_digits);
  if (goods_similarity_threshold !== undefined) setSetting('goods_similarity_threshold', goods_similarity_threshold);
  if (goods_core_word_prompt !== undefined) setSetting('goods_core_word_prompt', goods_core_word_prompt);
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
    const reservedQueryKeys = new Set(['q', 'filter', 'filterField', 'page', 'limit']);
    const filterableFields = new Set(['id', 'created_at', ...fields, ...searchFields]);
    if (q && searchFields.length) {
      conds.push('(' + searchFields.map(f => `${f} LIKE ?`).join(' OR ') + ')');
      searchFields.forEach(() => params.push(`%${q}%`));
    }
    if (filter && filterField && filter !== '全部') { conds.push(`${filterField} = ?`); params.push(filter); }
    Object.entries(req.query || {}).forEach(([key, value]) => {
      if (reservedQueryKeys.has(key) || !filterableFields.has(key)) return;
      const text = String(value ?? '').trim();
      if (!text || text === '全部') return;
      conds.push(`${key} LIKE ?`);
      params.push(`%${text}%`);
    });
    
    const where = conds.length ? ' WHERE ' + conds.join(' AND ') : '';
    const total = db.prepare(`SELECT COUNT(*) as c ${sql} ${where}`).get(...params).c;
    
    let finalSql = `SELECT * ${sql} ${where} ORDER BY ${options.orderBy || 'created_at DESC'}`;
    if (page && limit) {
      const safeLimit = Math.min(Math.max(parseInt(limit, 10) || 100, 1), 100);
      const safePage = Math.max(parseInt(page, 10) || 1, 1);
      finalSql += ` LIMIT ? OFFSET ?`;
      params.push(safeLimit, (safePage - 1) * safeLimit);
    }
    let rows = db.prepare(finalSql).all(...params);
    if (options.postProcessList) rows = options.postProcessList(rows);
    res.json(page ? { data: rows, total } : rows);
  });

  // Create
  app.post(`/api/${prefix}`, auth, (req, res) => {
    const id = genId();
    let data = { id, ...req.body, created_at: Date.now() };
    if (options.beforeCreate) {
      const next = options.beforeCreate(data, req);
      if (next && typeof next === 'object') data = next;
    }
    const cols = ['id', ...fields, 'created_at'].filter(f => data[f] !== undefined);
    const sql = `INSERT INTO ${table} (${cols.join(',')}) VALUES (${cols.map(() => '?').join(',')})`;
    try {
      db.prepare(sql).run(cols.map(c => data[c] ?? null));
      if (options.afterCreate) options.afterCreate(data, req);
      res.json({ id, ...data });
    } catch (e) { res.status(400).json({ error: e.message }); }
  });

  // Update
  app.put(`/api/${prefix}/:id`, auth, (req, res) => {
    let patch = { ...req.body };
    if (options.beforeUpdate) {
      const next = options.beforeUpdate(patch, req);
      if (next && typeof next === 'object') patch = next;
    }
    const updates = fields.filter(f => patch[f] !== undefined).map(f => `${f}=?`);
    if (!updates.length) return res.json({ ok: true });
    const vals = fields.filter(f => patch[f] !== undefined).map(f => patch[f]);
    db.prepare(`UPDATE ${table} SET ${updates.join(',')} WHERE id=?`).run(...vals, req.params.id);
    if (options.afterUpdate) options.afterUpdate(req.params.id, patch, req);
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
    if (table === 'goods_raw') {
      const raw = db.prepare('SELECT std_code FROM goods_raw WHERE id=?').get(id);
      if (raw?.std_code) db.prepare('UPDATE goods_std SET raw_count = CASE WHEN raw_count > 0 THEN raw_count - 1 ELSE 0 END WHERE code=?').run(raw.std_code);
    }
    if (table === 'goods_std') {
      const std = db.prepare('SELECT code FROM goods_std WHERE id=?').get(id);
      if (std?.code) {
        db.prepare(`${resetGoodsRawUnifyStateSql} WHERE std_code=?`).run(std.code);
        db.prepare('DELETE FROM mapping WHERE goods_code=?').run(std.code);
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
        if (table === 'goods_raw') {
          const raw = db.prepare('SELECT std_code FROM goods_raw WHERE id=?').get(id);
          if (raw?.std_code) db.prepare('UPDATE goods_std SET raw_count = CASE WHEN raw_count > 0 THEN raw_count - 1 ELSE 0 END WHERE code=?').run(raw.std_code);
        }
        if (table === 'goods_std') {
          const std = db.prepare('SELECT code FROM goods_std WHERE id=?').get(id);
          if (std?.code) {
            db.prepare(`${resetGoodsRawUnifyStateSql} WHERE std_code=?`).run(std.code);
            db.prepare('DELETE FROM mapping WHERE goods_code=?').run(std.code);
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
    if (table === 'goods_raw') db.prepare("UPDATE goods_std SET raw_count = 0").run();
    if (table === 'goods_std') {
      db.prepare(resetGoodsRawUnifyStateSql).run();
      db.prepare('DELETE FROM mapping').run();
    }
    db.prepare(`DELETE FROM ${table}`).run();
    res.json({ ok: true });
  });
}

crudRoutes(app, 'mat-raw', 'mat_raw', ['code','name','spec','new_code','new_name','status','source'], ['code','name']);
crudRoutes(app, 'mat-archive', 'mat_archive', ['code','name','synonyms','category','vector_text','raw_count'], ['code','name','synonyms'], {
  beforeCreate: (data) => {
    const source = buildMatVectorSource(data);
    return { ...data, vector_text: vectorToText(source) };
  },
  beforeUpdate: (patch, req) => {
    const prev = db.prepare('SELECT name, synonyms, category FROM mat_archive WHERE id=?').get(req.params.id) || {};
    const merged = {
      name: patch.name !== undefined ? patch.name : prev.name,
      synonyms: patch.synonyms !== undefined ? patch.synonyms : prev.synonyms,
      category: patch.category !== undefined ? patch.category : prev.category,
    };
    return { ...patch, vector_text: vectorToText(buildMatVectorSource(merged)) };
  },
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
crudRoutes(app, 'categories', 'categories', ['code','l1','l2','l3','l4','key_attrs','status'], ['code','l1','l2','l3','l4'], { orderBy: 'code ASC' });

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
crudRoutes(app, 'goods-raw', 'goods_raw', ['code','name','std_code','std_name','status','source','orig_code','orig_name','orig_brand','spec','orig_spec','attrs_json','attr_pairs','attrs_vector','brand','model','core_word','category_code','category_name','category_candidates','missing_attrs','std_name_proposed','match_status','same_items','similar_items','same_count','similar_count','review_candidates','review_reason','review_score'], ['code','name','orig_code','orig_name','orig_brand','brand','model','core_word']);

// ── Goods Unify Pipeline Steps ────────────────────

// Step 2: Import & Merge
app.post('/api/goods-unify/import', auth, (req, res) => {
  const { rows } = req.body;
  if (!rows || !rows.length) return res.status(400).json({ error: '数据为空' });

  const results = {
    total: rows.length,
    valid: 0,
    invalid: 0,
    success: 0,
    duplicate: 0,
    cleanedFields: 0,
    insertedIds: [],
    insertedItems: [],
    skipped: [],
    error_logs: [],
  };
  const insertStmt = db.prepare(`
    INSERT INTO goods_raw (id, code, name, orig_code, orig_name, orig_brand, spec, orig_spec, brand, status, source, match_status, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const tx = db.transaction(() => {
    rows.forEach((r, idx) => {
      const rawCode = r['MRO商品编号'] || r['编码'] || '';
      const rawName = r['MRO商品名称'] || r['名称'] || '';
      const rawSpec = r['规格'] || r['规格型号'] || '';
      const rawBrand = r['品牌'] || '';
      const origBrand = String(rawBrand || '').replace(/\r?\n+/g, ' ').trim();

      const origCode = sanitizeImportedCell(rawCode);
      const origName = cleanGoodsField(rawName, 'name');
      const spec = cleanGoodsField(rawSpec, 'spec');
      const brand = cleanGoodsField(rawBrand, 'brand');
      const mergedName = [origName, spec, brand].filter(Boolean).join(' ');

      const invalidReasons = [];
      if (!String(rawCode || '').trim()) invalidReasons.push('缺少必填字段：MRO商品编号');
      else if (!origCode) invalidReasons.push('MRO商品编号清洗后为空，请检查非法字符或异常格式');
      if (!String(rawName || '').trim()) invalidReasons.push('缺少必填字段：MRO商品名称');
      else if (!origName) invalidReasons.push('MRO商品名称清洗后为空，请检查非法字符或异常格式');

      if (invalidReasons.length) {
        results.invalid++;
        results.skipped.push(origCode || origName || '空行');
        results.error_logs.push({
          row_no: idx + 2,
          raw_code: String(rawCode || '').trim(),
          raw_name: String(rawName || '').trim(),
          raw_spec: String(rawSpec || '').trim(),
          raw_brand: String(rawBrand || '').trim(),
          reason: invalidReasons.join('；'),
        });
        return;
      }
      results.valid++;
      if (origCode !== String(rawCode || '').trim()) results.cleanedFields++;
      if (origName !== String(rawName || '').trim()) results.cleanedFields++;
      if (spec !== String(rawSpec || '').trim()) results.cleanedFields++;
      if (origBrand !== String(rawBrand || '').trim()) results.cleanedFields++;

      const existing = db.prepare('SELECT id FROM goods_raw WHERE orig_code = ?').get(origCode);
      if (existing) {
        results.duplicate++;
        results.error_logs.push({
          row_no: idx + 2,
          raw_code: String(rawCode || '').trim(),
          raw_name: String(rawName || '').trim(),
          raw_spec: String(rawSpec || '').trim(),
          raw_brand: String(rawBrand || '').trim(),
          reason: '商品编号重复，系统已跳过，不重复插入',
        });
        return;
      }
      const id = genId();
      insertStmt.run(id, origCode, mergedName, origCode, origName, origBrand, spec, spec, brand, '未归一', 'Excel导入', '待分类', Date.now());
      results.insertedIds.push(id);
      results.insertedItems.push({ id, orig_code: origCode, orig_name: origName });
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

app.post('/api/goods-unify/clean-data', auth, (req, res) => {
  const { ids } = req.body;
  if (!ids || !ids.length) return res.status(400).json({ error: '请选择商品' });

  const list = db.prepare(`SELECT id, orig_name, orig_spec, orig_brand, name, spec, brand FROM goods_raw WHERE id IN (${ids.map(() => '?').join(',')})`).all(...ids);
  const updateStmt = db.prepare('UPDATE goods_raw SET name=?, spec=?, brand=?, orig_brand=COALESCE(NULLIF(orig_brand, \'\'), ?) WHERE id=?');

  const summary = {
    total: list.length,
    changedRows: 0,
    changedFields: 0,
    unchangedRows: 0,
    details: [],
  };

  const batch = db.transaction(() => {
    list.forEach(item => {
      const cleanedName = cleanGoodsField(item.orig_name || item.name, 'name');
      const cleanedSpec = cleanGoodsField(item.orig_spec || item.spec, 'spec');
      const cleanedBrand = cleanGoodsField(item.orig_brand || item.brand, 'brand');
      const mergedName = [cleanedName, cleanedSpec, cleanedBrand].filter(Boolean).join(' ');

      const changes = [];
      if (mergedName !== (item.name || '')) changes.push('名称合并');
      if (cleanedSpec !== (item.spec || '')) changes.push('规格');
      if (cleanedBrand !== (item.brand || '')) changes.push('品牌');

      updateStmt.run(mergedName, cleanedSpec, cleanedBrand, String(item.brand || '').trim(), item.id);

      if (changes.length) {
        summary.changedRows++;
        summary.changedFields += changes.length;
        summary.details.push({ id: item.id, changes, cleanedName, cleanedSpec, cleanedBrand });
      } else {
        summary.unchangedRows++;
      }
    });
  });

  try {
    batch();
    res.json(summary);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});
crudRoutes(app, 'goods-std', 'goods_std', ['code','name','category','category_code','attrs','attrs_vector','raw_count','mat_code','mat_candidates'], ['code','name','category','mat_code'], {
  postProcessList: (rows) => {
    const matStmt = db.prepare('SELECT name FROM mat_archive WHERE code=?');
    return rows.map((row) => {
      const mat = row.mat_code ? matStmt.get(row.mat_code) : null;
      return { ...row, mat_name: mat?.name || '' };
    });
  }
});
crudRoutes(app, 'mapping', 'mapping', ['goods_code','goods_name','mat_code','mat_name','type','method'], ['goods_code','goods_name','mat_code','mat_name']);

app.get('/api/goods-std/:id/raw-items', auth, (req, res) => {
  const std = db.prepare('SELECT id, code, name FROM goods_std WHERE id=?').get(req.params.id);
  if (!std) return res.status(404).json({ error: '标准商品不存在' });
  const list = db.prepare(`
    SELECT id, orig_code, orig_name, orig_spec, orig_brand, category_name, status
    FROM goods_raw
    WHERE std_code=?
    ORDER BY created_at DESC
  `).all(std.code);
  res.json({ std, list });
});

app.get('/api/mapping-std', auth, (req, res) => {
  const { q = '', page = 1, limit = 100 } = req.query;
  const safeLimit = Math.min(Math.max(parseInt(limit, 10) || 100, 1), 100);
  const safePage = Math.max(parseInt(page, 10) || 1, 1);
  const offset = (safePage - 1) * safeLimit;
  const kw = `%${String(q || '').trim()}%`;

  const where = String(q || '').trim()
    ? `WHERE (
      g.code LIKE ? OR g.name LIKE ? OR COALESCE(g.category, '') LIKE ? OR
      COALESCE(g.mat_code, '') LIKE ? OR COALESCE(a.name, '') LIKE ? OR
      COALESCE(last_map.mat_name, '') LIKE ? OR COALESCE(g.mat_candidates, '') LIKE ?
    )`
    : '';
  const params = where ? [kw, kw, kw, kw, kw, kw, kw] : [];

  const total = db.prepare(`
    SELECT COUNT(*) AS c
    FROM goods_std g
    LEFT JOIN mat_archive a ON a.code = g.mat_code
    LEFT JOIN (
      SELECT m1.goods_code, m1.mat_code, m1.mat_name, m1.type, m1.method, m1.created_at
      FROM mapping m1
      JOIN (
        SELECT goods_code, MAX(created_at) AS max_created
        FROM mapping
        GROUP BY goods_code
      ) latest ON latest.goods_code = m1.goods_code AND latest.max_created = m1.created_at
    ) last_map ON last_map.goods_code = g.code
    ${where}
  `).get(...params).c;

  const data = db.prepare(`
    SELECT
      g.id,
      g.code AS goods_code,
      g.name AS goods_name,
      g.category,
      g.raw_count,
      g.mat_code,
      g.mat_candidates,
      COALESCE(a.name, last_map.mat_name, '') AS mat_name,
      COALESCE(last_map.type, CASE WHEN g.mat_code IS NOT NULL AND TRIM(g.mat_code) <> '' THEN '1:1' ELSE '' END) AS type,
      COALESCE(last_map.method, '') AS method,
      last_map.created_at
    FROM goods_std g
    LEFT JOIN mat_archive a ON a.code = g.mat_code
    LEFT JOIN (
      SELECT m1.goods_code, m1.mat_code, m1.mat_name, m1.type, m1.method, m1.created_at
      FROM mapping m1
      JOIN (
        SELECT goods_code, MAX(created_at) AS max_created
        FROM mapping
        GROUP BY goods_code
      ) latest ON latest.goods_code = m1.goods_code AND latest.max_created = m1.created_at
    ) last_map ON last_map.goods_code = g.code
    ${where}
    ORDER BY g.created_at DESC
    LIMIT ? OFFSET ?
  `).all(...params, safeLimit, offset);

  res.json({ data, total, page: safePage, limit: safeLimit });
});

// ── Special: Code generators ──────────────────────
app.get('/api/gen-mat-code', auth, (req, res) => res.json({ code: genMatCode() }));
app.get('/api/gen-goods-code', auth, (req, res) => res.json({ code: genGoodsCode() }));

// ── Special: Bulk save gov results ───────────────
app.post('/api/mat-gov/save-results', auth, (req, res) => {
  const { archive, rawMats } = req.body;
  const insertArch = db.prepare('INSERT OR REPLACE INTO mat_archive (id,code,name,synonyms,category,vector_text,raw_count,created_at) VALUES (?,?,?,?,?,?,?,?)');
  const insertRaw = db.prepare('INSERT OR REPLACE INTO mat_raw (id,code,name,spec,new_code,new_name,status,source,created_at) VALUES (?,?,?,?,?,?,?,?,?)');
  const updateArch = db.prepare('UPDATE mat_archive SET synonyms=?, category=?, vector_text=? WHERE id=?');
  const findArchByName = db.prepare('SELECT id, code, name FROM mat_archive WHERE name=?');
  const recalcRawCount = db.prepare(`
    UPDATE mat_archive
    SET raw_count = (
      SELECT COUNT(*)
      FROM mat_raw
      WHERE mat_raw.new_code = mat_archive.code
    )
  `);

  const saveAll = db.transaction(() => {
    const canonicalCodeByName = new Map();
    const canonicalNameByName = new Map();

    archive.forEach(a => {
      const archiveName = sanitizeImportedCell(a.name);
      if (!archiveName) return;
      const existing = findArchByName.get(archiveName);
      const vectorText = vectorToText(buildMatVectorSource(a));
      if (existing) {
        updateArch.run(a.synonyms || '', a.category || '', vectorText, existing.id);
        canonicalCodeByName.set(archiveName, existing.code);
        canonicalNameByName.set(archiveName, existing.name);
      } else {
        const nextCode = a.code || genMatCode();
        insertArch.run(a.id || genId(), nextCode, archiveName, a.synonyms || '', a.category || '', vectorText, a.raw_count || 0, a.created_at || Date.now());
        canonicalCodeByName.set(archiveName, nextCode);
        canonicalNameByName.set(archiveName, archiveName);
      }
    });

    rawMats.forEach(r => {
      const normalizedNewName = sanitizeImportedCell(r.new_name || '');
      let normalizedNewCode = sanitizeImportedCell(r.new_code || '');

      if (normalizedNewName) {
        if (!canonicalCodeByName.has(normalizedNewName)) {
          const existing = findArchByName.get(normalizedNewName);
          if (existing) {
            canonicalCodeByName.set(normalizedNewName, existing.code);
            canonicalNameByName.set(normalizedNewName, existing.name);
          } else {
            const nextCode = genMatCode();
            const vectorText = vectorToText(buildMatVectorSource({ name: normalizedNewName, synonyms: '', category: '' }));
            insertArch.run(genId(), nextCode, normalizedNewName, '', '', vectorText, 0, Date.now());
            canonicalCodeByName.set(normalizedNewName, nextCode);
            canonicalNameByName.set(normalizedNewName, normalizedNewName);
          }
        }

        normalizedNewCode = canonicalCodeByName.get(normalizedNewName) || normalizedNewCode;
      }

      insertRaw.run(
        r.id || genId(),
        r.code,
        r.name,
        r.spec || '',
        normalizedNewCode || '',
        normalizedNewName || '',
        r.status || '已治理',
        r.source || 'Excel导入',
        r.created_at || Date.now()
      );
    });

    recalcRawCount.run();
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
  const left = normalizeText(s1);
  const right = normalizeText(s2);
  if (!left || !right) return 0;
  const getBigrams = s => {
    const b = new Set();
    if (s.length < 2) {
      b.add(s);
      return b;
    }
    for (let i = 0; i < s.length - 1; i++) b.add(s.substring(i, i + 2));
    return b;
  };
  const b1 = getBigrams(left), b2 = getBigrams(right);
  let intersect = 0;
  for (const x of b1) if (b2.has(x)) intersect++;
  return (2.0 * intersect) / (b1.size + b2.size) || 0;
};

const compactNormalized = (value) => normalizeText(value).replace(/\s+/g, '');

const getElectricalPanelSemanticBoost = (rawName, extractedCore, category) => {
  const rawText = `${rawName || ''} ${extractedCore || ''}`;
  const compactText = compactNormalized(rawText);
  const compactCategory = compactNormalized(`${category.l3 || ''}${category.l4 || ''}`);
  if (!compactText || !compactCategory) return 0;

  const socketHints = [
    '插座', '插孔', '五孔', '四孔', '三孔', '二孔', '七孔', '十孔',
    '16a插座', '地插', '排插', '插排', '86型'
  ];
  const switchHints = [
    '开关', '一开', '二开', '三开', '四开', '单开', '双开',
    '三开双控', '双控', '单控', '按钮开关'
  ];

  const hasSocketHint = socketHints.some(token => compactText.includes(compactNormalized(token)));
  const hasSwitchHint = switchHints.some(token => compactText.includes(compactNormalized(token)));
  const isSocketCategory = compactCategory.includes('插座');
  const isSwitchCategory = compactCategory.includes('开关') && !compactCategory.includes('插座');

  let score = 0;

  if (hasSocketHint && isSocketCategory) score += 0.26;
  if (hasSocketHint && isSwitchCategory) score -= 0.18;

  if (hasSwitchHint && isSwitchCategory) score += 0.24;
  if (hasSwitchHint && isSocketCategory && !hasSocketHint) score -= 0.12;

  // “开关插座”是混合描述，需要靠后续结构词判断主类；
  // 出现“五孔/三孔”等插座形态词时，应更偏向插座而不是开关。
  if (compactText.includes('开关插座')) {
    if ((compactText.includes('五孔') || compactText.includes('三孔') || compactText.includes('二孔') || compactText.includes('插孔')) && isSocketCategory) {
      score += 0.18;
    }
    if ((compactText.includes('五孔') || compactText.includes('三孔') || compactText.includes('二孔') || compactText.includes('插孔')) && isSwitchCategory) {
      score -= 0.12;
    }
  }

  return score;
};

const scoreCategoryCandidate = (item, queryText, extractedCore, category) => {
  const rawName = item.orig_name || item.name || '';
  const sourceText = String(queryText || '').trim();
  const queryVector = vectorToText(sourceText);
  const vectorScore = cosineFromVectors(queryVector, category.vector_text || vectorToText(buildCategoryVectorSource(category)));
  const l4 = String(category.l4 || '');
  const l34 = `${category.l3 || ''}${category.l4 || ''}`;
  const lexicalDice = Math.max(
    getDice(sourceText, l4),
    getDice(sourceText, l34),
    extractedCore ? getDice(extractedCore, l4) : 0
  );

  const compactName = compactNormalized(sourceText);
  const compactL4 = compactNormalized(l4);
  const compactCore = compactNormalized(extractedCore);
  let containBoost = 0;
  if (compactName && compactL4 && compactName.includes(compactL4)) containBoost += 0.25;
  if (compactCore && compactL4 && compactL4.includes(compactCore)) containBoost += 0.12;
  if (compactName && compactCore && compactName.includes(compactCore)) containBoost += 0.05;

  // “开关插座”等强词共现给一点额外增益，减少明显可匹配词被漏召回。
  if (compactName.includes('开关插座') && compactL4.includes('开关插座')) containBoost += 0.2;

  const semanticBoost = getElectricalPanelSemanticBoost(rawName, extractedCore, category);

  const mixed = Math.max(0, Math.min(1, vectorScore * 0.65 + lexicalDice * 0.35 + containBoost + semanticBoost));
  return Number(mixed.toFixed(6));
};

app.get('/api/mat-archive/:id/similar', auth, (req, res) => {
  const threshold = parseFloat(getSetting('mat_similarity_threshold')) || 0.8;
  const target = db.prepare('SELECT id, name FROM mat_archive WHERE id=?').get(req.params.id);
  if (!target) return res.status(404).json({ error: '档案不存在' });
  const all = db.prepare('SELECT id, code, name FROM mat_archive WHERE id != ?').all(req.params.id);
  const similar = all.map(a => ({ ...a, score: getDice(target.name, a.name) })).filter(a => a.score >= threshold).sort((a, b) => b.score - a.score);
  res.json(similar);
});

app.get('/api/mat-archive/:id/raw-items', auth, (req, res) => {
  const archive = db.prepare('SELECT id, code, name FROM mat_archive WHERE id=?').get(req.params.id);
  if (!archive) return res.status(404).json({ error: '物料档案不存在' });
  const list = db.prepare(`
    SELECT id, code, name, spec, status, source, created_at
    FROM mat_raw
    WHERE new_code=?
    ORDER BY created_at DESC
  `).all(archive.code);
  res.json({ archive, list });
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
    const mergedSynonyms = [...synonyms].join(',');
    const nextVector = vectorToText(buildMatVectorSource({ name: target.name, synonyms: mergedSynonyms, category: target.category }));
    db.prepare('UPDATE mat_archive SET synonyms=?, vector_text=?, raw_count = raw_count + ? WHERE id=?').run(mergedSynonyms, nextVector, extraCount, targetId);
  });

  try {
    mergeBatch();
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

const runMatchCategory = async (ids, options = {}) => {
  if (!ids || !ids.length) return [];
  ensureCategoryVectors();
  const cats = db.prepare("SELECT code, l1, l2, l3, l4, key_attrs, vector_text FROM categories WHERE l4 IS NOT NULL").all();
  const rawGoods = db.prepare(`SELECT id, name, orig_name, core_word, category_code FROM goods_raw WHERE id IN (${ids.map(() => '?').join(',')})`).all(...ids);
  const coreWordPromptSetting = getSetting('goods_core_word_prompt') || '提取商品的关键品名，去掉品牌、规格、型号、包装、单位等修饰属性';

  const results = [];
  for (const item of rawGoods) {
    if (!options.force && item.category_code) {
      results.push({ id: item.id, status: 'skip', reason: '已有分类' });
      continue;
    }

    let extractedCore = '';
    try {
      const corePrompt = `${coreWordPromptSetting}\n要求：只返回“核心词”纯文本，不要解释，不要JSON。\n商品名称：${item.orig_name || item.name}`;
      const coreResp = await requestAIChat({ systemPrompt: '你是商品关键核心词提取助手。', userPrompt: corePrompt });
      extractedCore = stripCodeFence(coreResp.content).replace(/["'`]/g, '').trim();
    } catch (e) {
      extractedCore = item.core_word || '';
    }
    const queryText = buildCoreWordSynonymText(extractedCore || item.core_word || '');
    const top5Recall = cats
      .map(c => ({
        ...c,
        score: scoreCategoryCandidate(item, queryText, extractedCore, c),
      }))
      .sort((a, b) => b.score - a.score)
      .slice(0, 5);
    const top5 = top5Recall.filter(c => c.score >= 0.05);

    const hasSimilar = top5.length > 0;
    const candidatesPayload = top5.map(c => ({
      code: c.code,
      name: `${c.l1}>${c.l2}>${c.l3}>${c.l4}`,
      score: Number((c.score || 0).toFixed(4)),
    }));
    const filteredCandidatesPayload = filterCategoryCandidates(candidatesPayload, item.category_code);
    const prompt = `请从候选分类中选择最匹配“${item.orig_name || item.name}”的四级分类编码，仅返回编码；如果都不匹配返回“未匹配”。\n候选项：\n${candidatesPayload.map(c => `${c.code}|${c.name}|相似度:${c.score}`).join('\n')}`;

    try {
      const resp = hasSimilar
        ? await requestAIChat({ systemPrompt: '你是一个专业的商品分类专家。必须只返回候选编码。', userPrompt: prompt })
        : { content: '未匹配' };
      const content = stripCodeFence(resp.content).trim();
      const matched = top5.find(c => content === c.code || content.startsWith(c.code));

      if (!matched) {
        const statusText = filteredCandidatesPayload.length ? '疑似分类待确认' : '未匹配到分类';
        db.prepare('UPDATE goods_raw SET category_code=?, category_name=?, category_candidates=?, match_status=? WHERE id=?')
          .run(null, null, filteredCandidatesPayload.length ? JSON.stringify(filteredCandidatesPayload) : null, statusText, item.id);
        results.push({ id: item.id, status: filteredCandidatesPayload.length ? 'suspect' : 'fail', candidates: filteredCandidatesPayload });
      } else {
        const name = `${matched.l1}>${matched.l2}>${matched.l3}>${matched.l4}`;
        db.prepare('UPDATE goods_raw SET category_code=?, category_name=?, category_candidates=?, match_status=? WHERE id=?')
          .run(matched.code, name, null, '分类已匹配', item.id);
        results.push({ id: item.id, status: 'success', category: name, score: matched.score });
      }
    } catch (e) {
      results.push({ id: item.id, status: 'error', error: e.message });
    }
  }
  return results;
};

const runExtractFeatures = async (ids) => {
  if (!ids || !ids.length) return [];
  const list = db.prepare(`SELECT id, name, orig_name, spec, orig_spec, orig_brand, category_code, category_name, core_word, brand FROM goods_raw WHERE id IN (${ids.map(() => '?').join(',')})`).all(...ids);
  const coreWordPromptSetting = getSetting('goods_core_word_prompt') || '提取商品的关键品名，去掉品牌、规格、型号、包装、单位等修饰属性';

  const results = [];
  for (const item of list) {
    if (!item.category_code) { results.push({ id: item.id, status: 'skip', reason: '无分类' }); continue; }
    const cat = db.prepare('SELECT key_attrs, l4 FROM categories WHERE code = ?').get(item.category_code);
    const keyAttrs = cat?.key_attrs || '';
    const keyAttrList = keyAttrs.split(',').map(s => s.trim()).filter(Boolean);

    const systemPrompt = `你是一个商品属性提取专家。你只能依据“商品名称”提取信息，不能使用规格、经验推测或补全。
核心词提取规则请严格遵循：${coreWordPromptSetting}
请把核心词拆分为：品类核心词（表示商品属于什么品类）+功能核心词（表示用途/功能）。
品牌请尽量同时提取中文和英文（如名称里有中英文品牌都要提取）。
返回JSON：{"brand":"品牌","brand_zh":"中文品牌","brand_en":"英文品牌","category_core_word":"品类核心词","function_core_word":"功能核心词","model":"型号","attrs":{"属性名":"值"}}。
若无法从商品名称明确提取，请填"unknown"。`;
    const userPrompt = `分类：${item.category_name}\n分类关键属性：${keyAttrs || '无'}\n商品名称：${item.orig_name || item.name}`;

    try {
      const resp = await requestAIChat({ systemPrompt, userPrompt });
      const rawData = JSON.parse(stripCodeFence(resp.content));
      const attrs = rawData.attrs && typeof rawData.attrs === 'object' && !Array.isArray(rawData.attrs) ? rawData.attrs : {};
      const brandZh = sanitizeImportedCell(rawData.brand_zh || rawData.brandZh || '');
      const brandEn = sanitizeImportedCell(rawData.brand_en || rawData.brandEn || '');
      const mergedBrandText = [
        rawData.brand,
        brandZh,
        brandEn,
        item.brand,
        item.orig_brand,
      ].filter(Boolean).join(' | ');
      let normalizedBrand = normalizeBrandFormat(mergedBrandText || '');
      if (!/[A-Za-z]/.test(normalizedBrand || '')) {
        const enHint = extractEnglishBrandHintFromName(item.orig_name || item.name);
        if (enHint) normalizedBrand = normalizeBrandFormat(`${normalizedBrand}|${enHint}`);
      }
      const categoryCoreWord = sanitizeImportedCell(rawData.category_core_word || rawData.category_word || '');
      const functionCoreWord = sanitizeImportedCell(rawData.function_core_word || rawData.function_word || '');
      const fallbackCoreWord = sanitizeImportedCell(rawData.core_word || item.core_word || '');
      const combinedCoreWord = mergeCoreWords(categoryCoreWord, functionCoreWord, fallbackCoreWord);
      keyAttrList.forEach((key) => {
        if (attrs[key] == null || String(attrs[key]).trim() === '') attrs[key] = 'unknown';
      });
      const data = {
        brand: normalizedBrand,
        core_word: combinedCoreWord,
        model: rawData.model || 'unknown',
        attrs,
      };

      if (data.brand) {
        ensureBrandExists(data.brand);
      }

      const stdName = buildStdName({
        brand: data.brand,
        coreWord: data.core_word,
        model: data.model,
        pack: extractPackagingSpec(item.orig_spec, item.spec, item.orig_name, item.name),
        attrs: data.attrs,
        keyAttrs,
      });

      const missing = [];
      if (!data.brand) missing.push('品牌');
      if (data.core_word === 'unknown') missing.push('核心词');
      if (data.model === 'unknown') missing.push('型号');
      Object.entries(data.attrs).forEach(([k, v]) => { if (v === 'unknown') missing.push(k); });
      const attrPairs = JSON.stringify(data.attrs);
      const attrsVector = vectorToText(attrsObjectToText(attrPairs));
      const isComplete = missing.length === 0;

      db.prepare(`UPDATE goods_raw SET brand=?, core_word=?, model=?, attrs_json=?, attr_pairs=?, attrs_vector=?, missing_attrs=?, std_name_proposed=?, match_status=?, status=? WHERE id=?`)
        .run(
          data.brand,
          data.core_word,
          data.model,
          attrPairs,
          attrPairs,
          attrsVector,
          missing.join(','),
          stdName,
          isComplete ? '属性完整' : '属性缺失',
          isComplete ? '待归一' : '待补充属性',
          item.id
        );

      results.push({ id: item.id, status: 'success', stdName, missing });
    } catch (e) {
      results.push({ id: item.id, status: 'error', error: e.message });
    }
  }
  return results;
};

const runMatchStandard = async (ids) => {
  if (!ids || !ids.length) return [];
  const list = db.prepare(`SELECT * FROM goods_raw WHERE id IN (${ids.map(() => '?').join(',')})`).all(...ids);
  const readPeerRelationStmt = db.prepare('SELECT id, orig_code, orig_name, name, std_code, same_items, similar_items FROM goods_raw WHERE id=?');
  const updatePeerRelationStmt = db.prepare('UPDATE goods_raw SET same_items=?, similar_items=?, same_count=?, similar_count=? WHERE id=?');

  const results = [];
  for (const item of list) {
    if (item.status === '已归一') {
      results.push({ id: item.id, status: 'skip', reason: '已归一' });
      continue;
    }
    if (!item.category_code) {
      results.push({ id: item.id, status: 'skip', reason: '缺少分类' });
      continue;
    }
    if (item.missing_attrs && String(item.missing_attrs).trim()) {
      results.push({ id: item.id, status: 'skip', reason: '属性缺失，待人工补充' });
      continue;
    }

    const sourceVector = item.attrs_vector || vectorToText(attrsObjectToText(item.attr_pairs || item.attrs_json || item.spec));
    const candidates = db.prepare(`
      SELECT id, orig_code, orig_name, name, std_code, std_name, attrs_vector, attr_pairs, attrs_json
      FROM goods_raw
      WHERE category_code = ?
        AND id != ?
        AND attrs_vector IS NOT NULL
        AND TRIM(attrs_vector) <> ''
    `).all(item.category_code, item.id);

    const top20 = candidates
      .map(c => ({
        ...c,
        score: cosineFromVectors(sourceVector, c.attrs_vector),
      }))
      .filter(c => c.score > 0)
      .sort((a, b) => b.score - a.score)
      .slice(0, 20);

    if (!top20.length) {
      db.prepare(`
        UPDATE goods_raw
        SET same_items=?, similar_items=?, same_count=?, similar_count=?, match_status=?, status=?
        WHERE id=?
      `).run('[]', '[]', '0', '0', '同款识别完成', '待生成标准商品', item.id);
      results.push({ id: item.id, status: 'success', sameCount: 0, similarCount: 0 });
      continue;
    }

    try {
      const prompt = `请从候选列表中识别与目标商品“${item.orig_name || item.name}”同款的商品ID，必须只返回JSON数组，如["id1","id2"]；没有同款返回[]。\n目标商品属性对：${item.attr_pairs || item.attrs_json || '{}'}\n候选Top20：\n${top20.map(c => `${c.id}|${c.orig_code || ''}|${c.orig_name || c.name}|score:${c.score}|attrs:${c.attr_pairs || c.attrs_json || '{}'}`).join('\n')}`;
      const resp = await requestAIChat({ systemPrompt: '你是商品同款识别专家，只允许从候选ID中选择。', userPrompt: prompt });
      const sameIds = extractCandidateIds(resp.content, top20);
      const sameSet = new Set(sameIds);

      const sameItems = top20.filter(c => sameSet.has(c.id)).map(c => ({
        id: c.id,
        orig_code: c.orig_code || '',
        name: c.orig_name || c.name || '',
        score: Number((c.score || 0).toFixed(4)),
        std_code: c.std_code || '',
      }));
      const similarItems = top20.filter(c => !sameSet.has(c.id)).map(c => ({
        id: c.id,
        orig_code: c.orig_code || '',
        name: c.orig_name || c.name || '',
        score: Number((c.score || 0).toFixed(4)),
        std_code: c.std_code || '',
      }));

      const syncBidirectional = db.transaction(() => {
        db.prepare(`
          UPDATE goods_raw
          SET same_items=?, similar_items=?, same_count=?, similar_count=?, match_status=?, status=?
          WHERE id=?
        `).run(
          JSON.stringify(sameItems),
          JSON.stringify(similarItems),
          String(sameItems.length),
          String(similarItems.length),
          '同款识别完成',
          '待生成标准商品',
          item.id
        );

        // 双向刷新历史关系：当前识别结果需要同步到候选商品上，避免“只更新新商品，不更新历史商品”。
        top20.forEach((peer) => {
          const peerRow = readPeerRelationStmt.get(peer.id);
          if (!peerRow) return;

          let peerSame = [];
          let peerSimilar = [];
          try { peerSame = JSON.parse(peerRow.same_items || '[]'); } catch (e) { peerSame = []; }
          try { peerSimilar = JSON.parse(peerRow.similar_items || '[]'); } catch (e) { peerSimilar = []; }

          const compact = (arr) => arr.filter(x => x && x.id && x.id !== item.id);
          peerSame = compact(peerSame);
          peerSimilar = compact(peerSimilar);

          const relationItem = {
            id: item.id,
            orig_code: item.orig_code || '',
            name: item.orig_name || item.name || '',
            score: Number((peer.score || 0).toFixed(4)),
            std_code: item.std_code || '',
          };

          if (sameSet.has(peer.id)) peerSame.push(relationItem);
          else peerSimilar.push(relationItem);

          peerSame.sort((a, b) => Number(b.score || 0) - Number(a.score || 0));
          peerSimilar.sort((a, b) => Number(b.score || 0) - Number(a.score || 0));

          updatePeerRelationStmt.run(
            JSON.stringify(peerSame),
            JSON.stringify(peerSimilar),
            String(peerSame.length),
            String(peerSimilar.length),
            peer.id
          );
        });
      });
      syncBidirectional();

      results.push({ id: item.id, status: 'success', sameCount: sameItems.length, similarCount: similarItems.length });
    } catch (e) {
      results.push({ id: item.id, status: 'error', error: e.message });
    }
  }
  return results;
};

const runGenerateStandards = (ids) => {
  if (!ids || !ids.length) return [];
  const selectedRaw = db.prepare(`SELECT * FROM goods_raw WHERE id IN (${ids.map(() => '?').join(',')})`).all(...ids);
  if (!selectedRaw.length) return [];
  const selected = selectedRaw.filter(item => (
    item.status !== '已归一'
    && item.category_code
    && !(item.missing_attrs && String(item.missing_attrs).trim())
  ));
  const results = selectedRaw
    .filter(item => !selected.find(s => s.id === item.id))
    .map(item => ({ id: item.id, action: 'skip', reason: item.status === '已归一' ? '已归一' : '分类或属性不完整' }));
  if (!selected.length) return results;

  const parent = new Map();
  const find = (x) => {
    if (!parent.has(x)) parent.set(x, x);
    if (parent.get(x) !== x) parent.set(x, find(parent.get(x)));
    return parent.get(x);
  };
  const union = (a, b) => {
    const ra = find(a);
    const rb = find(b);
    if (ra !== rb) parent.set(rb, ra);
  };

  const involvedIds = new Set(selected.map(item => item.id));
  selected.forEach((item) => {
    let sameItems = [];
    try { sameItems = JSON.parse(item.same_items || '[]'); } catch (e) { sameItems = []; }
    sameItems.forEach((peer) => {
      if (!peer?.id) return;
      involvedIds.add(peer.id);
      union(item.id, peer.id);
    });
  });

  const allRows = db.prepare(`SELECT * FROM goods_raw WHERE id IN (${[...involvedIds].map(() => '?').join(',')})`).all(...[...involvedIds]);
  const rowMap = new Map(allRows.map(row => [row.id, row]));
  allRows.forEach((row) => { find(row.id); });

  const groupedByRoot = new Map();
  selected.forEach((item) => {
    const root = find(item.id);
    if (!groupedByRoot.has(root)) groupedByRoot.set(root, new Set());
    groupedByRoot.get(root).add(item.id);
  });

  const linkStmt = db.prepare(`
    UPDATE goods_raw
    SET std_code=?, std_name=?, status='已归一', match_status='已完成',
        review_candidates=NULL, review_reason=NULL, review_score=NULL
    WHERE id=?
  `);

  const tx = db.transaction(() => {
    groupedByRoot.forEach((selectedIds, root) => {
      const groupAll = allRows.filter(row => find(row.id) === root);
      const existing = groupAll.find(row => row.std_code);
      if (existing?.std_code) {
        const std = db.prepare('SELECT code, name FROM goods_std WHERE code=?').get(existing.std_code);
        if (!std) throw new Error(`标准商品不存在: ${existing.std_code}`);
        groupAll.forEach((row) => linkStmt.run(std.code, std.name, row.id));
        results.push({ root, action: 'reuse', stdCode: std.code, groupSize: groupAll.length });
        return;
      }

      const representative = groupAll.find(row => row.category_code) || groupAll[0];
      if (!representative?.category_code) {
        results.push({ root, action: 'skip', reason: '分组缺少分类编码' });
        return;
      }

      const created = createStdFromRaw(representative);
      groupAll.forEach((row) => linkStmt.run(created.stdCode, created.stdName, row.id));
      results.push({ root, action: 'create', stdCode: created.stdCode, groupSize: groupAll.length });
    });

    db.prepare('UPDATE goods_std SET raw_count = (SELECT COUNT(*) FROM goods_raw WHERE std_code = goods_std.code)').run();
  });

  tx();
  return results;
};

// Step 3: Match Category
app.post('/api/goods-unify/match-category', auth, async (req, res) => {
  const { ids } = req.body;
  if (!ids || !ids.length) return res.status(400).json({ error: '请选择商品' });
  res.json(await runMatchCategory(ids));
});

// Step 4: Extract Features
app.post('/api/goods-unify/extract-features', auth, async (req, res) => {
  const { ids } = req.body;
  res.json(await runExtractFeatures(ids));
});

// Step 5: Match Standard
app.post('/api/goods-unify/match-standard', auth, async (req, res) => {
  const { ids } = req.body;
  res.json(await runMatchStandard(ids));
});

app.post('/api/goods-unify/generate-standard', auth, (req, res) => {
  const { ids } = req.body;
  if (!ids || !ids.length) return res.status(400).json({ error: '请选择商品' });
  try {
    const results = runGenerateStandards(ids);
    const normalizedRows = db.prepare(`
      SELECT COUNT(*) as c
      FROM goods_raw
      WHERE id IN (${ids.map(() => '?').join(',')})
        AND status='已归一'
    `).get(...ids).c;
    res.json({
      ok: true,
      results,
      summary: {
        created: results.filter(r => r.action === 'create').length,
        reused: results.filter(r => r.action === 'reuse').length,
        skipped: results.filter(r => r.action === 'skip').length,
        normalizedRows,
        normalizeFailedRows: ids.length - normalizedRows,
      }
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/goods-unify/process-selected', auth, async (req, res) => {
  const { ids } = req.body;
  if (!ids || !ids.length) return res.status(400).json({ error: '请选择商品' });

  const selected = db.prepare(`
    SELECT id, category_code, status, match_status, missing_attrs, attr_pairs, attrs_json, brand, core_word, model
    FROM goods_raw
    WHERE id IN (${ids.map(() => '?').join(',')})
  `).all(...ids);
  const unifiedItems = selected.filter(item => item.status === '已归一');
  if (unifiedItems.length) {
    return res.status(400).json({ error: `已归一商品不可再次发起归一（${unifiedItems.length}条）` });
  }
  const categoryIds = selected.filter(item => !item.category_code).map(item => item.id);

  const categoryResults = await runMatchCategory(categoryIds);
  const selectedAfterCategory = db.prepare(`
    SELECT id, category_code, status, match_status, missing_attrs, attr_pairs, attrs_json, brand, core_word, model
    FROM goods_raw
    WHERE id IN (${ids.map(() => '?').join(',')})
  `).all(...ids);
  const featureIds = selectedAfterCategory
    .filter((item) => {
      if (!item.category_code || String(item.category_code).trim() === '') return false;
      const hasManualOrExtractedAttrs = !!String(item.attr_pairs || item.attrs_json || '').trim();
      const hasCoreFields = [item.brand, item.core_word, item.model].some(v => {
        const text = String(v || '').trim();
        return text && text !== 'unknown';
      });
      const hasMissing = !!String(item.missing_attrs || '').trim();
      const matchStatus = String(item.match_status || '').trim();
      const shouldExtractBecauseUnprocessed = !hasManualOrExtractedAttrs || !hasCoreFields;
      const shouldExtractBecausePending = ['分类已匹配', '属性缺失'].includes(matchStatus) || item.status === '待补充属性';
      return hasMissing || shouldExtractBecauseUnprocessed || shouldExtractBecausePending;
    })
    .map(item => item.id);
  const standardIds = selectedAfterCategory.filter(item => item.status !== '已归一').map(item => item.id);
  const featureResults = await runExtractFeatures(featureIds);
  const standardResults = await runMatchStandard(standardIds);
  const generateResults = runGenerateStandards(standardIds);

  res.json({
    ok: true,
    categoryResults,
    featureResults,
    standardResults,
    generateResults,
    summary: {
      categoryMatched: categoryResults.filter(item => item.status === 'success').length,
      featureExtracted: featureResults.filter(item => item.status === 'success').length,
      goodsUnified: generateResults.filter(item => item.action === 'create' || item.action === 'reuse').length,
      sameDetected: standardResults.filter(item => item.status === 'success').length,
      stdCreated: generateResults.filter(item => item.action === 'create').length,
      stdReused: generateResults.filter(item => item.action === 'reuse').length,
    },
  });
});

app.get('/api/goods-unify/item-relations/:id', auth, (req, res) => {
  const { type = 'same' } = req.query;
  const row = db.prepare('SELECT id, same_items, similar_items FROM goods_raw WHERE id=?').get(req.params.id);
  if (!row) return res.status(404).json({ error: '商品不存在' });
  try {
    const payload = type === 'similar' ? row.similar_items : row.same_items;
    const list = JSON.parse(payload || '[]');
    res.json(list);
  } catch (e) {
    res.json([]);
  }
});

app.post('/api/goods-unify/update-attrs', auth, (req, res) => {
  const { id, brand, core_word, model, attrs } = req.body;
  if (!id) return res.status(400).json({ error: '缺少商品ID' });
  const item = db.prepare('SELECT id, category_code FROM goods_raw WHERE id=?').get(id);
  if (!item) return res.status(404).json({ error: '商品不存在' });
  if (!item.category_code) return res.status(400).json({ error: '请先关联分类' });

  const cat = db.prepare('SELECT key_attrs FROM categories WHERE code=?').get(item.category_code);
  const keyAttrs = (cat?.key_attrs || '').split(',').map(s => s.trim()).filter(Boolean);
  const attrObj = attrs && typeof attrs === 'object' && !Array.isArray(attrs) ? attrs : {};
  keyAttrs.forEach((key) => {
    if (!attrObj[key] || String(attrObj[key]).trim() === '') attrObj[key] = 'unknown';
  });

  const normalizedBrand = normalizeBrandFormat(brand || '');
  const normalizedCore = mergeCoreWords('', '', core_word || 'unknown');
  const normalizedModel = sanitizeImportedCell(model || 'unknown') || 'unknown';
  const missing = [];
  if (!normalizedBrand) missing.push('品牌');
  if (normalizedCore === 'unknown') missing.push('核心词');
  if (normalizedModel === 'unknown') missing.push('型号');
  Object.entries(attrObj).forEach(([key, value]) => { if (!value || value === 'unknown') missing.push(key); });

  const stdName = buildStdName({
    brand: normalizedBrand,
    coreWord: normalizedCore,
    model: normalizedModel,
    pack: extractPackagingSpec(item.orig_spec, item.spec, item.orig_name, item.name),
    attrs: attrObj,
    keyAttrs: cat?.key_attrs || '',
  });
  const attrPairs = JSON.stringify(attrObj);
  const attrsVector = vectorToText(attrsObjectToText(attrPairs));
  const isComplete = missing.length === 0;
  db.prepare(`
    UPDATE goods_raw
    SET brand=?, core_word=?, model=?, attrs_json=?, attr_pairs=?, attrs_vector=?, missing_attrs=?, std_name_proposed=?, match_status=?, status=?
    WHERE id=?
  `).run(
    normalizedBrand,
    normalizedCore,
    normalizedModel,
    attrPairs,
    attrPairs,
    attrsVector,
    missing.join(','),
    stdName,
    isComplete ? '属性完整' : '属性缺失',
    isComplete ? '待归一' : '待补充属性',
    id
  );

  if (normalizedBrand) ensureBrandExists(normalizedBrand);
  res.json({ ok: true, missing, std_name_proposed: stdName });
});

app.post('/api/goods-unify/review-standard', auth, (req, res) => {
  const { id, action, stdCode } = req.body;
  if (!id || !action) return res.status(400).json({ error: '缺少参数' });

  const item = db.prepare('SELECT * FROM goods_raw WHERE id=?').get(id);
  if (!item) return res.status(404).json({ error: '原始商品不存在' });
  if (!item.std_name_proposed) return res.status(400).json({ error: '该商品缺少待归一标准名' });

  try {
    let result;
    if (action === 'match_existing') {
      if (!stdCode) return res.status(400).json({ error: '请选择标准商品编码' });
      result = linkRawToExistingStd(item, stdCode);
    } else if (action === 'create_new') {
      result = createStdFromRaw(item);
    } else {
      return res.status(400).json({ error: '不支持的审核动作' });
    }

    res.json({ ok: true, ...result });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Step 6: Match Material
app.post('/api/goods-unify/match-material', auth, async (req, res) => {
  const { ids } = req.body; // std goods ids
  if (!ids || !ids.length) return res.status(400).json({ error: '请选择标准商品' });
  ensureMatVectors();
  const list = db.prepare(`SELECT id, code, name, attrs, attrs_vector, mat_code FROM goods_std WHERE id IN (${ids.map(() => '?').join(',')})`).all(...ids);
  const mats = db.prepare('SELECT code, name, synonyms, category, vector_text FROM mat_archive').all();
  
  const results = [];
  for (const item of list) {
    const sourceText = buildGoodsCoreWordMaterialSource(item.code, item.name, item.attrs);
    const sourceVector = vectorToText(sourceText);
    const top20 = mats
      .map(m => ({
        ...m,
        score: cosineFromVectors(sourceVector, m.vector_text || vectorToText(buildMatVectorSource(m))),
      }))
      .filter(m => m.score > 0)
      .sort((a,b) => b.score - a.score)
      .slice(0, 20);

    const candidateTop = top20.filter(m => m.score >= 0.05).slice(0, 5);
    const candidatePayload = candidateTop.map(m => ({
      code: m.code,
      name: m.name,
      score: Number((m.score || 0).toFixed(4)),
    }));

    const prompt = `请从候选物料中选择与商品“${item.name}”最匹配的物料编码，仅返回编码；如果都不匹配返回“未匹配”。\n商品匹配关键词：${sourceText || item.name}\n候选项：\n${candidatePayload.map(m => `${m.code}|${m.name}|相似度:${m.score}`).join('\n')}`;

    try {
      const resp = candidatePayload.length
        ? await requestAIChat({ systemPrompt: '你是商品物料匹配专家，只能从候选编码中选择。', userPrompt: prompt })
        : { content: '未匹配' };
      const pickedCode = extractCandidateCode(stripCodeFence(resp.content), candidatePayload);
      const matched = candidateTop.find(m => m.code === pickedCode);
      const suspectPayload = filterMatCandidates(candidatePayload, matched?.code || item.mat_code);

      if (matched) {
        db.prepare('UPDATE goods_std SET mat_code=?, mat_candidates=? WHERE id=?')
          .run(matched.code, suspectPayload.length ? JSON.stringify(suspectPayload) : null, item.id);

        db.prepare('INSERT OR REPLACE INTO mapping (id, goods_code, goods_name, mat_code, mat_name, type, method, created_at) VALUES (?,?,?,?,?,?,?,?)')
          .run(genId(), item.code, item.name, matched.code, matched.name, '1:1', 'AI自动匹配', Date.now());

        results.push({
          id: item.id,
          goodsCode: item.code,
          goodsName: item.name,
          status: 'success',
          matCode: matched.code,
          matName: matched.name,
          candidates: suspectPayload,
        });
      } else {
        db.prepare('UPDATE goods_std SET mat_candidates=? WHERE id=?')
          .run(suspectPayload.length ? JSON.stringify(suspectPayload) : null, item.id);
        results.push({ id: item.id, goodsCode: item.code, goodsName: item.name, status: 'none', candidates: suspectPayload });
      }
    } catch (e) {
      const fallbackCandidates = filterMatCandidates(candidatePayload, item.mat_code);
      db.prepare('UPDATE goods_std SET mat_candidates=? WHERE id=?')
        .run(fallbackCandidates.length ? JSON.stringify(fallbackCandidates) : null, item.id);
      results.push({
        id: item.id,
        goodsCode: item.code,
        goodsName: item.name,
        status: fallbackCandidates.length ? 'none' : 'error',
        error: fallbackCandidates.length ? '' : e.message,
        candidates: fallbackCandidates,
      });
    }
  }
  res.json(results);
});

app.post('/api/goods-unify/manual-match-material', auth, (req, res) => {
  const { stdId, matCode } = req.body;
  if (!stdId) return res.status(400).json({ error: '缺少标准商品ID' });
  if (!matCode) return res.status(400).json({ error: '请选择物料编码' });

  const std = db.prepare('SELECT id, code, name FROM goods_std WHERE id=?').get(stdId);
  if (!std) return res.status(404).json({ error: '标准商品不存在' });

  const mat = db.prepare('SELECT code, name FROM mat_archive WHERE code=?').get(matCode);
  if (!mat) return res.status(404).json({ error: '物料不存在' });

  const tx = db.transaction(() => {
    const prev = db.prepare('SELECT mat_candidates FROM goods_std WHERE id=?').get(std.id);
    let nextCandidates = [];
    try { nextCandidates = filterMatCandidates(JSON.parse(prev?.mat_candidates || '[]'), mat.code); } catch (e) { nextCandidates = []; }
    db.prepare('UPDATE goods_std SET mat_code=?, mat_candidates=? WHERE id=?')
      .run(mat.code, nextCandidates.length ? JSON.stringify(nextCandidates) : null, std.id);
    db.prepare('INSERT OR REPLACE INTO mapping (id, goods_code, goods_name, mat_code, mat_name, type, method, created_at) VALUES (?,?,?,?,?,?,?,?)')
      .run(genId(), std.code, std.name, mat.code, mat.name, '1:1', '人工确认', Date.now());
  });

  try {
    tx();
    res.json({ ok: true, stdId: std.id, goodsCode: std.code, matCode: mat.code, matName: mat.name });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── File Upload & Parse ───────────────────────────
app.post('/api/upload/csv', auth, upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: '未收到文件' });
  try {
    const text = req.file.buffer.toString('utf-8');
    const parsedRows = parseUploadedCsv(text);
    const rows = filterMeaningfulCsvRows(parsedRows);
    if (rows.length > 10000) return res.status(400).json({ error: '文件行数超过10000行限制' });
    res.json({ rows, count: rows.length, columns: rows.length > 0 ? Object.keys(rows[0]) : [] });
  } catch (e) { res.status(400).json({ error: 'CSV解析失败: ' + e.message }); }
});

app.post('/api/upload/brand-logo', auth, upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: '未收到文件' });
  if (!String(req.file.mimetype || '').startsWith('image/')) {
    return res.status(400).json({ error: '仅支持图片文件' });
  }

  try {
    const extByMime = {
      'image/jpeg': '.jpg',
      'image/jpg': '.jpg',
      'image/png': '.png',
      'image/webp': '.webp',
      'image/gif': '.gif',
      'image/svg+xml': '.svg',
    };
    const extFromName = path.extname(req.file.originalname || '').toLowerCase();
    const safeExt = /^[.a-z0-9]+$/.test(extFromName) ? extFromName : '';
    const ext = safeExt || extByMime[req.file.mimetype] || '.png';
    const fileName = `brand-${Date.now()}-${Math.random().toString(36).slice(2, 8)}${ext}`;
    const uploadDir = path.join(__dirname, '..', 'public', 'uploads', 'brand-logos');
    fs.mkdirSync(uploadDir, { recursive: true });
    fs.writeFileSync(path.join(uploadDir, fileName), req.file.buffer);
    res.json({ url: `/uploads/brand-logos/${fileName}` });
  } catch (e) {
    res.status(500).json({ error: 'Logo上传失败: ' + e.message });
  }
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
    pendingUnifyIssues: db.prepare(`
      SELECT COUNT(*) as c
      FROM goods_raw
      WHERE status IN ('待补充属性', '待审核')
         OR match_status IN ('未匹配', '待补充属性', '待审核')
    `).get().c,
    pendingMaterialMappingGoods: db.prepare(`
      SELECT COUNT(*) as c
      FROM goods_std
      WHERE mat_code IS NULL OR TRIM(mat_code) = ''
    `).get().c,
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
