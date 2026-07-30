const fs = require('fs');
const src = fs.readFileSync('/home/toge/src/redismm/src/EmbeddedRedis.cpp', 'utf8');

// Strategy: find each "// ---- New ... ----" section delimiter and its following
// function definitions. Insert "#if 0" before the first function in the section
// and "#endif" at the end of the last function.

// Identify the line numbers of all "// ---- New " section delimiters
const lines = src.split('\n');
const newSections = [];

for (let i = 0; i < lines.length; i++) {
  if (lines[i].startsWith('// ---- New ')) {
    newSections.push(i);
  }
}

// For each section, find the functions contained within it
// Insert #if 0 after the section comment and #endif at the end
// We need to be careful about proper brace matching

// Actually, the simplest approach: find the first function after each section comment
// and wrap it with #if 0, then close #endif before the next section or at EOF

// Find "auto EmbeddedRedis::<func>(" patterns that ARE NOT "Pipeline::"
// These are new functions that follow section delimiters

// Let me collect all original function names
const originalFuncs = new Set([
  'set', 'get', 'hset', 'hget', 'hgetall',
  'lpush', 'rpush', 'lpop', 'rpop',
  'sadd', 'smembers',
  'zadd', 'zrangebyscore',
  'xadd',
  'del', 'exists',
  'expire', 'ttl', 'pexpire', 'pttl', 'persist',
  'pipeline'
]);

// Find where the original function definitions end
let lastOriginalEnd = 0;
let foundCount = 0;
for (let i = 0; i < lines.length; i++) {
  const m = lines[i].match(/^auto EmbeddedRedis::(\w+)\(/);
  if (m && !m[1].startsWith('Pipeline') && originalFuncs.has(m[1])) {
    foundCount++;
    // Find the closing brace
    let depth = 0, j = i;
    for (; j < lines.length; j++) {
      for (let c = 0; c < lines[j].length; c++) {
        if (lines[j][c] === '{') depth++;
        if (lines[j][c] === '}') depth--;
      }
      if (depth === 0) break;
    }
    lastOriginalEnd = Math.max(lastOriginalEnd, j + 1);
  }
}

console.log(`Found ${foundCount} original functions, last ends at line ${lastOriginalEnd}`);

// Now create the output: everything up to lastOriginalEnd stays, everything after is #if 0'd
// But we need to keep the function signatures for the linker (they're declared in the header)
// So let's keep the signature but replace the body with a stub

const output = [];
let afterOriginal = false;

for (let i = 0; i < lines.length; i++) {
  if (i === lastOriginalEnd) {
    afterOriginal = true;
    console.log(`Inserting #if 0 at line ${i + 1}`);
  }

  if (afterOriginal) {
    // For new functions: keep the signature, replace body with stub
    const funcMatch = lines[i].match(/^auto EmbeddedRedis::(\w+)\(/);
    if (funcMatch && !lines[i].includes('Pipeline::')) {
      // Keep the signature line(s), find the opening brace
      let braceLine = i;
      let braceCol = lines[i].indexOf('{');
      while (braceCol === -1 && braceLine < lines.length) {
        braceCol = lines[++braceLine].indexOf('{');
      }

      // Output the signature lines
      for (let k = i; k < braceLine; k++) {
        output.push(lines[k]);
      }
      // Output the opening brace
      output.push(lines[braceLine] ? '  {' : '{');
      output.push('');

      // Determine return type and generate stub
      const sigLine = lines[i];
      let stub = '    return {};';
      if (sigLine.includes('-> Result<void>')) stub = '    return {};';
      else if (sigLine.includes('-> Result<bool>')) stub = '    return false;';
      else if (sigLine.includes('-> Result<uint64_t>')) stub = '    return 0;';
      else if (sigLine.includes('-> Result<int64_t>')) stub = '    return 0;';
      else if (sigLine.includes('-> Result<double>')) stub = '    return 0.0;';
      else if (sigLine.includes('-> Result<std::string>')) stub = '    return std::unexpected(ErrorCode::RocksDBError);';
      else if (sigLine.includes('-> Result<std::vector<std::string>>')) stub = '    return std::vector<std::string>{};';
      else if (sigLine.includes('-> Result<std::unordered_map')) stub = '    return std::unordered_map<std::string, std::string>{};';
      else if (sigLine.includes('-> Result<std::vector<StreamEntry>>')) stub = '    return std::vector<StreamEntry>{};';
      else if (sigLine.includes('-> Result<std::vector<std::optional<std::string>>>')) stub = '    return std::vector<std::optional<std::string>>{};';
      else if (sigLine.includes('-> Pipeline&')) stub = '    return *this;';
      else if (sigLine.includes('-> std::optional<MetaValue>')) stub = '    return std::nullopt;';
      else if (sigLine.includes('-> void')) stub = '    return;';

      // Find and skip the original body
      let depth = 0;
      let k = braceLine;
      while (k < lines.length) {
        for (let c = 0; c < lines[k].length; c++) {
          if (lines[k][c] === '{') depth++;
          if (lines[k][c] === '}') depth--;
        }
        if (depth === 0) break;
        k++;
      }

      output.push(stub);
      output.push('  }');
      i = k;
      continue;
    }

    // For non-function lines in the new section, skip them
    // unless they're namespace/class structure lines
    if (lines[i].trim() === '' || lines[i].trim().startsWith('//')) {
      // Keep blank lines and comments
      output.push(lines[i]);
    } else if (lines[i].trim() === '} // namespace redismm' ||
               lines[i].trim() === 'namespace redismm {') {
      output.push(lines[i]);
    }
    // Skip everything else (code inside new sections)
    // Actually, we need to handle '#if 0' properly - let's just insert it
    // Wait I'm overcomplicating this. Let me just add #if 0/#endif.
  } else {
    output.push(lines[i]);
  }
}

// Clean up output - add #endif at the end
if (afterOriginal) {
  output.push('');
  output.push('#endif');
}

fs.writeFileSync('/home/toge/src/redismm/src/EmbeddedRedis.cpp.stubbed2', output.join('\n'), { encoding: 'utf8' });
console.log(`Output: ${output.length} lines, written to stubbed2`);
