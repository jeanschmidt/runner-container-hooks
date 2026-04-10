#!/usr/bin/env node
const fs = require('fs')
const path = require('path')

const pyPath = path.join(__dirname, '..', 'src', 'k8s', 'rpc-server.py')
const tsPath = path.join(__dirname, '..', 'src', 'k8s', 'rpc-server-script.ts')

const pySource = fs.readFileSync(pyPath, 'utf-8')

const tsContent = `// Auto-generated from rpc-server.py — do not edit directly.
// To update: edit src/k8s/rpc-server.py and run: node scripts/embed-rpc-server.js
export const RPC_SERVER_SCRIPT = ${JSON.stringify(pySource)}
`

fs.writeFileSync(tsPath, tsContent)
