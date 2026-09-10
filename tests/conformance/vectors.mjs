import { readFileSync, writeFileSync } from 'node:fs';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { swapTree, reverseSwapTree, Musig, TaprootUtils, SwapTreeSerializer, Networks } from 'boltz-core';
import { secp256k1 } from '@noble/curves/secp256k1.js';
import { Address } from '@scure/btc-signer';
const h = value => Buffer.from(value).toString('hex');
const providerPrivate = new Uint8Array(32); providerPrivate[31] = 1;
const clientPrivate = new Uint8Array(32); clientPrivate[31] = 2;
const provider = secp256k1.getPublicKey(providerPrivate);
const client = secp256k1.getPublicKey(clientPrivate);
const preimage = new Uint8Array(32).fill(3);
const preimageHash = createHash('sha256').update(preimage).digest();
const cases = {};
for (const reverse of [false, true]) {
 const tree = (reverse ? reverseSwapTree : swapTree)(false, preimageHash, reverse ? client : provider, reverse ? provider : client, 250);
 const aggregate = Musig.create(providerPrivate, [provider, client]);
 const tweaked = TaprootUtils.tweakMusig(aggregate, tree.tree);
 const hashTree = TaprootUtils.taprootHashTree(tree.tree);
 cases[reverse ? 'reverse' : 'submarine'] = {
  providerPublicKey: h(provider), clientPublicKey: h(client), preimage: h(preimage), preimageHash: h(preimageHash), timeoutBlockHeight: 250,
  swapTree: SwapTreeSerializer.serializeSwapTree(tree), internalKey: h(aggregate.internalKey), merkleRoot: h(hashTree.hash), outputKey: h(tweaked.aggPubkey),
  address: Address(Networks.regtest).encode({type:'tr',pubkey:tweaked.aggPubkey}),
  claimControlBlock: h(TaprootUtils.createControlBlock(hashTree, tree.claimLeaf, aggregate.internalKey)),
  refundControlBlock: h(TaprootUtils.createControlBlock(hashTree, tree.refundLeaf, aggregate.internalKey))
 };
}
const actual = {source:'BoltzExchange/boltz-core',version:'5.0.0',commit:'7612d67102ad370673c26be000ecc8804a5bec45',cases};
const fixturePath = new URL('./boltz-vectors.json', import.meta.url);
if (process.argv.includes('--write')) {
  writeFileSync(fixturePath, JSON.stringify(actual, null, 2)+'\n');
} else {
  assert.deepEqual(actual, JSON.parse(readFileSync(fixturePath, 'utf8')));
  console.log('Both Taproot vectors match boltz-core 5.0.0');
}
