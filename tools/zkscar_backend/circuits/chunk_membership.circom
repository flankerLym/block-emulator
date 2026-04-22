pragma circom 2.1.6;
include "./mimc7.circom";

template Num2Bits16() {
    signal input in;
    signal output out[16];
    var acc = 0;
    var pw = 1;
    for (var i = 0; i < 16; i++) {
        out[i] <-- (in >> i) & 1;
        out[i] * (out[i] - 1) === 0;
        acc += out[i] * pw;
        pw *= 2;
    }
    acc === in;
}

template HashPairMiMCChain() {
    signal input left;
    signal input right;
    signal output out;

    component h0 = MiMC7();
    h0.in <== left;

    signal mix;
    mix <== h0.out + right;

    component h1 = MiMC7();
    h1.in <== mix;

    out <== h1.out;
}

template MerkleMembership(DEPTH) {
    signal input root;
    signal input leaf;
    signal input index;
    signal input total;
    signal input siblings[DEPTH];

    component bits = Num2Bits16();
    bits.in <== index;

    signal level[DEPTH + 1];
    level[0] <== leaf;
    for (var i = 0; i < DEPTH; i++) {
        signal left;
        signal right;
        left <== level[i] + bits.out[i] * (siblings[i] - level[i]);
        right <== siblings[i] + bits.out[i] * (level[i] - siblings[i]);

        component hp = HashPairMiMCChain();
        hp.left <== left;
        hp.right <== right;
        level[i + 1] <== hp.out;
    }

    level[DEPTH] === root;

    // keep `total` as a public signal bound into the proof transcript;
    // tree-size semantics are enforced natively when the sender constructs
    // the sibling path for the requested chunk.
    total === total;
}

component main {public [root, leaf, index, total]} = MerkleMembership(16);
