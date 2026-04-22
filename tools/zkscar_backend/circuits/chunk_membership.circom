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
        signal mix;
        mix <== left + right;
        component h = MiMC7();
        h.in <== mix;
        level[i + 1] <== h.out;
    }
    level[DEPTH] === root;
    total === total;
}

component main {public [root, leaf, index, total]} = MerkleMembership(16);
