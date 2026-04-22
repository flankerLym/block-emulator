pragma circom 2.1.6;
include "./mimc7.circom";

template IsZero() {
    signal input in;
    signal output out;
    signal inv;
    inv <-- in != 0 ? 1 / in : 0;
    out <== 1 - in * inv;
    in * out === 0;
}

template Num2BitsN(N) {
    signal input in;
    signal output out[N];
    var acc = 0;
    var pw = 1;
    for (var i = 0; i < N; i++) {
        out[i] <-- (in >> i) & 1;
        out[i] * (out[i] - 1) === 0;
        acc += out[i] * pw;
        pw *= 2;
    }
    acc === in;
}

template LessThan(N) {
    signal input a;
    signal input b;
    signal output out;

    component bits = Num2BitsN(N + 1);
    bits.in <== a + (1 << N) - b;
    out <== 1 - bits.out[N];
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

    component totalNonZero = IsZero();
    totalNonZero.in <== total;
    totalNonZero.out === 0;

    component indexLtTotal = LessThan(16);
    indexLtTotal.a <== index;
    indexLtTotal.b <== total;
    indexLtTotal.out === 1;

    component totalWithinDepth = LessThan(17);
    totalWithinDepth.a <== total;
    totalWithinDepth.b <== (1 << DEPTH) + 1;
    totalWithinDepth.out === 1;

    component bits = Num2BitsN(16);
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
}

component main {public [root, leaf, index, total]} = MerkleMembership(16);
