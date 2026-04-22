pragma circom 2.1.6;
include "./mimc7.circom";

template RVCSemanticBatch(MAXB) {
    signal input epochTag;
    signal input fromShard;
    signal input toShard;
    signal input batchSize;
    signal input semanticDigest;
    signal input witnessBundleBinding;
    signal input certificateBinding;

    signal input active[MAXB];
    signal input addr[MAXB];
    signal input sourceBalance[MAXB];
    signal input sourceNonce[MAXB];
    signal input sourceCode[MAXB];
    signal input sourceStore[MAXB];
    signal input freezeBalance[MAXB];
    signal input freezeNonce[MAXB];
    signal input freezeCode[MAXB];
    signal input freezeStore[MAXB];
    signal input capsuleBalance[MAXB];
    signal input capsuleNonce[MAXB];
    signal input capsuleCode[MAXB];
    signal input capsuleStore[MAXB];
    signal input debtRoot[MAXB];

    signal sumActive[MAXB + 1];
    sumActive[0] <== 0;
    for (var i = 0; i < MAXB; i++) {
        active[i] * (active[i] - 1) === 0;
        (sourceBalance[i] - capsuleBalance[i]) * active[i] === 0;
        (sourceNonce[i] - capsuleNonce[i]) * active[i] === 0;
        (sourceCode[i] - capsuleCode[i]) * active[i] === 0;
        (sourceStore[i] - capsuleStore[i]) * active[i] === 0;
        (freezeBalance[i] - sourceBalance[i]) * active[i] === 0;
        (freezeNonce[i] - sourceNonce[i]) * active[i] === 0;
        (freezeCode[i] - sourceCode[i]) * active[i] === 0;
        (freezeStore[i] - sourceStore[i]) * active[i] === 0;
        sumActive[i + 1] <== sumActive[i] + active[i];
    }
    sumActive[MAXB] === batchSize;

    signal meta0; meta0 <== epochTag;
    component h0 = MiMC7();
    h0.in <== meta0;
    signal state1; state1 <== h0.out + fromShard;
    component h1 = MiMC7();
    h1.in <== state1;
    signal state2; state2 <== h1.out + toShard;
    component h2 = MiMC7();
    h2.in <== state2;
    signal state3; state3 <== h2.out + batchSize;
    component h3 = MiMC7();
    h3.in <== state3;
    signal state4; state4 <== h3.out + witnessBundleBinding;
    component h4 = MiMC7();
    h4.in <== state4;
    signal state5; state5 <== h4.out + certificateBinding;
    component h5 = MiMC7();
    h5.in <== state5;
    signal rolling[MAXB * 14 + 1];
    rolling[0] <== h5.out;

    for (var i = 0; i < MAXB; i++) {
        signal v0; v0 <== rolling[i * 14] + addr[i] * active[i];
        component c0 = MiMC7(); c0.in <== v0; rolling[i * 14 + 1] <== c0.out;
        signal v1; v1 <== rolling[i * 14 + 1] + sourceBalance[i] * active[i];
        component c1 = MiMC7(); c1.in <== v1; rolling[i * 14 + 2] <== c1.out;
        signal v2; v2 <== rolling[i * 14 + 2] + sourceNonce[i] * active[i];
        component c2 = MiMC7(); c2.in <== v2; rolling[i * 14 + 3] <== c2.out;
        signal v3; v3 <== rolling[i * 14 + 3] + sourceCode[i] * active[i];
        component c3 = MiMC7(); c3.in <== v3; rolling[i * 14 + 4] <== c3.out;
        signal v4; v4 <== rolling[i * 14 + 4] + sourceStore[i] * active[i];
        component c4 = MiMC7(); c4.in <== v4; rolling[i * 14 + 5] <== c4.out;
        signal v5; v5 <== rolling[i * 14 + 5] + freezeBalance[i] * active[i];
        component c5 = MiMC7(); c5.in <== v5; rolling[i * 14 + 6] <== c5.out;
        signal v6; v6 <== rolling[i * 14 + 6] + freezeNonce[i] * active[i];
        component c6 = MiMC7(); c6.in <== v6; rolling[i * 14 + 7] <== c6.out;
        signal v7; v7 <== rolling[i * 14 + 7] + freezeCode[i] * active[i];
        component c7 = MiMC7(); c7.in <== v7; rolling[i * 14 + 8] <== c7.out;
        signal v8; v8 <== rolling[i * 14 + 8] + freezeStore[i] * active[i];
        component c8 = MiMC7(); c8.in <== v8; rolling[i * 14 + 9] <== c8.out;
        signal v9; v9 <== rolling[i * 14 + 9] + capsuleBalance[i] * active[i];
        component c9 = MiMC7(); c9.in <== v9; rolling[i * 14 + 10] <== c9.out;
        signal v10; v10 <== rolling[i * 14 + 10] + capsuleNonce[i] * active[i];
        component c10 = MiMC7(); c10.in <== v10; rolling[i * 14 + 11] <== c10.out;
        signal v11; v11 <== rolling[i * 14 + 11] + capsuleCode[i] * active[i];
        component c11 = MiMC7(); c11.in <== v11; rolling[i * 14 + 12] <== c11.out;
        signal v12; v12 <== rolling[i * 14 + 12] + capsuleStore[i] * active[i];
        component c12 = MiMC7(); c12.in <== v12; rolling[i * 14 + 13] <== c12.out;
        signal v13; v13 <== rolling[i * 14 + 13] + debtRoot[i] * active[i];
        component c13 = MiMC7(); c13.in <== v13; rolling[i * 14 + 14] <== c13.out;
    }

    rolling[MAXB * 14] === semanticDigest;
}

component main {public [epochTag, fromShard, toShard, batchSize, semanticDigest, witnessBundleBinding, certificateBinding]} = RVCSemanticBatch(32);
