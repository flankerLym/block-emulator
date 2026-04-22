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

template RetirementFinality(MAXR, MAXW) {
    signal input epochTag;
    signal input fromShard;
    signal input toShard;
    signal input hydratedFlag;
    signal input debtRootClearedFlag;
    signal input settledReceiptCount;
    signal input outstandingReceiptCount;
    signal input postCutoverWriteCount;
    signal input debtWitnessDigest;
    signal input noWriteWitnessDigest;
    signal input retirementWitnessDigest;
    signal input accountBinding;
    signal input rvcBinding;

    signal input receiptActive[MAXR];
    signal input receiptKey[MAXR];
    signal input receiptSettled[MAXR];
    signal input writeActive[MAXW];
    signal input writeKey[MAXW];

    hydratedFlag * (hydratedFlag - 1) === 0;
    debtRootClearedFlag * (debtRootClearedFlag - 1) === 0;
    hydratedFlag === 1;
    debtRootClearedFlag === 1;

    signal settledSum[MAXR + 1];
    signal outstandingSum[MAXR + 1];
    settledSum[0] <== 0;
    outstandingSum[0] <== 0;
    for (var i = 0; i < MAXR; i++) {
        receiptActive[i] * (receiptActive[i] - 1) === 0;
        receiptSettled[i] * (receiptSettled[i] - 1) === 0;
        receiptSettled[i] * (1 - receiptActive[i]) === 0;
        settledSum[i + 1] <== settledSum[i] + receiptActive[i] * receiptSettled[i];
        outstandingSum[i + 1] <== outstandingSum[i] + receiptActive[i] * (1 - receiptSettled[i]);
    }
    settledSum[MAXR] === settledReceiptCount;
    outstandingSum[MAXR] === outstandingReceiptCount;
    outstandingReceiptCount === 0;

    signal writeSum[MAXW + 1];
    writeSum[0] <== 0;
    for (var j = 0; j < MAXW; j++) {
        writeActive[j] * (writeActive[j] - 1) === 0;
        writeSum[j + 1] <== writeSum[j] + writeActive[j];
    }
    writeSum[MAXW] === postCutoverWriteCount;
    postCutoverWriteCount === 0;

    signal debtMeta0; debtMeta0 <== epochTag;
    component dh0 = MiMC7(); dh0.in <== debtMeta0;
    signal debtMeta1; debtMeta1 <== dh0.out + fromShard;
    component dh1 = MiMC7(); dh1.in <== debtMeta1;
    signal debtMeta2; debtMeta2 <== dh1.out + toShard;
    component dh2 = MiMC7(); dh2.in <== debtMeta2;
    signal debtMeta3; debtMeta3 <== dh2.out + accountBinding;
    component dh3 = MiMC7(); dh3.in <== debtMeta3;
    signal debtMeta4; debtMeta4 <== dh3.out + rvcBinding;
    component dh4 = MiMC7(); dh4.in <== debtMeta4;
    signal debtRolling[MAXR * 3 + 1];
    debtRolling[0] <== dh4.out;
    for (var k = 0; k < MAXR; k++) {
        signal dv0; dv0 <== debtRolling[k * 3] + receiptActive[k];
        component dc0 = MiMC7(); dc0.in <== dv0; debtRolling[k * 3 + 1] <== dc0.out;
        signal dv1; dv1 <== debtRolling[k * 3 + 1] + receiptKey[k] * receiptActive[k];
        component dc1 = MiMC7(); dc1.in <== dv1; debtRolling[k * 3 + 2] <== dc1.out;
        signal dv2; dv2 <== debtRolling[k * 3 + 2] + receiptSettled[k] * receiptActive[k];
        component dc2 = MiMC7(); dc2.in <== dv2; debtRolling[k * 3 + 3] <== dc2.out;
    }
    debtRolling[MAXR * 3] === debtWitnessDigest;

    signal writeMeta0; writeMeta0 <== epochTag;
    component wh0 = MiMC7(); wh0.in <== writeMeta0;
    signal writeMeta1; writeMeta1 <== wh0.out + fromShard;
    component wh1 = MiMC7(); wh1.in <== writeMeta1;
    signal writeMeta2; writeMeta2 <== wh1.out + toShard;
    component wh2 = MiMC7(); wh2.in <== writeMeta2;
    signal writeMeta3; writeMeta3 <== wh2.out + accountBinding;
    component wh3 = MiMC7(); wh3.in <== writeMeta3;
    signal writeMeta4; writeMeta4 <== wh3.out + rvcBinding;
    component wh4 = MiMC7(); wh4.in <== writeMeta4;
    signal writeRolling[MAXW * 2 + 1];
    writeRolling[0] <== wh4.out;
    for (var m = 0; m < MAXW; m++) {
        signal wv0; wv0 <== writeRolling[m * 2] + writeActive[m];
        component wc0 = MiMC7(); wc0.in <== wv0; writeRolling[m * 2 + 1] <== wc0.out;
        signal wv1; wv1 <== writeRolling[m * 2 + 1] + writeKey[m] * writeActive[m];
        component wc1 = MiMC7(); wc1.in <== wv1; writeRolling[m * 2 + 2] <== wc1.out;
    }
    writeRolling[MAXW * 2] === noWriteWitnessDigest;

    signal rv0; rv0 <== epochTag;
    component rh0 = MiMC7(); rh0.in <== rv0;
    signal rv1; rv1 <== rh0.out + fromShard;
    component rh1 = MiMC7(); rh1.in <== rv1;
    signal rv2; rv2 <== rh1.out + toShard;
    component rh2 = MiMC7(); rh2.in <== rv2;
    signal rv3; rv3 <== rh2.out + hydratedFlag;
    component rh3 = MiMC7(); rh3.in <== rv3;
    signal rv4; rv4 <== rh3.out + debtRootClearedFlag;
    component rh4 = MiMC7(); rh4.in <== rv4;
    signal rv5; rv5 <== rh4.out + settledReceiptCount;
    component rh5 = MiMC7(); rh5.in <== rv5;
    signal rv6; rv6 <== rh5.out + outstandingReceiptCount;
    component rh6 = MiMC7(); rh6.in <== rv6;
    signal rv7; rv7 <== rh6.out + postCutoverWriteCount;
    component rh7 = MiMC7(); rh7.in <== rv7;
    signal rv8; rv8 <== rh7.out + debtWitnessDigest;
    component rh8 = MiMC7(); rh8.in <== rv8;
    signal rv9; rv9 <== rh8.out + noWriteWitnessDigest;
    component rh9 = MiMC7(); rh9.in <== rv9;
    signal rv10; rv10 <== rh9.out + accountBinding;
    component rh10 = MiMC7(); rh10.in <== rv10;
    signal rv11; rv11 <== rh10.out + rvcBinding;
    component rh11 = MiMC7(); rh11.in <== rv11;

    rh11.out === retirementWitnessDigest;
}

component main {public [epochTag, fromShard, toShard, hydratedFlag, debtRootClearedFlag, settledReceiptCount, outstandingReceiptCount, postCutoverWriteCount, debtWitnessDigest, noWriteWitnessDigest, retirementWitnessDigest, accountBinding, rvcBinding]} = RetirementFinality(64, 32);
