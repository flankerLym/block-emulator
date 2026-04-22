pragma circom 2.1.6;
include "./mimc7.circom";

template RetirementFinality(MAXR, MAXW) {
    signal input addressBinding;
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
    signal input rvcBinding;

    signal input receiptActive[MAXR];
    signal input receiptKey[MAXR];
    signal input receiptSettled[MAXR];
    signal input writeActive[MAXW];
    signal input writeKey[MAXW];

    hydratedFlag === 1;
    debtRootClearedFlag === 1;
    outstandingReceiptCount === 0;
    postCutoverWriteCount === 0;

    signal settledSum[MAXR + 1];
    signal outstandingSum[MAXR + 1];
    settledSum[0] <== 0;
    outstandingSum[0] <== 0;

    signal debtRolling[MAXR * 2 + 1];
    debtRolling[0] <== 0;

    for (var i = 0; i < MAXR; i++) {
        receiptActive[i] * (receiptActive[i] - 1) === 0;
        receiptSettled[i] * (receiptSettled[i] - 1) === 0;
        receiptSettled[i] * (1 - receiptActive[i]) === 0;

        settledSum[i + 1] <== settledSum[i] + receiptSettled[i];
        outstandingSum[i + 1] <== outstandingSum[i] + (receiptActive[i] - receiptSettled[i]);

        signal debtInput0;
        debtInput0 <== debtRolling[i * 2] + receiptKey[i] * receiptActive[i];
        component debtHash0 = MiMC7();
        debtHash0.in <== debtInput0;
        debtRolling[i * 2 + 1] <== debtHash0.out;

        signal debtInput1;
        debtInput1 <== debtRolling[i * 2 + 1] + receiptSettled[i] * receiptActive[i];
        component debtHash1 = MiMC7();
        debtHash1.in <== debtInput1;
        debtRolling[i * 2 + 2] <== debtHash1.out;
    }

    settledSum[MAXR] === settledReceiptCount;
    outstandingSum[MAXR] === outstandingReceiptCount;
    debtRolling[MAXR * 2] === debtWitnessDigest;

    signal writeSum[MAXW + 1];
    writeSum[0] <== 0;
    signal writeRolling[MAXW + 1];
    writeRolling[0] <== 0;

    for (var j = 0; j < MAXW; j++) {
        writeActive[j] * (writeActive[j] - 1) === 0;
        writeSum[j + 1] <== writeSum[j] + writeActive[j];
        signal writeInput;
        writeInput <== writeRolling[j] + writeKey[j] * writeActive[j];
        component writeHash = MiMC7();
        writeHash.in <== writeInput;
        writeRolling[j + 1] <== writeHash.out;
    }

    writeSum[MAXW] === postCutoverWriteCount;
    writeRolling[MAXW] === noWriteWitnessDigest;

    signal meta0; meta0 <== addressBinding;
    component h0 = MiMC7(); h0.in <== meta0;
    signal meta1; meta1 <== h0.out + epochTag;
    component h1 = MiMC7(); h1.in <== meta1;
    signal meta2; meta2 <== h1.out + fromShard;
    component h2 = MiMC7(); h2.in <== meta2;
    signal meta3; meta3 <== h2.out + toShard;
    component h3 = MiMC7(); h3.in <== meta3;
    signal meta4; meta4 <== h3.out + settledReceiptCount;
    component h4 = MiMC7(); h4.in <== meta4;
    signal meta5; meta5 <== h4.out + outstandingReceiptCount;
    component h5 = MiMC7(); h5.in <== meta5;
    signal meta6; meta6 <== h5.out + postCutoverWriteCount;
    component h6 = MiMC7(); h6.in <== meta6;
    signal meta7; meta7 <== h6.out + debtWitnessDigest;
    component h7 = MiMC7(); h7.in <== meta7;
    signal meta8; meta8 <== h7.out + noWriteWitnessDigest;
    component h8 = MiMC7(); h8.in <== meta8;
    signal meta9; meta9 <== h8.out + rvcBinding;
    component h9 = MiMC7(); h9.in <== meta9;

    h9.out === retirementWitnessDigest;
}

component main {public [addressBinding, epochTag, fromShard, toShard, hydratedFlag, debtRootClearedFlag, settledReceiptCount, outstandingReceiptCount, postCutoverWriteCount, debtWitnessDigest, noWriteWitnessDigest, retirementWitnessDigest, rvcBinding]} = RetirementFinality(64, 32);
