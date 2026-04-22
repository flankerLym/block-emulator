pragma circom 2.1.6;
include "./mimc7.circom";

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
}

component main {public [epochTag, fromShard, toShard, hydratedFlag, debtRootClearedFlag, settledReceiptCount, outstandingReceiptCount, postCutoverWriteCount, debtWitnessDigest, noWriteWitnessDigest, retirementWitnessDigest, rvcBinding]} = RetirementFinality(64, 32);
