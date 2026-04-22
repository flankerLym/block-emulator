$ErrorActionPreference = 'Stop'
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$Art = Join-Path $Root 'artifacts'
$Cir = Join-Path $Root 'circuits'
$Ptau = Join-Path $Art 'pot12_final.ptau'
New-Item -ItemType Directory -Force -Path (Join-Path $Art 'rvc_semantic_batch') | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $Art 'chunk_membership') | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $Art 'retirement_finality') | Out-Null

if (-not (Get-Command circom -ErrorAction SilentlyContinue)) { throw 'circom not found in PATH' }
if (-not (Get-Command snarkjs -ErrorAction SilentlyContinue)) { throw 'snarkjs not found in PATH' }

if (-not (Test-Path $Ptau)) {
  snarkjs powersoftau new bn128 12 (Join-Path $Art 'pot12_0000.ptau') -v
  snarkjs powersoftau contribute (Join-Path $Art 'pot12_0000.ptau') (Join-Path $Art 'pot12_0001.ptau') --name='ZKSCAR' -v -e='zkscar'
  snarkjs powersoftau prepare phase2 (Join-Path $Art 'pot12_0001.ptau') $Ptau -v
}

circom (Join-Path $Cir 'rvc_semantic_batch.circom') --r1cs --wasm --sym -o (Join-Path $Art 'rvc_semantic_batch')
snarkjs groth16 setup (Join-Path $Art 'rvc_semantic_batch/rvc_semantic_batch.r1cs') $Ptau (Join-Path $Art 'rvc_semantic_batch/rvc_semantic_batch_0000.zkey')
snarkjs zkey contribute (Join-Path $Art 'rvc_semantic_batch/rvc_semantic_batch_0000.zkey') (Join-Path $Art 'rvc_semantic_batch/rvc_semantic_batch_final.zkey') --name='ZKSCAR-RVC' -v -e='rvc'
snarkjs zkey export verificationkey (Join-Path $Art 'rvc_semantic_batch/rvc_semantic_batch_final.zkey') (Join-Path $Art 'rvc_semantic_batch/verification_key.json')

circom (Join-Path $Cir 'chunk_membership.circom') --r1cs --wasm --sym -o (Join-Path $Art 'chunk_membership')
snarkjs groth16 setup (Join-Path $Art 'chunk_membership/chunk_membership.r1cs') $Ptau (Join-Path $Art 'chunk_membership/chunk_membership_0000.zkey')
snarkjs zkey contribute (Join-Path $Art 'chunk_membership/chunk_membership_0000.zkey') (Join-Path $Art 'chunk_membership/chunk_membership_final.zkey') --name='ZKSCAR-CHUNK' -v -e='chunk'
snarkjs zkey export verificationkey (Join-Path $Art 'chunk_membership/chunk_membership_final.zkey') (Join-Path $Art 'chunk_membership/verification_key.json')

circom (Join-Path $Cir 'retirement_finality.circom') --r1cs --wasm --sym -o (Join-Path $Art 'retirement_finality')
snarkjs groth16 setup (Join-Path $Art 'retirement_finality/retirement_finality.r1cs') $Ptau (Join-Path $Art 'retirement_finality/retirement_finality_0000.zkey')
snarkjs zkey contribute (Join-Path $Art 'retirement_finality/retirement_finality_0000.zkey') (Join-Path $Art 'retirement_finality/retirement_finality_final.zkey') --name='ZKSCAR-RETIREMENT' -v -e='retirement'
snarkjs zkey export verificationkey (Join-Path $Art 'retirement_finality/retirement_finality_final.zkey') (Join-Path $Art 'retirement_finality/verification_key.json')

Write-Host "Artifacts built under $Art"
